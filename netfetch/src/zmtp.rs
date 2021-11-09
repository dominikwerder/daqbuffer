use crate::bsread::parse_zmtp_message;
use crate::bsread::ChannelDesc;
use crate::bsread::GlobalTimestamp;
use crate::bsread::HeadA;
use crate::bsread::HeadB;
use crate::netbuf::NetBuf;
use crate::netbuf::RP_REW_PT;
use async_channel::Receiver;
use async_channel::Sender;
#[allow(unused)]
use bytes::BufMut;
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, StreamExt};
use netpod::log::*;
use netpod::timeunits::SEC;
use serde_json::Value as JsVal;
use std::fmt;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

//#[test]
#[allow(unused)]
fn test_listen() -> Result<(), Error> {
    use std::time::Duration;
    let fut = async move {
        let _ = tokio::time::timeout(Duration::from_millis(16000), zmtp_client("camtest:9999")).await;
        Ok::<_, Error>(())
    };
    taskrun::run(fut)
}

//#[test]
#[allow(unused)]
fn test_service() -> Result<(), Error> {
    //use std::time::Duration;
    let fut = async move {
        let sock = tokio::net::TcpListener::bind("0.0.0.0:9999").await?;
        loop {
            info!("accepting...");
            let (conn, remote) = sock.accept().await?;
            info!("new connection from {:?}", remote);
            let mut zmtp = Zmtp::new(conn, SocketType::PUSH);
            let fut = async move {
                while let Some(item) = zmtp.next().await {
                    info!("item from {:?}  {:?}", remote, item);
                }
                Ok::<_, Error>(())
            };
            taskrun::spawn(fut);
        }
        //Ok::<_, Error>(())
    };
    taskrun::run(fut)
}

pub async fn zmtp_00() -> Result<(), Error> {
    let addr = "S10-CPPM-MOT0991:9999";
    zmtp_client(addr).await?;
    Ok(())
}

pub async fn zmtp_client(addr: &str) -> Result<(), Error> {
    let conn = tokio::net::TcpStream::connect(addr).await?;
    let mut zmtp = Zmtp::new(conn, SocketType::PULL);
    let mut i1 = 0;
    while let Some(item) = zmtp.next().await {
        match item {
            Ok(ev) => match ev {
                ZmtpEvent::ZmtpMessage(msg) => {
                    info!("Message frames: {}", msg.frames.len());
                    match parse_zmtp_message(&msg) {
                        Ok(msg) => info!("{:?}", msg),
                        Err(e) => {
                            error!("{}", e);
                            for frame in &msg.frames {
                                info!("Frame: {:?}", frame);
                            }
                        }
                    }
                }
            },
            Err(e) => {
                error!("{}", e);
                return Err(e);
            }
        }
        i1 += 1;
        if i1 > 100 {
            break;
        }
    }
    Ok(())
}

enum ConnState {
    InitSend,
    InitRecv1,
    InitRecv2,
    ReadFrameFlags,
    ReadFrameShort,
    ReadFrameLong,
    ReadFrameBody(usize),
}

impl ConnState {
    fn need_min(&self) -> usize {
        use ConnState::*;
        match self {
            InitSend => 0,
            InitRecv1 => 11,
            InitRecv2 => 53,
            ReadFrameFlags => 1,
            ReadFrameShort => 1,
            ReadFrameLong => 8,
            ReadFrameBody(msglen) => *msglen,
        }
    }
}

pub enum SocketType {
    PUSH,
    PULL,
}

struct DummyData {
    ts: u64,
    pulse: u64,
    value: i64,
}

impl DummyData {
    fn make_zmtp_msg(&self) -> Result<ZmtpMessage, Error> {
        let head_b = HeadB {
            htype: "bsr_d-1.1".into(),
            channels: vec![ChannelDesc {
                name: "TESTCHAN".into(),
                ty: "int64".into(),
                shape: JsVal::Array(vec![JsVal::Number(serde_json::Number::from(1))]),
                encoding: "little".into(),
            }],
        };
        let hb = serde_json::to_vec(&head_b).unwrap();
        use md5::Digest;
        let mut h = md5::Md5::new();
        h.update(&hb);
        let mut md5hex = String::with_capacity(32);
        for c in h.finalize() {
            use fmt::Write;
            write!(&mut md5hex, "{:02x}", c).unwrap();
        }
        let head_a = HeadA {
            htype: "bsr_m-1.1".into(),
            hash: md5hex,
            pulse_id: serde_json::Number::from(self.pulse),
            global_timestamp: GlobalTimestamp {
                sec: self.ts / SEC,
                ns: self.ts % SEC,
            },
        };
        // TODO write directly to output buffer.
        let ha = serde_json::to_vec(&head_a).unwrap();
        let hf = self.value.to_le_bytes().to_vec();
        let hp = [(self.ts / SEC).to_be_bytes(), (self.ts % SEC).to_be_bytes()].concat();
        let mut msg = ZmtpMessage { frames: vec![] };
        let fr = ZmtpFrame {
            msglen: 0,
            has_more: false,
            is_command: false,
            data: ha,
        };
        msg.frames.push(fr);
        let fr = ZmtpFrame {
            msglen: 0,
            has_more: false,
            is_command: false,
            data: hb,
        };
        msg.frames.push(fr);
        let fr = ZmtpFrame {
            msglen: 0,
            has_more: false,
            is_command: false,
            data: hf,
        };
        msg.frames.push(fr);
        let fr = ZmtpFrame {
            msglen: 0,
            has_more: false,
            is_command: false,
            data: hp,
        };
        msg.frames.push(fr);
        Ok(msg)
    }
}

struct Zmtp {
    done: bool,
    complete: bool,
    socket_type: SocketType,
    conn: TcpStream,
    conn_state: ConnState,
    buf: NetBuf,
    outbuf: NetBuf,
    out_enable: bool,
    msglen: usize,
    has_more: bool,
    is_command: bool,
    frames: Vec<ZmtpFrame>,
    inp_eof: bool,
    data_tx: Sender<DummyData>,
    data_rx: Receiver<DummyData>,
}

impl Zmtp {
    fn new(conn: TcpStream, socket_type: SocketType) -> Self {
        //conn.set_send_buffer_size(1024 * 64)?;
        //conn.set_recv_buffer_size(1024 * 1024 * 4)?;
        //info!("send_buffer_size  {:8}", conn.send_buffer_size()?);
        //info!("recv_buffer_size  {:8}", conn.recv_buffer_size()?);
        let (tx, rx) = async_channel::bounded(1);
        Self {
            done: false,
            complete: false,
            socket_type,
            conn,
            conn_state: ConnState::InitSend,
            buf: NetBuf::new(),
            outbuf: NetBuf::new(),
            out_enable: false,
            msglen: 0,
            has_more: false,
            is_command: false,
            frames: vec![],
            inp_eof: false,
            data_tx: tx,
            data_rx: rx,
        }
    }

    fn inpbuf_conn(&mut self) -> (&mut TcpStream, ReadBuf) {
        (&mut self.conn, self.buf.read_buf_for_fill())
    }

    fn outbuf_conn(&mut self) -> (&[u8], &mut TcpStream) {
        (self.outbuf.data(), &mut self.conn)
    }

    fn parse_item(&mut self) -> Result<Option<ZmtpEvent>, Error> {
        match self.conn_state {
            ConnState::InitSend => {
                info!("parse_item  InitSend");
                self.outbuf.put_slice(&[0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 3])?;
                self.conn_state = ConnState::InitRecv1;
                Ok(None)
            }
            ConnState::InitRecv1 => {
                self.buf.adv(10)?;
                let ver = self.buf.read_u8()?;
                info!("parse_item  InitRecv1  major version {}", ver);
                if ver != 3 {
                    return Err(Error::with_msg_no_trace(format!("bad version {}", ver)));
                }
                self.outbuf.put_slice(&[0, 0x4e, 0x55, 0x4c, 0x4c])?;
                let a = vec![0; 48];
                self.outbuf.put_slice(&a)?;
                self.conn_state = ConnState::InitRecv2;
                Ok(None)
            }
            ConnState::InitRecv2 => {
                info!("parse_item  InitRecv2");
                let msgrem = self.conn_state.need_min();
                let ver_min = self.buf.read_u8()?;
                let msgrem = msgrem - 1;
                info!("Peer minor version {}", ver_min);
                // TODO parse greeting remainder.. sec-scheme.
                self.buf.adv(msgrem)?;
                match self.socket_type {
                    SocketType::PUSH => {
                        self.outbuf
                            .put_slice(&b"\x04\x1a\x05READY\x0bSocket-Type\x00\x00\x00\x04PUSH"[..])?;
                    }
                    SocketType::PULL => {
                        self.outbuf
                            .put_slice(&b"\x04\x1a\x05READY\x0bSocket-Type\x00\x00\x00\x04PULL"[..])?;
                    }
                }
                self.out_enable = true;
                self.conn_state = ConnState::ReadFrameFlags;
                let tx = self.data_tx.clone();
                let fut1 = async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        let dd = DummyData {
                            ts: 420032002200887766,
                            pulse: 123123123123,
                            value: -777,
                        };
                        match tx.send(dd).await {
                            Ok(()) => {
                                info!("item send to channel");
                            }
                            Err(_) => break,
                        }
                    }
                };
                taskrun::spawn(fut1);
                Ok(None)
            }
            ConnState::ReadFrameFlags => {
                let flags = self.buf.read_u8()?;
                let has_more = flags & 0x01 != 0;
                let long_size = flags & 0x02 != 0;
                let is_command = flags & 0x04 != 0;
                self.has_more = has_more;
                self.is_command = is_command;
                trace!(
                    "parse_item  ReadFrameFlags  has_more {}  long_size {}  is_command {}",
                    has_more,
                    long_size,
                    is_command
                );
                if is_command {
                    warn!("Got zmtp command frame");
                }
                if false && is_command {
                    return Err(Error::with_msg_no_trace("got zmtp command frame"));
                }
                if long_size {
                    self.conn_state = ConnState::ReadFrameLong;
                } else {
                    self.conn_state = ConnState::ReadFrameShort;
                }
                Ok(None)
            }
            ConnState::ReadFrameShort => {
                self.msglen = self.buf.read_u8()? as usize;
                trace!("parse_item  ReadFrameShort  msglen {}", self.msglen);
                self.conn_state = ConnState::ReadFrameBody(self.msglen);
                if self.msglen > 1024 * 64 {
                    return Err(Error::with_msg_no_trace(format!(
                        "larger msglen not yet supported  {}",
                        self.msglen,
                    )));
                }
                Ok(None)
            }
            ConnState::ReadFrameLong => {
                self.msglen = self.buf.read_u64()? as usize;
                trace!("parse_item  ReadFrameShort  msglen {}", self.msglen);
                self.conn_state = ConnState::ReadFrameBody(self.msglen);
                if self.msglen > 1024 * 64 {
                    return Err(Error::with_msg_no_trace(format!(
                        "larger msglen not yet supported  {}",
                        self.msglen,
                    )));
                }
                Ok(None)
            }
            ConnState::ReadFrameBody(msglen) => {
                let data = self.buf.read_bytes(msglen)?.to_vec();
                self.msglen = 0;
                if false {
                    let n1 = data.len().min(256);
                    let s = String::from_utf8_lossy(&data[..n1]);
                    trace!("parse_item  ReadFrameBody  msglen {}  string {}", msglen, s);
                }
                self.conn_state = ConnState::ReadFrameFlags;
                if !self.is_command {
                    let g = ZmtpFrame {
                        msglen: self.msglen,
                        has_more: self.has_more,
                        is_command: self.is_command,
                        data,
                    };
                    self.frames.push(g);
                }
                if self.has_more {
                    Ok(None)
                } else {
                    let g = ZmtpMessage {
                        frames: mem::replace(&mut self.frames, vec![]),
                    };
                    Ok(Some(ZmtpEvent::ZmtpMessage(g)))
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct ZmtpMessage {
    frames: Vec<ZmtpFrame>,
}

impl ZmtpMessage {
    pub fn frames(&self) -> &Vec<ZmtpFrame> {
        &self.frames
    }

    pub fn emit_to_buffer(&self, out: &mut NetBuf) -> Result<(), Error> {
        let n = self.frames.len();
        for (i, fr) in self.frames.iter().enumerate() {
            let mut flags: u8 = 2;
            if i < n - 1 {
                flags |= 1;
            }
            out.put_u8(flags)?;
            out.put_u64(fr.data().len() as u64)?;
            out.put_slice(fr.data())?;
        }
        Ok(())
    }
}

pub struct ZmtpFrame {
    msglen: usize,
    has_more: bool,
    is_command: bool,
    data: Vec<u8>,
}

impl ZmtpFrame {
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

impl fmt::Debug for ZmtpFrame {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let data = match String::from_utf8(self.data.clone()) {
            Ok(s) => s
                .chars()
                .filter(|x| {
                    //
                    x.is_ascii_alphanumeric() || x.is_ascii_punctuation() || x.is_ascii_whitespace()
                })
                .collect::<String>(),
            Err(_) => format!("Binary {{ len: {} }}", self.data.len()),
        };
        f.debug_struct("ZmtpFrame")
            .field("msglen", &self.msglen)
            .field("has_more", &self.has_more)
            .field("is_command", &self.is_command)
            .field("data", &data)
            .finish()
    }
}

enum Int<T> {
    NoWork,
    Pending,
    Empty,
    Item(T),
    Done,
}

impl<T> Int<T> {
    fn item_count(&self) -> u32 {
        if let Int::Item(_) = self {
            1
        } else {
            0
        }
    }
}

impl<T> fmt::Debug for Int<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NoWork => write!(f, "NoWork"),
            Self::Pending => write!(f, "Pending"),
            Self::Empty => write!(f, "Empty"),
            Self::Item(_) => write!(f, "Item"),
            Self::Done => write!(f, "Done"),
        }
    }
}

#[derive(Debug)]
enum ZmtpEvent {
    ZmtpMessage(ZmtpMessage),
}

impl Stream for Zmtp {
    type Item = Result<ZmtpEvent, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.complete {
            panic!("poll_next on complete")
        } else if self.done {
            self.complete = true;
            return Ready(None);
        }
        loop {
            let mut item_count = 0;
            let serialized: Int<Result<(), Error>> = if self.out_enable && self.outbuf.wrcap() >= RP_REW_PT {
                match self.data_rx.poll_next_unpin(cx) {
                    Ready(Some(item)) => {
                        let msg = item.make_zmtp_msg().unwrap();
                        match msg.emit_to_buffer(&mut self.outbuf) {
                            Ok(_) => Int::Empty,
                            Err(e) => {
                                self.done = true;
                                Int::Item(Err(e))
                            }
                        }
                        /*let mut msgs = Vec::with_capacity(1024 * 8);
                        msgs.put_u8(1 | 2);
                        msgs.put_u64(ha.len() as u64);
                        msgs.put_slice(&ha);
                        msgs.put_u8(1 | 2);
                        msgs.put_u64(hb.len() as u64);
                        msgs.put_slice(&hb);
                        msgs.put_u8(1 | 2);
                        msgs.put_u64(hf.len() as u64);
                        msgs.put_slice(&hf);
                        msgs.put_u8(2);
                        msgs.put_u64(hp.len() as u64);
                        msgs.put_slice(&hp);
                        self.outbuf.put_slice(&msgs).unwrap();
                        Int::Empty*/
                    }
                    Ready(None) => Int::Done,
                    Pending => Int::Pending,
                }
            } else {
                Int::NoWork
            };
            item_count += serialized.item_count();
            let write: Int<Result<(), _>> = if item_count > 0 {
                Int::NoWork
            } else if self.outbuf.len() > 0 {
                let (b, w) = self.outbuf_conn();
                pin_mut!(w);
                match w.poll_write(cx, b) {
                    Ready(k) => match k {
                        Ok(k) => match self.outbuf.adv(k) {
                            Ok(()) => {
                                info!("sent {} bytes", k);
                                self.outbuf.rewind_if_needed();
                                Int::Empty
                            }
                            Err(e) => {
                                error!("advance error {:?}", e);
                                Int::Item(Err(e))
                            }
                        },
                        Err(e) => {
                            error!("output write error {:?}", e);
                            Int::Item(Err(e.into()))
                        }
                    },
                    Pending => Int::Pending,
                }
            } else {
                Int::NoWork
            };
            info!("write result: {:?}  {}", write, self.outbuf.len());
            item_count += write.item_count();
            let read: Int<Result<(), _>> = if item_count > 0 || self.inp_eof {
                Int::NoWork
            } else {
                if self.buf.cap() < self.conn_state.need_min() {
                    self.done = true;
                    let e = Error::with_msg_no_trace(format!(
                        "buffer too small for need_min  {}  {}",
                        self.buf.cap(),
                        self.conn_state.need_min()
                    ));
                    Int::Item(Err(e))
                } else if self.buf.len() < self.conn_state.need_min() {
                    let (w, mut rbuf) = self.inpbuf_conn();
                    pin_mut!(w);
                    match w.poll_read(cx, &mut rbuf) {
                        Ready(k) => match k {
                            Ok(()) => {
                                let nf = rbuf.filled().len();
                                if nf == 0 {
                                    info!("EOF");
                                    self.inp_eof = true;
                                    Int::Done
                                } else {
                                    info!("received {} bytes", rbuf.filled().len());
                                    if false {
                                        let t = rbuf.filled().len();
                                        let t = if t < 32 { t } else { 32 };
                                        info!("got data  {:?}", &rbuf.filled()[0..t]);
                                    }
                                    match self.buf.wpadv(nf) {
                                        Ok(()) => Int::Empty,
                                        Err(e) => Int::Item(Err(e)),
                                    }
                                }
                            }
                            Err(e) => Int::Item(Err(e.into())),
                        },
                        Pending => Int::Pending,
                    }
                } else {
                    Int::NoWork
                }
            };
            item_count += read.item_count();
            let parsed = if item_count > 0 || self.buf.len() < self.conn_state.need_min() {
                Int::NoWork
            } else {
                match self.parse_item() {
                    Ok(k) => match k {
                        Some(k) => Int::Item(Ok(k)),
                        None => Int::Empty,
                    },
                    Err(e) => Int::Item(Err(e)),
                }
            };
            item_count += parsed.item_count();
            let _ = item_count;
            {
                use Int::*;
                match (serialized, write, read, parsed) {
                    (NoWork | Done, NoWork | Done, NoWork | Done, NoWork | Done) => {
                        warn!("all NoWork or Done");
                        break Poll::Pending;
                    }
                    (Item(Err(e)), _, _, _) => {
                        self.done = true;
                        break Poll::Ready(Some(Err(e)));
                    }
                    (_, Item(Err(e)), _, _) => {
                        self.done = true;
                        break Poll::Ready(Some(Err(e)));
                    }
                    (_, _, Item(Err(e)), _) => {
                        self.done = true;
                        break Poll::Ready(Some(Err(e)));
                    }
                    (_, _, _, Item(Err(e))) => {
                        self.done = true;
                        break Poll::Ready(Some(Err(e)));
                    }
                    (Item(_), _, _, _) => {
                        continue;
                    }
                    (_, Item(_), _, _) => {
                        continue;
                    }
                    (_, _, Item(_), _) => {
                        continue;
                    }
                    (_, _, _, Item(Ok(item))) => {
                        break Poll::Ready(Some(Ok(item)));
                    }
                    (Empty, _, _, _) => continue,
                    (_, Empty, _, _) => continue,
                    (_, _, Empty, _) => continue,
                    (_, _, _, Empty) => continue,
                    #[allow(unreachable_patterns)]
                    (Pending, Pending | NoWork | Done, Pending | NoWork | Done, Pending | NoWork | Done) => {
                        break Poll::Pending
                    }
                    #[allow(unreachable_patterns)]
                    (Pending | NoWork | Done, Pending, Pending | NoWork | Done, Pending | NoWork | Done) => {
                        break Poll::Pending
                    }
                    #[allow(unreachable_patterns)]
                    (Pending | NoWork | Done, Pending | NoWork | Done, Pending, Pending | NoWork | Done) => {
                        break Poll::Pending
                    }
                    #[allow(unreachable_patterns)]
                    (Pending | NoWork | Done, Pending | NoWork | Done, Pending | NoWork | Done, Pending) => {
                        break Poll::Pending
                    }
                }
            };
        }
    }
}
