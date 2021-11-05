use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, StreamExt};
use netpod::log::*;
use std::fmt;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

use crate::bsread::parse_zmtp_message;

#[test]
fn test_listen() -> Result<(), Error> {
    use std::time::Duration;
    let fut = async move {
        let _ = tokio::time::timeout(Duration::from_millis(16000), zmtp_client("camtest:9999")).await;
        Ok::<_, Error>(())
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
    let mut zmtp = Zmtp::new(conn);
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
    ReadFrameBody,
}

struct Zmtp {
    done: bool,
    complete: bool,
    conn: TcpStream,
    conn_state: ConnState,
    buf: NetBuf,
    outbuf: NetBuf,
    need_min: usize,
    msglen: usize,
    has_more: bool,
    is_command: bool,
    frames: Vec<ZmtpFrame>,
}

impl Zmtp {
    fn new(conn: TcpStream) -> Self {
        //conn.set_send_buffer_size(1024 * 64)?;
        //conn.set_recv_buffer_size(1024 * 1024 * 4)?;
        //info!("send_buffer_size  {:8}", conn.send_buffer_size()?);
        //info!("recv_buffer_size  {:8}", conn.recv_buffer_size()?);
        Self {
            done: false,
            complete: false,
            conn,
            conn_state: ConnState::InitSend,
            buf: NetBuf::new(),
            outbuf: NetBuf::new(),
            need_min: 0,
            msglen: 0,
            has_more: false,
            is_command: false,
            frames: vec![],
        }
    }

    fn buf_conn(&mut self) -> (&mut TcpStream, ReadBuf) {
        (&mut self.conn, self.buf.read_buf_for_fill())
    }

    fn outbuf_conn(&mut self) -> (&[u8], &mut TcpStream) {
        let b = &self.outbuf.buf[self.outbuf.rp..self.outbuf.wp];
        let w = &mut self.conn;
        (b, w)
    }

    fn parse_item(&mut self) -> Result<Option<ZmtpEvent>, Error> {
        match self.conn_state {
            ConnState::InitSend => {
                info!("parse_item  InitSend");
                // TODO allow to specify a minimum amount of needed space.
                // TODO factor writing into the buffer in some way...
                let mut b = self.outbuf.read_buf_for_fill();
                b.put_slice(&[0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 3]);
                self.outbuf.wp += b.filled().len();
                self.conn_state = ConnState::InitRecv1;
                self.need_min = 11;
                Ok(None)
            }
            ConnState::InitRecv1 => {
                let ver = self.buf.buf[self.buf.rp + 10];
                self.buf.rp += self.need_min;
                info!("parse_item  InitRecv1  major version {}", ver);
                if ver != 3 {
                    return Err(Error::with_msg_no_trace(format!("bad version {}", ver)));
                }
                let mut b = self.outbuf.read_buf_for_fill();
                b.put_slice(&[0, 0x4e, 0x55, 0x4c, 0x4c]);
                let a = vec![0; 48];
                b.put_slice(&a);
                self.outbuf.wp += b.filled().len();
                self.conn_state = ConnState::InitRecv2;
                self.need_min = 53;
                Ok(None)
            }
            ConnState::InitRecv2 => {
                info!("parse_item  InitRecv2");
                self.buf.rp += self.need_min;
                let mut b = self.outbuf.read_buf_for_fill();
                b.put_slice(&b"\x04\x1a\x05READY\x0bSocket-Type\x00\x00\x00\x04PULL"[..]);
                self.outbuf.wp += b.filled().len();
                self.conn_state = ConnState::ReadFrameFlags;
                self.need_min = 1;
                Ok(None)
            }
            ConnState::ReadFrameFlags => {
                let flags = self.buf.buf[self.buf.rp + 0];
                self.buf.rp += self.need_min;
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
                if false && is_command {
                    return Err(Error::with_msg_no_trace("got zmtp command frame"));
                }
                if long_size {
                    self.conn_state = ConnState::ReadFrameLong;
                    self.need_min = 8;
                } else {
                    self.conn_state = ConnState::ReadFrameShort;
                    self.need_min = 1;
                }
                Ok(None)
            }
            ConnState::ReadFrameShort => {
                let len = self.buf.buf[self.buf.rp + 0];
                self.buf.rp += self.need_min;
                self.msglen = len as usize;
                trace!("parse_item  ReadFrameShort  self.msglen {}", self.msglen);
                self.conn_state = ConnState::ReadFrameBody;
                self.need_min = self.msglen;
                Ok(None)
            }
            ConnState::ReadFrameLong => {
                let mut a = [0; 8];
                for i1 in 0..8 {
                    a[i1] = self.buf.buf[self.buf.rp + i1];
                }
                self.buf.rp += self.need_min;
                self.msglen = usize::from_be_bytes(a);
                trace!("parse_item  ReadFrameLong  self.msglen {}", self.msglen);
                self.conn_state = ConnState::ReadFrameBody;
                self.need_min = self.msglen;
                Ok(None)
            }
            ConnState::ReadFrameBody => {
                let n1 = self.buf.len();
                let n1 = if n1 < 256 { n1 } else { 256 };
                let data = self.buf.buf[self.buf.rp..(self.buf.rp + self.msglen)].to_vec();
                if false {
                    let s = String::from_utf8_lossy(&self.buf.buf[self.buf.rp..(self.buf.rp + n1)]);
                    trace!(
                        "parse_item  ReadFrameBody  self.need_min {}  string {}",
                        self.need_min,
                        s
                    );
                }
                self.buf.rp += self.need_min;
                self.conn_state = ConnState::ReadFrameFlags;
                self.need_min = 1;
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

struct NetBuf {
    buf: Vec<u8>,
    wp: usize,
    rp: usize,
}

impl NetBuf {
    fn new() -> Self {
        Self {
            buf: vec![0; 1024 * 128],
            wp: 0,
            rp: 0,
        }
    }

    fn len(&self) -> usize {
        self.wp - self.rp
    }

    fn read_buf_for_fill(&mut self) -> ReadBuf {
        self.rewind_if_needed();
        let read_buf = ReadBuf::new(&mut self.buf[self.wp..]);
        read_buf
    }

    fn rewind_if_needed(&mut self) {
        if self.rp != 0 && self.rp == self.wp {
            self.rp = 0;
            self.wp = 0;
        } else {
            self.buf.copy_within(self.rp..self.wp, 0);
            self.wp -= self.rp;
            self.rp = 0;
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
        'outer: loop {
            let write_pending = loop {
                if self.outbuf.len() > 0 {
                    let (b, w) = self.outbuf_conn();
                    pin_mut!(w);
                    match w.poll_write(cx, b) {
                        Ready(k) => match k {
                            Ok(k) => {
                                self.outbuf.rp += k;
                                info!("sent {} bytes", k);
                            }
                            Err(e) => {
                                self.done = true;
                                break 'outer Ready(Some(Err(e.into())));
                            }
                        },
                        Pending => break true,
                    }
                } else {
                    break false;
                }
            };
            let read_pending = loop {
                if self.buf.len() < self.need_min {
                    let nf1 = self.buf.buf.len() - self.buf.rp;
                    let nf2 = self.need_min;
                    let (w, mut rbuf) = self.buf_conn();
                    if nf1 < nf2 {
                        break 'outer Ready(Some(Err(Error::with_msg_no_trace("buffer too small for need_min"))));
                    }
                    pin_mut!(w);
                    let r = w.poll_read(cx, &mut rbuf);
                    match r {
                        Ready(k) => match k {
                            Ok(_) => {
                                info!("received {} bytes", rbuf.filled().len());
                                if false {
                                    let t = rbuf.filled().len();
                                    let t = if t < 32 { t } else { 32 };
                                    info!("got data  {:?}", &rbuf.filled()[0..t]);
                                }
                                self.buf.wp += rbuf.filled().len();
                            }
                            Err(e) => {
                                self.done = true;
                                break 'outer Ready(Some(Err(e.into())));
                            }
                        },
                        Pending => break true,
                    }
                } else {
                    break false;
                }
            };
            if self.buf.len() >= self.need_min {
                match self.parse_item() {
                    Ok(k) => match k {
                        Some(k) => break 'outer Ready(Some(Ok(k))),
                        None => (),
                    },
                    Err(e) => {
                        self.done = true;
                        break 'outer Ready(Some(Err(e.into())));
                    }
                }
            }
            if write_pending || read_pending {
                break 'outer Pending;
            }
        }
    }
}
