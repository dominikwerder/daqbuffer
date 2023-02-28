use crate::slidebuf::SlideBuf;
use bytes::Bytes;
use err::Error;
use futures_util::pin_mut;
use futures_util::Stream;
use items_0::streamitem::StreamItem;
use items_0::streamitem::TERM_FRAME_TYPE_ID;
use items_2::framable::INMEM_FRAME_FOOT;
use items_2::framable::INMEM_FRAME_HEAD;
use items_2::framable::INMEM_FRAME_MAGIC;
use items_2::inmem::InMemoryFrame;
use netpod::log::*;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::io::AsyncRead;
use tokio::io::ReadBuf;

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => ();
    ($($arg:tt)*) => (trace!($($arg)*));
}

impl err::ToErr for crate::slidebuf::Error {
    fn to_err(self) -> Error {
        Error::with_msg_no_trace(format!("{self}"))
    }
}

/// Interprets a byte stream as length-delimited frames.
///
/// Emits each frame as a single item. Therefore, each item must fit easily into memory.
pub struct InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    inp: T,
    buf: SlideBuf,
    need_min: usize,
    done: bool,
    complete: bool,
    inp_bytes_consumed: u64,
}

impl<T> InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    pub fn new(inp: T, bufcap: usize) -> Self {
        Self {
            inp,
            buf: SlideBuf::new(bufcap),
            need_min: INMEM_FRAME_HEAD,
            done: false,
            complete: false,
            inp_bytes_consumed: 0,
        }
    }

    fn poll_upstream(&mut self, cx: &mut Context) -> Poll<Result<usize, Error>> {
        trace2!("poll_upstream");
        use Poll::*;
        let mut buf = ReadBuf::new(self.buf.available_writable_area(self.need_min - self.buf.len())?);
        let inp = &mut self.inp;
        pin_mut!(inp);
        match AsyncRead::poll_read(inp, cx, &mut buf) {
            Ready(Ok(())) => {
                let n = buf.filled().len();
                self.buf.wadv(n)?;
                trace!("recv bytes {}", n);
                Ready(Ok(n))
            }
            Ready(Err(e)) => Ready(Err(e.into())),
            Pending => Pending,
        }
    }

    // Try to consume bytes to parse a frame.
    // Update the need_min to the most current state.
    // Must only be called when at least `need_min` bytes are available.
    fn parse(&mut self) -> Result<Option<InMemoryFrame>, Error> {
        let buf = self.buf.data();
        if buf.len() < self.need_min {
            return Err(Error::with_msg_no_trace("expect at least need_min"));
        }
        if buf.len() < INMEM_FRAME_HEAD {
            return Err(Error::with_msg_no_trace("expect at least enough bytes for the header"));
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into()?);
        let encid = u32::from_le_bytes(buf[4..8].try_into()?);
        let tyid = u32::from_le_bytes(buf[8..12].try_into()?);
        let len = u32::from_le_bytes(buf[12..16].try_into()?);
        let payload_crc_exp = u32::from_le_bytes(buf[16..20].try_into()?);
        if magic != INMEM_FRAME_MAGIC {
            let n = buf.len().min(64);
            let u = String::from_utf8_lossy(&buf[0..n]);
            let msg = format!(
                "InMemoryFrameAsyncReadStream  tryparse  incorrect magic: {}  buf as utf8: {:?}",
                magic, u
            );
            error!("{msg}");
            return Err(Error::with_msg(msg));
        }
        if len > 1024 * 1024 * 50 {
            let msg = format!(
                "InMemoryFrameAsyncReadStream  tryparse  huge buffer  len {}  self.inp_bytes_consumed {}",
                len, self.inp_bytes_consumed
            );
            error!("{msg}");
            return Err(Error::with_msg(msg));
        }
        let lentot = INMEM_FRAME_HEAD + INMEM_FRAME_FOOT + len as usize;
        if buf.len() < lentot {
            // TODO count cases in production
            self.need_min = lentot;
            return Ok(None);
        }
        let p1 = INMEM_FRAME_HEAD + len as usize;
        let mut h = crc32fast::Hasher::new();
        h.update(&buf[..p1]);
        let frame_crc = h.finalize();
        let mut h = crc32fast::Hasher::new();
        h.update(&buf[INMEM_FRAME_HEAD..p1]);
        let payload_crc = h.finalize();
        let frame_crc_ind = u32::from_le_bytes(buf[p1..p1 + 4].try_into()?);
        let payload_crc_match = payload_crc_exp == payload_crc;
        let frame_crc_match = frame_crc_ind == frame_crc;
        if !frame_crc_match || !payload_crc_match {
            let _ss = String::from_utf8_lossy(&buf[..buf.len().min(256)]);
            let msg = format!(
                "InMemoryFrameAsyncReadStream  tryparse  crc mismatch A  {}  {}",
                payload_crc_match, frame_crc_match,
            );
            error!("{msg}");
            let e = Error::with_msg_no_trace(msg);
            return Err(e);
        }
        self.inp_bytes_consumed += lentot as u64;
        trace!("parsed frame well  len {}", len);
        let ret = InMemoryFrame {
            len,
            tyid,
            encid,
            buf: Bytes::from(buf[INMEM_FRAME_HEAD..p1].to_vec()),
        };
        self.buf.adv(lentot)?;
        self.need_min = INMEM_FRAME_HEAD;
        Ok(Some(ret))
    }
}

impl<T> Stream for InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<StreamItem<InMemoryFrame>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let span = span!(Level::INFO, "InMemRd");
        let _spanguard = span.enter();
        loop {
            break if self.complete {
                panic!("poll_next on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if self.buf.len() >= self.need_min {
                match self.parse() {
                    Ok(None) => {
                        if self.buf.len() >= self.need_min {
                            self.done = true;
                            let e = Error::with_msg_no_trace("enough bytes but nothing parsed");
                            Ready(Some(Err(e)))
                        } else {
                            continue;
                        }
                    }
                    Ok(Some(item)) => {
                        if item.tyid() == TERM_FRAME_TYPE_ID {
                            self.done = true;
                            continue;
                        } else {
                            Ready(Some(Ok(StreamItem::DataItem(item))))
                        }
                    }
                    Err(e) => {
                        self.done = true;
                        Ready(Some(Err(e)))
                    }
                }
            } else {
                match self.poll_upstream(cx) {
                    Ready(Ok(n1)) => {
                        if n1 == 0 {
                            self.done = true;
                            continue;
                        } else {
                            continue;
                        }
                    }
                    Ready(Err(e)) => {
                        error!("poll_upstream  need_min {}  buf {:?}  {:?}", self.need_min, self.buf, e);
                        self.done = true;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                }
            };
        }
    }
}
