use bytes::{BufMut, BytesMut};
use err::Error;
use futures_core::Stream;
use futures_util::pin_mut;
use items::inmem::InMemoryFrame;
use items::StreamItem;
use items::{INMEM_FRAME_FOOT, INMEM_FRAME_HEAD, INMEM_FRAME_MAGIC};
use netpod::log::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/**
Interprets a byte stream as length-delimited frames.

Emits each frame as a single item. Therefore, each item must fit easily into memory.
*/
pub struct InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    inp: T,
    buf: BytesMut,
    bufcap: usize,
    wp: usize,
    tryparse: bool,
    errored: bool,
    completed: bool,
    inp_bytes_consumed: u64,
}

impl<T> InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    pub fn new(inp: T, bufcap: usize) -> Self {
        let mut t = Self {
            inp,
            buf: BytesMut::new(),
            bufcap,
            wp: 0,
            tryparse: false,
            errored: false,
            completed: false,
            inp_bytes_consumed: 0,
        };
        t.buf = t.empty_buf();
        t
    }

    fn empty_buf(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.bufcap);
        buf.resize(buf.capacity(), 0);
        buf
    }

    fn poll_upstream(&mut self, cx: &mut Context) -> Poll<Result<usize, Error>> {
        if true || self.wp > 0 {
            let mut bnew = self.empty_buf();
            assert!(self.buf.len() >= self.wp);
            assert!(bnew.capacity() >= self.wp);
            trace!(
                "InMemoryFrameAsyncReadStream  re-use {} bytes from previous i/o",
                self.wp,
            );
            bnew[..].as_mut().put_slice(&self.buf[..self.wp]);
            self.buf = bnew;
        }
        trace!("prepare read from  wp {}  self.buf.len() {}", self.wp, self.buf.len(),);
        let gg = self.buf.len() - self.wp;
        let mut buf2 = ReadBuf::new(&mut self.buf[self.wp..]);
        if gg < 1 || gg > 1024 * 1024 * 40 {
            use bytes::Buf;
            panic!(
                "have gg {}  len {}  cap {}  rem {}  rem mut {}  self.wp {}",
                gg,
                self.buf.len(),
                self.buf.capacity(),
                self.buf.remaining(),
                self.buf.remaining_mut(),
                self.wp,
            );
        }
        assert!(buf2.remaining() == gg);
        assert!(buf2.capacity() == gg);
        assert!(buf2.filled().len() == 0);
        let j = &mut self.inp;
        pin_mut!(j);
        use Poll::*;
        match AsyncRead::poll_read(j, cx, &mut buf2) {
            Ready(Ok(_)) => {
                let n1 = buf2.filled().len();
                trace!("InMemoryFrameAsyncReadStream  READ {} FROM UPSTREAM", n1);
                Ready(Ok(n1))
            }
            Ready(Err(e)) => Ready(Err(e.into())),
            Pending => Pending,
        }
    }

    fn tryparse(
        &mut self,
        buf: BytesMut,
        wp: usize,
    ) -> (Option<Option<Result<InMemoryFrame, Error>>>, BytesMut, usize) {
        let mut buf = buf;
        let nb = wp;
        if nb >= INMEM_FRAME_HEAD {
            let magic = u32::from_le_bytes(*arrayref::array_ref![buf, 0, 4]);
            let encid = u32::from_le_bytes(*arrayref::array_ref![buf, 4, 4]);
            let tyid = u32::from_le_bytes(*arrayref::array_ref![buf, 8, 4]);
            let len = u32::from_le_bytes(*arrayref::array_ref![buf, 12, 4]);
            if magic != INMEM_FRAME_MAGIC {
                let z = nb.min(32);
                let u = String::from_utf8_lossy(&buf[0..z]);
                let e = Error::with_msg("INCORRECT MAGIC");
                error!("incorrect magic  buf as utf8: {:?}  error: {:?}", u, e);
                let msg = format!(
                    "InMemoryFrameAsyncReadStream  tryparse  incorrect magic: {}  buf as utf8: {:?}",
                    magic, u
                );
                error!("{}", msg);
                return (Some(Some(Err(Error::with_msg(format!("{}", msg))))), buf, wp);
            }
            if len == 0 {
                if nb != INMEM_FRAME_HEAD + INMEM_FRAME_FOOT {
                    warn!("stop-frame with nb {}", nb);
                }
                (Some(None), buf, wp)
            } else {
                if len > 1024 * 1024 * 50 {
                    let msg = format!(
                        "InMemoryFrameAsyncReadStream  tryparse  huge buffer  len {}  self.inp_bytes_consumed {}",
                        len, self.inp_bytes_consumed
                    );
                    error!("{}", msg);
                    return (Some(Some(Err(Error::with_msg(msg)))), buf, wp);
                } else if len > 1024 * 1024 * 1 {
                    // TODO
                    //warn!("InMemoryFrameAsyncReadStream  big len received  {}", len);
                }
                let nl = len as usize + INMEM_FRAME_HEAD + INMEM_FRAME_FOOT;
                if self.bufcap < nl {
                    // TODO count cases in production
                    let n = 2 * nl;
                    debug!("Adjust bufcap  old {}  new {}", self.bufcap, n);
                    self.bufcap = n;
                }
                if nb < nl {
                    (None, buf, wp)
                } else {
                    use bytes::Buf;
                    let mut h = crc32fast::Hasher::new();
                    h.update(&buf[..(nl - INMEM_FRAME_FOOT)]);
                    let frame_crc = h.finalize();
                    let mut h = crc32fast::Hasher::new();
                    h.update(&buf[INMEM_FRAME_HEAD..(nl - INMEM_FRAME_FOOT)]);
                    let payload_crc = h.finalize();
                    let frame_crc_ind =
                        u32::from_le_bytes(*arrayref::array_ref![buf, INMEM_FRAME_HEAD + len as usize, 4]);
                    let payload_crc_ind = u32::from_le_bytes(*arrayref::array_ref![buf, 16, 4]);
                    let payload_crc_match = payload_crc_ind == payload_crc;
                    let frame_crc_match = frame_crc_ind == frame_crc;
                    if !payload_crc_match || !frame_crc_match {
                        return (
                            Some(Some(Err(Error::with_msg(format!(
                                "InMemoryFrameAsyncReadStream  tryparse  crc mismatch  {}  {}",
                                payload_crc_match, frame_crc_match,
                            ))))),
                            buf,
                            wp,
                        );
                    }
                    let mut buf3 = buf.split_to(nl);
                    buf3.advance(INMEM_FRAME_HEAD);
                    buf3.truncate(len as usize);
                    let mut h = crc32fast::Hasher::new();
                    h.update(&buf3);
                    let payload_crc_2 = h.finalize();
                    let payload_crc_2_match = payload_crc_2 == payload_crc_ind;
                    if !payload_crc_2_match {
                        return (
                            Some(Some(Err(Error::with_msg(format!(
                                "InMemoryFrameAsyncReadStream  tryparse  crc mismatch  {}  {}  {}",
                                payload_crc_match, frame_crc_match, payload_crc_2_match,
                            ))))),
                            buf,
                            wp,
                        );
                    }
                    self.inp_bytes_consumed += nl as u64;
                    let ret = InMemoryFrame {
                        len,
                        tyid,
                        encid,
                        buf: buf3.freeze(),
                    };
                    (Some(Some(Ok(ret))), buf, wp - nl)
                }
            }
        } else {
            (None, buf, wp)
        }
    }
}

impl<T> Stream for InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<StreamItem<InMemoryFrame>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        assert!(!self.completed);
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        'outer: loop {
            if self.tryparse {
                let r = {
                    let buf = std::mem::replace(&mut self.buf, BytesMut::new());
                    let wp = self.wp;
                    let (r, buf, wp) = self.tryparse(buf, wp);
                    self.buf = buf;
                    self.wp = wp;
                    r
                };
                break match r {
                    None => {
                        self.tryparse = false;
                        continue 'outer;
                    }
                    Some(None) => {
                        self.tryparse = false;
                        self.completed = true;
                        Ready(None)
                    }
                    Some(Some(Ok(item))) => Ready(Some(Ok(StreamItem::DataItem(item)))),
                    Some(Some(Err(e))) => {
                        self.tryparse = false;
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                };
            } else {
                let r = self.poll_upstream(cx);
                break match r {
                    Ready(Ok(n1)) => {
                        self.wp += n1;
                        if n1 == 0 {
                            let n2 = self.buf.len();
                            if n2 != 0 {
                                // TODO anything more to handle here?
                                debug!(
                                    "InMemoryFrameAsyncReadStream  n2 != 0  n2 {}  consumed {}",
                                    n2, self.inp_bytes_consumed
                                );
                            }
                            self.completed = true;
                            Ready(None)
                        } else {
                            self.tryparse = true;
                            continue 'outer;
                        }
                    }
                    Ready(Err(e)) => {
                        trace!("poll_upstream  GIVES  Error");
                        self.errored = true;
                        Ready(Some(Err(e.into())))
                    }
                    Pending => Pending,
                };
            }
        }
    }
}
