use crate::frame::makeframe::{INMEM_FRAME_HEAD, INMEM_FRAME_MAGIC};
use bytes::{BufMut, Bytes, BytesMut};
use err::Error;
use futures_core::Stream;
use futures_util::pin_mut;
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
    pub fn new(inp: T) -> Self {
        // TODO make capacity adjustable.
        let bufcap = 512;
        let mut t = Self {
            inp,
            buf: BytesMut::new(),
            bufcap: bufcap,
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
        if self.wp > 0 {
            // TODO copy only if we gain capacity in the current buffer.
            // Also copy if the bufcap got increased: how to find out with BytesMut?  Question about how capacity is defined exactly...
            // Avoid copies after e.g. after a previous Pending.
            let mut bnew = self.empty_buf();
            assert!(self.buf.len() >= self.wp);
            assert!(bnew.capacity() >= self.wp);
            trace!(
                "InMemoryFrameAsyncReadStream  re-use {} bytes from previous i/o",
                self.wp,
            );
            bnew[0..].as_mut().put_slice(&self.buf[..self.wp]);
            self.buf = bnew;
        }
        trace!(
            "..............   PREPARE READ FROM  wp {}  self.buf.len() {}",
            self.wp,
            self.buf.len(),
        );
        let gg = self.buf.len() - self.wp;
        let mut buf2 = ReadBuf::new(&mut self.buf[self.wp..]);
        assert!(gg > 0);
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
        trace!(
            "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++   tryparse with  buf.len() {}  wp {}",
            buf.len(),
            wp
        );
        const HEAD: usize = INMEM_FRAME_HEAD;
        let mut buf = buf;
        let nb = wp;
        if nb >= HEAD {
            let magic = u32::from_le_bytes(*arrayref::array_ref![buf, 0, 4]);
            let encid = u32::from_le_bytes(*arrayref::array_ref![buf, 4, 4]);
            let tyid = u32::from_le_bytes(*arrayref::array_ref![buf, 8, 4]);
            let len = u32::from_le_bytes(*arrayref::array_ref![buf, 12, 4]);
            if magic != INMEM_FRAME_MAGIC {
                error!("InMemoryFrameAsyncReadStream  tryparse  incorrect magic: {}", magic);
                return (
                    Some(Some(Err(Error::with_msg(format!(
                        "InMemoryFrameAsyncReadStream  tryparse  incorrect magic: {}",
                        magic
                    ))))),
                    buf,
                    wp,
                );
            }
            trace!("tryparse len {}", len);
            if len == 0 {
                if nb != HEAD {
                    return (
                        Some(Some(Err(Error::with_msg(format!(
                            "InMemoryFrameAsyncReadStream  tryparse  unexpected amount left {}",
                            nb
                        ))))),
                        buf,
                        wp,
                    );
                }
                (Some(None), buf, wp)
            } else {
                if len > 1024 * 32 {
                    warn!("InMemoryFrameAsyncReadStream  big len received  {}", len);
                }
                if len > 1024 * 1024 * 2 {
                    error!("InMemoryFrameAsyncReadStream  too long len {}", len);
                    return (
                        Some(Some(Err(Error::with_msg(format!(
                            "InMemoryFrameAsyncReadStream  tryparse  huge buffer  len {}  self.inp_bytes_consumed {}",
                            len, self.inp_bytes_consumed
                        ))))),
                        buf,
                        wp,
                    );
                }
                if len == 0 && len > 1024 * 512 {
                    return (
                        Some(Some(Err(Error::with_msg(format!(
                            "InMemoryFrameAsyncReadStream  tryparse  len {}  self.inp_bytes_consumed {}",
                            len, self.inp_bytes_consumed
                        ))))),
                        buf,
                        wp,
                    );
                }
                let nl = len as usize + HEAD;
                if self.bufcap < nl {
                    // TODO count cases in production
                    let n = 2 * nl;
                    warn!("Adjust bufcap  old {}  new {}", self.bufcap, n);
                    self.bufcap = n;
                }
                if nb >= nl {
                    use bytes::Buf;
                    let mut buf3 = buf.split_to(nl);
                    buf3.advance(HEAD);
                    self.inp_bytes_consumed += nl as u64;
                    let ret = InMemoryFrame {
                        len,
                        tyid,
                        encid,
                        buf: buf3.freeze(),
                    };
                    (Some(Some(Ok(ret))), buf, wp - nl)
                } else {
                    (None, buf, wp)
                }
            }
        } else {
            (None, buf, wp)
        }
    }
}

pub struct InMemoryFrame {
    encid: u32,
    tyid: u32,
    len: u32,
    buf: Bytes,
}

impl InMemoryFrame {
    pub fn encid(&self) -> u32 {
        self.encid
    }
    pub fn tyid(&self) -> u32 {
        self.tyid
    }
    pub fn len(&self) -> u32 {
        self.len
    }
    pub fn buf(&self) -> &Bytes {
        &self.buf
    }
}

impl<T> Stream for InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<InMemoryFrame, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        trace!("InMemoryFrameAsyncReadStream  poll_next");
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
                    Some(Some(Ok(k))) => Ready(Some(Ok(k))),
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
                        trace!("poll_upstream  GIVES  Ready  {}", n1);
                        self.wp += n1;
                        if n1 == 0 {
                            let n2 = self.buf.len();
                            if n2 != 0 {
                                warn!(
                                    "InMemoryFrameAsyncReadStream  n2 != 0  n2 {}  consumed {}  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~",
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
