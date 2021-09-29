use crate::dtflags::{ARRAY, BIG_ENDIAN, COMPRESSION, SHAPE};
use bytes::{Bytes, BytesMut};
use err::Error;
use futures_core::Stream;
use futures_util::future::FusedFuture;
use futures_util::StreamExt;
use netpod::histo::HistoLog2;
use netpod::{log::*, FileIoBufferSize};
use netpod::{ChannelConfig, Node, Shape};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use std::{fmt, mem};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncRead, ReadBuf};

pub mod agg;
#[cfg(test)]
pub mod aggtest;
pub mod binned;
pub mod binnedstream;
pub mod cache;
pub mod channelexec;
pub mod dataopen;
pub mod decode;
pub mod eventblobs;
pub mod eventchunker;
pub mod events;
pub mod frame;
pub mod gen;
pub mod index;
pub mod merge;
pub mod mergeblobs;
pub mod paths;
pub mod rangefilter;
pub mod raw;
pub mod streamlog;

// TODO transform this into a self-test or remove.
pub async fn read_test_1(query: &netpod::AggQuerySingleChannel, node: Node) -> Result<netpod::BodyStream, Error> {
    let path = paths::datapath(query.timebin as u64, &query.channel_config, 0, &node);
    debug!("try path: {:?}", path);
    let fin = OpenOptions::new().read(true).open(path).await?;
    let meta = fin.metadata().await;
    debug!("file meta {:?}", meta);
    let stream = netpod::BodyStream {
        inner: Box::new(FileReader {
            file: fin,
            nreads: 0,
            buffer_size: query.buffer_size,
        }),
    };
    Ok(stream)
}

struct FileReader {
    file: tokio::fs::File,
    nreads: u32,
    buffer_size: u32,
}

impl Stream for FileReader {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let blen = self.buffer_size as usize;
        let mut buf2 = BytesMut::with_capacity(blen);
        buf2.resize(buf2.capacity(), 0);
        if buf2.as_mut().len() != blen {
            panic!("logic");
        }
        let mut buf = tokio::io::ReadBuf::new(buf2.as_mut());
        if buf.filled().len() != 0 {
            panic!("logic");
        }
        match Pin::new(&mut self.file).poll_read(cx, &mut buf) {
            Poll::Ready(Ok(_)) => {
                let rlen = buf.filled().len();
                if rlen == 0 {
                    Poll::Ready(None)
                } else {
                    if rlen != blen {
                        info!("short read  {} of {}", buf.filled().len(), blen);
                    }
                    self.nreads += 1;
                    Poll::Ready(Some(Ok(buf2.freeze())))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(Error::from(e)))),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct Fopen1 {
    #[allow(dead_code)]
    opts: OpenOptions,
    fut: Pin<Box<dyn Future<Output = Result<File, std::io::Error>>>>,
    term: bool,
}

impl Fopen1 {
    pub fn new(path: PathBuf) -> Self {
        let fut = Box::pin(async {
            let mut o1 = OpenOptions::new();
            let o2 = o1.read(true);
            let res = o2.open(path);
            res.await
        }) as Pin<Box<dyn Future<Output = Result<File, std::io::Error>>>>;
        let _fut2: Box<dyn Future<Output = u32>> = Box::new(async { 123 });
        Self {
            opts: OpenOptions::new(),
            fut,
            term: false,
        }
    }
}

impl Future for Fopen1 {
    type Output = Result<File, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let g = self.fut.as_mut();
        match g.poll(cx) {
            Poll::Ready(Ok(k)) => {
                self.term = true;
                Poll::Ready(Ok(k))
            }
            Poll::Ready(Err(k)) => {
                self.term = true;
                Poll::Ready(Err(k.into()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl FusedFuture for Fopen1 {
    fn is_terminated(&self) -> bool {
        self.term
    }
}

unsafe impl Send for Fopen1 {}

pub struct FileChunkRead {
    buf: BytesMut,
    cap0: usize,
    rem0: usize,
    remmut0: usize,
    duration: Duration,
}

impl fmt::Debug for FileChunkRead {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FileChunkRead")
            .field("buf.len", &self.buf.len())
            .field("buf.cap", &self.buf.capacity())
            .field("cap0", &self.cap0)
            .field("rem0", &self.rem0)
            .field("remmut0", &self.remmut0)
            .field("duration", &self.duration)
            .finish()
    }
}

pub struct FileContentStream {
    file: File,
    file_io_buffer_size: FileIoBufferSize,
    read_going: bool,
    buf: BytesMut,
    ts1: Instant,
    nlog: usize,
    done: bool,
    complete: bool,
}

impl FileContentStream {
    pub fn new(file: File, file_io_buffer_size: FileIoBufferSize) -> Self {
        Self {
            file,
            file_io_buffer_size,
            read_going: false,
            buf: BytesMut::new(),
            ts1: Instant::now(),
            nlog: 0,
            done: false,
            complete: false,
        }
    }
}

impl Stream for FileContentStream {
    type Item = Result<FileChunkRead, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll_next on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else {
                let mut buf = if !self.read_going {
                    self.ts1 = Instant::now();
                    let mut buf = BytesMut::new();
                    buf.resize(self.file_io_buffer_size.0, 0);
                    buf
                } else {
                    mem::replace(&mut self.buf, BytesMut::new())
                };
                let mutsl = buf.as_mut();
                let mut rb = ReadBuf::new(mutsl);
                let f1 = &mut self.file;
                let f2 = Pin::new(f1);
                let pollres = AsyncRead::poll_read(f2, cx, &mut rb);
                match pollres {
                    Ready(Ok(_)) => {
                        let nread = rb.filled().len();
                        let cap0 = rb.capacity();
                        let rem0 = rb.remaining();
                        let remmut0 = nread;
                        buf.truncate(nread);
                        self.read_going = false;
                        let ts2 = Instant::now();
                        if nread == 0 {
                            let ret = FileChunkRead {
                                buf,
                                cap0,
                                rem0,
                                remmut0,
                                duration: ts2.duration_since(self.ts1),
                            };
                            self.done = true;
                            Ready(Some(Ok(ret)))
                        } else {
                            let ret = FileChunkRead {
                                buf,
                                cap0,
                                rem0,
                                remmut0,
                                duration: ts2.duration_since(self.ts1),
                            };
                            if false && self.nlog < 6 {
                                self.nlog += 1;
                                info!("{:?}  ret {:?}", self.file_io_buffer_size, ret);
                            }
                            Ready(Some(Ok(ret)))
                        }
                    }
                    Ready(Err(e)) => {
                        self.done = true;
                        Ready(Some(Err(e.into())))
                    }
                    Pending => Pending,
                }
            };
        }
    }
}

pub fn file_content_stream(
    file: File,
    file_io_buffer_size: FileIoBufferSize,
) -> impl Stream<Item = Result<FileChunkRead, Error>> + Send {
    FileContentStream::new(file, file_io_buffer_size)
}

pub struct NeedMinBuffer {
    inp: Pin<Box<dyn Stream<Item = Result<FileChunkRead, Error>> + Send>>,
    need_min: u32,
    left: Option<FileChunkRead>,
    buf_len_histo: HistoLog2,
    errored: bool,
    completed: bool,
}

impl NeedMinBuffer {
    pub fn new(inp: Pin<Box<dyn Stream<Item = Result<FileChunkRead, Error>> + Send>>) -> Self {
        Self {
            inp: inp,
            need_min: 1,
            left: None,
            buf_len_histo: HistoLog2::new(8),
            errored: false,
            completed: false,
        }
    }

    pub fn put_back(&mut self, buf: FileChunkRead) {
        assert!(self.left.is_none());
        self.left = Some(buf);
    }

    pub fn set_need_min(&mut self, need_min: u32) {
        self.need_min = need_min;
    }
}

// TODO remove this again
impl Drop for NeedMinBuffer {
    fn drop(&mut self) {
        info!("NeedMinBuffer  Drop Stats:\nbuf_len_histo: {:?}", self.buf_len_histo);
    }
}

impl Stream for NeedMinBuffer {
    type Item = Result<FileChunkRead, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("NeedMinBuffer  poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        loop {
            let mut again = false;
            let z = match self.inp.poll_next_unpin(cx) {
                Ready(Some(Ok(fcr))) => {
                    self.buf_len_histo.ingest(fcr.buf.len() as u32);
                    //info!("NeedMinBuffer got buf  len {}", fcr.buf.len());
                    match self.left.take() {
                        Some(mut lfcr) => {
                            // TODO measure:
                            lfcr.buf.unsplit(fcr.buf);
                            lfcr.duration += fcr.duration;
                            let fcr = lfcr;
                            if fcr.buf.len() as u32 >= self.need_min {
                                //info!("with left ready  len {}  need_min {}", buf.len(), self.need_min);
                                Ready(Some(Ok(fcr)))
                            } else {
                                //info!("with left not enough  len {}  need_min {}", buf.len(), self.need_min);
                                self.left.replace(fcr);
                                again = true;
                                Pending
                            }
                        }
                        None => {
                            if fcr.buf.len() as u32 >= self.need_min {
                                //info!("simply ready  len {}  need_min {}", buf.len(), self.need_min);
                                Ready(Some(Ok(fcr)))
                            } else {
                                //info!("no previous leftover, need more  len {}  need_min {}", buf.len(), self.need_min);
                                self.left.replace(fcr);
                                again = true;
                                Pending
                            }
                        }
                    }
                }
                Ready(Some(Err(e))) => Ready(Some(Err(e.into()))),
                Ready(None) => {
                    info!("NeedMinBuffer  histo: {:?}", self.buf_len_histo);
                    Ready(None)
                }
                Pending => Pending,
            };
            if !again {
                break z;
            }
        }
    }
}

pub mod dtflags {
    pub const COMPRESSION: u8 = 0x80;
    pub const ARRAY: u8 = 0x40;
    pub const BIG_ENDIAN: u8 = 0x20;
    pub const SHAPE: u8 = 0x10;
}

trait ChannelConfigExt {
    fn dtflags(&self) -> u8;
}

impl ChannelConfigExt for ChannelConfig {
    fn dtflags(&self) -> u8 {
        let mut ret = 0;
        if self.compression {
            ret |= COMPRESSION;
        }
        match self.shape {
            Shape::Scalar => {}
            Shape::Wave(_) => {
                ret |= SHAPE;
            }
            Shape::Image(_, _) => {
                ret |= SHAPE;
            }
        }
        if self.byte_order.is_be() {
            ret |= BIG_ENDIAN;
        }
        if self.array {
            ret |= ARRAY;
        }
        ret
    }
}

pub trait HasSeenBeforeRangeCount {
    fn seen_before_range_count(&self) -> usize;
}
