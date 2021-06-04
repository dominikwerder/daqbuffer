use crate::dataopen::open_files;
use crate::dtflags::{ARRAY, BIG_ENDIAN, COMPRESSION, SHAPE};
use bytes::{Bytes, BytesMut};
use err::Error;
use futures_core::Stream;
use futures_util::future::FusedFuture;
use futures_util::{pin_mut, select, FutureExt, StreamExt};
use netpod::log::*;
use netpod::{ChannelConfig, NanoRange, Node, Shape};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncRead;

pub mod agg;
#[cfg(test)]
pub mod aggtest;
pub mod binned;
pub mod binnedstream;
pub mod cache;
pub mod channelconfig;
pub mod dataopen;
pub mod decode;
pub mod eventblobs;
pub mod eventchunker;
pub mod frame;
pub mod gen;
pub mod index;
pub mod merge;
pub mod paths;
pub mod raw;
pub mod streamlog;

pub async fn read_test_1(query: &netpod::AggQuerySingleChannel, node: Node) -> Result<netpod::BodyStream, Error> {
    let path = paths::datapath(query.timebin as u64, &query.channel_config, &node);
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

#[allow(dead_code)]
struct Fopen1 {
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
            //() == res;
            //todo!()
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

#[allow(dead_code)]
fn unused_raw_concat_channel_read_stream_try_open_in_background(
    query: &netpod::AggQuerySingleChannel,
    node: Node,
) -> impl Stream<Item = Result<Bytes, Error>> + Send {
    let query = query.clone();
    let node = node.clone();
    async_stream::stream! {
        use tokio::io::AsyncReadExt;
        let mut fopen = None;
        let mut fopen_avail = false;
        let mut file_prep: Option<File> = None;
        let mut file: Option<File> = None;
        let mut reading = None;
        let mut i1 = 0;
        let mut i9 = 0;
        loop {
            let blen = query.buffer_size as usize;
            {
                if !fopen_avail && file_prep.is_none() && i1 < 16 {
                    info!("Prepare open task for next file {}", query.timebin + i1);
                    fopen.replace(Fopen1::new(paths::datapath(query.timebin as u64 + i1 as u64, &query.channel_config, &node)));
                    fopen_avail = true;
                    i1 += 1;
                }
            }
            if !fopen_avail && file_prep.is_none() && file.is_none() && reading.is_none() {
                info!("Nothing more to do");
                break;
            }
            // TODO
            // When the file is available, I can prepare the next reading.
            // But next iteration, the file is not available, but reading is, so I should read!
            // I can not simply drop the reading future, that would lose the request.

            if let Some(read) = &mut reading {
                let k: Result<(tokio::fs::File, BytesMut), Error> = read.await;
                if k.is_err() {
                    error!("LONELY READ ERROR");
                }
                let k = k.unwrap();
                reading = None;
                file = Some(k.0);
                yield Ok(k.1.freeze());
            }
            else if fopen.is_some() {
                if file.is_some() {
                    if reading.is_none() {
                        let mut buf = BytesMut::with_capacity(blen);
                        let mut file2 = file.take().unwrap();
                        let a = async move {
                            file2.read_buf(&mut buf).await?;
                            Ok::<_, Error>((file2, buf))
                        };
                        let a = Box::pin(a);
                        reading = Some(a.fuse());
                    }
                    // TODO do I really have to take out the future while waiting on it?
                    // I think the issue is now with the mutex guard, can I get rid of the mutex again?
                    let mut fopen3 = fopen.take().unwrap();
                    let bufres = select! {
                        // TODO can I avoid the unwraps via matching already above?
                        f = fopen3 => {
                            fopen_avail = false;
                            // TODO feed out the potential error:
                            file_prep = Some(f.unwrap());
                            None
                        }
                        k = reading.as_mut().unwrap() => {
                            info!("COMBI  read chunk");
                            reading = None;
                            // TODO handle the error somehow here...
                            if k.is_err() {
                                error!("READ ERROR IN COMBI");
                            }
                            let k = k.unwrap();
                            file = Some(k.0);
                            Some(k.1)
                        }
                    };
                    if fopen_avail {
                        fopen.replace(fopen3);
                    }
                    if let Some(k) = bufres {
                        yield Ok(k.freeze());
                    }
                }
                else {
                    info!("-----------------   no file open yet, await only opening of the file");
                    // TODO try to avoid this duplicated code:
                    if fopen.is_none() {
                        error!("logic  BB");
                    }
                    let fopen3 = fopen.take().unwrap();
                    let f = fopen3.await?;
                    info!("opened next file SOLO");
                    fopen_avail = false;
                    file = Some(f);
                }
            }
            else if file.is_some() {
                loop {
                    let mut buf = BytesMut::with_capacity(blen);
                    let mut file2 = file.take().unwrap();
                    let n1 = file2.read_buf(&mut buf).await?;
                    if n1 == 0 {
                        if file_prep.is_some() {
                            file.replace(file_prep.take().unwrap());
                        }
                        else {
                            info!("After read loop, next file not yet ready");
                        }
                        break;
                    }
                    else {
                        file.replace(file2);
                        yield Ok(buf.freeze());
                    }
                }
            }
            i9 += 1;
            if i9 > 100 {
                break;
            }
        }
    }
}

#[allow(dead_code)]
fn unused_raw_concat_channel_read_stream_file_pipe(
    range: &NanoRange,
    channel_config: &ChannelConfig,
    node: Node,
    buffer_size: usize,
) -> impl Stream<Item = Result<BytesMut, Error>> + Send {
    let range = range.clone();
    let channel_config = channel_config.clone();
    let node = node.clone();
    async_stream::stream! {
        let chrx = open_files(&range, &channel_config, node);
        while let Ok(file) = chrx.recv().await {
            let mut file = match file {
                Ok(k) => k.file.unwrap(),
                Err(_) => break
            };
            loop {
                let mut buf = BytesMut::with_capacity(buffer_size);
                use tokio::io::AsyncReadExt;
                let n1 = file.read_buf(&mut buf).await?;
                if n1 == 0 {
                    info!("file EOF");
                    break;
                }
                else {
                    yield Ok(buf);
                }
            }
        }
    }
}

pub struct FileChunkRead {
    buf: BytesMut,
    duration: Duration,
}

pub fn file_content_stream(
    mut file: tokio::fs::File,
    buffer_size: usize,
) -> impl Stream<Item = Result<FileChunkRead, Error>> + Send {
    async_stream::stream! {
        use tokio::io::AsyncReadExt;
        loop {
            let mut buf = BytesMut::with_capacity(buffer_size);
            let inst1 = Instant::now();
            let n1 = file.read_buf(&mut buf).await?;
            let inst2 = Instant::now();
            if n1 == 0 {
                info!("file EOF");
                break;
            }
            else {
                let ret = FileChunkRead {
                    buf,
                    duration: inst2.duration_since(inst1),
                };
                yield Ok(ret);
            }
        }
    }
}

pub struct NeedMinBuffer {
    inp: Pin<Box<dyn Stream<Item = Result<FileChunkRead, Error>> + Send>>,
    need_min: u32,
    left: Option<FileChunkRead>,
    errored: bool,
    completed: bool,
}

impl NeedMinBuffer {
    pub fn new(inp: Pin<Box<dyn Stream<Item = Result<FileChunkRead, Error>> + Send>>) -> Self {
        Self {
            inp: inp,
            need_min: 1,
            left: None,
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
                    //info!("NeedMin got buf  len {}", buf.len());
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
                Ready(None) => Ready(None),
                Pending => Pending,
            };
            if !again {
                break z;
            }
        }
    }
}

pub fn raw_concat_channel_read_stream(
    query: &netpod::AggQuerySingleChannel,
    node: Node,
) -> impl Stream<Item = Result<Bytes, Error>> + Send {
    let mut query = query.clone();
    let node = node.clone();
    async_stream::stream! {
        let mut i1 = 0;
        loop {
            let timebin = 18700 + i1;
            query.timebin = timebin;
            let s2 = raw_concat_channel_read_stream_timebin(&query, node.clone());
            pin_mut!(s2);
            while let Some(item) = s2.next().await {
                yield item;
            }
            i1 += 1;
            if i1 > 15 {
                break;
            }
        }
    }
}

pub fn raw_concat_channel_read_stream_timebin(
    query: &netpod::AggQuerySingleChannel,
    node: Node,
) -> impl Stream<Item = Result<Bytes, Error>> {
    let query = query.clone();
    let node = node.clone();
    async_stream::stream! {
        let path = paths::datapath(query.timebin as u64, &query.channel_config, &node);
        debug!("try path: {:?}", path);
        let mut fin = OpenOptions::new().read(true).open(path).await?;
        let meta = fin.metadata().await?;
        debug!("file meta {:?}", meta);
        let blen = query.buffer_size as usize;
        use tokio::io::AsyncReadExt;
        loop {
            let mut buf = BytesMut::with_capacity(blen);
            assert!(buf.is_empty());
            if false {
                buf.resize(buf.capacity(), 0);
                if buf.as_mut().len() != blen {
                    panic!("logic");
                }
            }
            let n1 = fin.read_buf(&mut buf).await?;
            if n1 == 0 {
                break;
            }
            yield Ok(buf.freeze());
        }
    }
}

/**
Read all events from all timebins for the given channel and split.
*/
#[allow(dead_code)]
pub struct RawConcatChannelReader {
    ksprefix: String,
    keyspace: u32,
    channel: netpod::Channel,
    split: u32,
    tbsize: u32,
    buffer_size: u32,
    tb: u32,
    //file_reader: Option<FileReader>,

    // TODO
    // Not enough to store a simple future here.
    // That will only resolve to a single output.
    // • How can I transition between Stream and async world?
    // • I guess I must not poll a completed Future which comes from some async fn again after it completed.
    // • relevant crates:  async-stream, tokio-stream
    fopen: Option<Box<dyn Future<Output = Option<Result<Bytes, Error>>> + Send>>,
}

impl RawConcatChannelReader {
    pub fn read(self) -> Result<netpod::BodyStream, Error> {
        let res = netpod::BodyStream { inner: Box::new(self) };
        Ok(res)
    }
}

impl futures_core::Stream for RawConcatChannelReader {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
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
