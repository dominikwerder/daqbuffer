pub mod agg;
#[cfg(test)]
pub mod aggtest;
pub mod binnedstream;
pub mod cache;
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
pub mod read3;
pub mod read4;
pub mod streamlog;

use bytes::Bytes;
use bytes::BytesMut;
use err::Error;
use futures_core::Stream;
use futures_util::future::FusedFuture;
use futures_util::{FutureExt, StreamExt, TryFutureExt};
use netpod::log::*;
use netpod::ReadSys;
use netpod::{ChannelConfig, DiskIoTune, Node, Shape};
use std::collections::VecDeque;
use std::future::Future;
use std::io::SeekFrom;
use std::mem;
use std::os::unix::prelude::AsRawFd;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;
use streams::dtflags::{ARRAY, BIG_ENDIAN, COMPRESSION, SHAPE};
use streams::filechunkread::FileChunkRead;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::ReadBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc;

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
        err::todo();
        // TODO remove if no longer used?
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

pub struct FileContentStream {
    file: File,
    disk_io_tune: DiskIoTune,
    read_going: bool,
    buf: BytesMut,
    ts1: Instant,
    nlog: usize,
    done: bool,
    complete: bool,
}

impl FileContentStream {
    pub fn new(file: File, disk_io_tune: DiskIoTune) -> Self {
        Self {
            file,
            disk_io_tune,
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
                    buf.resize(self.disk_io_tune.read_buffer_len, 0);
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
                        buf.truncate(nread);
                        self.read_going = false;
                        let ts2 = Instant::now();
                        if nread == 0 {
                            let ret = FileChunkRead::with_buf_dur(buf, ts2.duration_since(self.ts1));
                            self.done = true;
                            Ready(Some(Ok(ret)))
                        } else {
                            let ret = FileChunkRead::with_buf_dur(buf, ts2.duration_since(self.ts1));
                            if false && self.nlog < 6 {
                                self.nlog += 1;
                                info!("{:?}  ret {:?}", self.disk_io_tune, ret);
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

fn start_read5(
    file: File,
    tx: async_channel::Sender<Result<FileChunkRead, Error>>,
    disk_io_tune: DiskIoTune,
) -> Result<(), Error> {
    let fut = async move {
        info!("start_read5 BEGIN {disk_io_tune:?}");
        let mut file = file;
        loop {
            let mut buf = BytesMut::new();
            buf.resize(1024 * 256, 0);
            match file.read(&mut buf).await {
                Ok(n) => {
                    buf.truncate(n);
                    let item = FileChunkRead::with_buf(buf);
                    match tx.send(Ok(item)).await {
                        Ok(()) => {}
                        Err(_e) => break,
                    }
                }
                Err(e) => match tx.send(Err(e.into())).await {
                    Ok(()) => {}
                    Err(_e) => break,
                },
            }
        }
        info!("start_read5 DONE");
    };
    tokio::task::spawn(fut);
    Ok(())
}

pub struct FileContentStream5 {
    rx: async_channel::Receiver<Result<FileChunkRead, Error>>,
}

impl FileContentStream5 {
    pub fn new(file: File, disk_io_tune: DiskIoTune) -> Result<Self, Error> {
        let (tx, rx) = async_channel::bounded(32);
        start_read5(file, tx, disk_io_tune)?;
        let ret = Self { rx };
        Ok(ret)
    }
}

impl Stream for FileContentStream5 {
    type Item = Result<FileChunkRead, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.rx.poll_next_unpin(cx)
    }
}

enum FCS2 {
    Idle,
    Reading(
        (
            Box<BytesMut>,
            Pin<Box<dyn Future<Output = Result<usize, Error>> + Send>>,
        ),
    ),
}

pub struct FileContentStream2 {
    fcs: FCS2,
    file: Pin<Box<File>>,
    disk_io_tune: DiskIoTune,
    done: bool,
    complete: bool,
}

impl FileContentStream2 {
    pub fn new(file: File, disk_io_tune: DiskIoTune) -> Self {
        let file = Box::pin(file);
        Self {
            fcs: FCS2::Idle,
            file,
            disk_io_tune,
            done: false,
            complete: false,
        }
    }

    fn make_reading(&mut self) {
        let mut buf = Box::new(BytesMut::with_capacity(self.disk_io_tune.read_buffer_len));
        let bufref = unsafe { &mut *((&mut buf as &mut BytesMut) as *mut BytesMut) };
        let fileref = unsafe { &mut *((&mut self.file) as *mut Pin<Box<File>>) };
        let fut = AsyncReadExt::read_buf(fileref, bufref).map_err(|e| e.into());
        self.fcs = FCS2::Reading((buf, Box::pin(fut)));
    }
}

impl Stream for FileContentStream2 {
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
                match self.fcs {
                    FCS2::Idle => {
                        self.make_reading();
                        continue;
                    }
                    FCS2::Reading((ref mut buf, ref mut fut)) => match fut.poll_unpin(cx) {
                        Ready(Ok(n)) => {
                            let buf2 = std::mem::replace(buf as &mut BytesMut, BytesMut::new());
                            let item = FileChunkRead::with_buf(buf2);
                            if n == 0 {
                                self.done = true;
                            } else {
                                self.make_reading();
                            }
                            Ready(Some(Ok(item)))
                        }
                        Ready(Err(e)) => {
                            self.done = true;
                            Ready(Some(Err(e.into())))
                        }
                        Pending => Pending,
                    },
                }
            };
        }
    }
}

enum FCS3 {
    GetPosition,
    ReadingSimple,
    Reading,
}

enum ReadStep {
    Fut(Pin<Box<dyn Future<Output = Result<read3::ReadResult, Error>> + Send>>),
    Res(Result<read3::ReadResult, Error>),
}

pub struct FileContentStream3 {
    fcs: FCS3,
    file: Pin<Box<File>>,
    file_pos: u64,
    eof: bool,
    disk_io_tune: DiskIoTune,
    get_position_fut: Pin<Box<dyn Future<Output = Result<u64, Error>> + Send>>,
    read_fut: Pin<Box<dyn Future<Output = Result<read3::ReadResult, Error>> + Send>>,
    reads: VecDeque<ReadStep>,
    done: bool,
    complete: bool,
}

impl FileContentStream3 {
    pub fn new(file: File, disk_io_tune: DiskIoTune) -> Self {
        let mut file = Box::pin(file);
        let ffr = unsafe {
            let ffr = Pin::get_unchecked_mut(file.as_mut());
            std::mem::transmute::<&mut File, &'static mut File>(ffr)
        };
        let ff = ffr
            .seek(SeekFrom::Current(0))
            .map_err(|_| Error::with_msg_no_trace(format!("Seek error")));
        Self {
            fcs: FCS3::GetPosition,
            file,
            file_pos: 0,
            eof: false,
            disk_io_tune,
            get_position_fut: Box::pin(ff),
            read_fut: Box::pin(futures_util::future::ready(Err(Error::with_msg_no_trace(format!(
                "dummy"
            ))))),
            reads: VecDeque::new(),
            done: false,
            complete: false,
        }
    }
}

impl Stream for FileContentStream3 {
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
                match self.fcs {
                    FCS3::GetPosition => match self.get_position_fut.poll_unpin(cx) {
                        Ready(Ok(k)) => {
                            info!("current file pos: {k}");
                            self.file_pos = k;
                            if false {
                                let fd = self.file.as_raw_fd();
                                let count = self.disk_io_tune.read_buffer_len as u64;
                                self.read_fut = Box::pin(read3::Read3::get().read(fd, self.file_pos, count));
                                self.file_pos += count;
                                self.fcs = FCS3::ReadingSimple;
                            } else {
                                self.fcs = FCS3::Reading;
                            }
                            continue;
                        }
                        Ready(Err(e)) => {
                            self.done = true;
                            Ready(Some(Err(e)))
                        }
                        Pending => Pending,
                    },
                    FCS3::ReadingSimple => match self.read_fut.poll_unpin(cx) {
                        Ready(Ok(res)) => {
                            if res.eof {
                                let item = FileChunkRead::with_buf(res.buf);
                                self.done = true;
                                Ready(Some(Ok(item)))
                            } else {
                                let item = FileChunkRead::with_buf(res.buf);
                                let fd = self.file.as_raw_fd();
                                let count = self.disk_io_tune.read_buffer_len as u64;
                                self.read_fut = Box::pin(read3::Read3::get().read(fd, self.file_pos, count));
                                self.file_pos += count;
                                Ready(Some(Ok(item)))
                            }
                        }
                        Ready(Err(e)) => {
                            self.done = true;
                            Ready(Some(Err(e)))
                        }
                        Pending => Pending,
                    },
                    FCS3::Reading => {
                        while !self.eof && self.reads.len() < self.disk_io_tune.read_queue_len {
                            let fd = self.file.as_raw_fd();
                            let pos = self.file_pos;
                            let count = self.disk_io_tune.read_buffer_len as u64;
                            trace!("create ReadTask  fd {fd}  pos {pos}  count {count}");
                            let r3 = read3::Read3::get();
                            let fut = r3.read(fd, pos, count);
                            self.reads.push_back(ReadStep::Fut(Box::pin(fut)));
                            self.file_pos += count;
                        }
                        for e in &mut self.reads {
                            match e {
                                ReadStep::Fut(k) => match k.poll_unpin(cx) {
                                    Ready(k) => {
                                        trace!("received a result");
                                        *e = ReadStep::Res(k);
                                    }
                                    Pending => {}
                                },
                                ReadStep::Res(_) => {}
                            }
                        }
                        if let Some(ReadStep::Res(_)) = self.reads.front() {
                            if let Some(ReadStep::Res(res)) = self.reads.pop_front() {
                                trace!("pop front result");
                                match res {
                                    Ok(rr) => {
                                        if rr.eof {
                                            if self.eof {
                                                trace!("see EOF in ReadResult  AGAIN");
                                            } else {
                                                debug!("see EOF in ReadResult  SET OUR FLAG");
                                                self.eof = true;
                                            }
                                        }
                                        let res = FileChunkRead::with_buf(rr.buf);
                                        Ready(Some(Ok(res)))
                                    }
                                    Err(e) => {
                                        error!("received ReadResult error: {e}");
                                        self.done = true;
                                        let e = Error::with_msg(format!("I/O error: {e}"));
                                        Ready(Some(Err(e)))
                                    }
                                }
                            } else {
                                self.done = true;
                                let e = Error::with_msg(format!("logic error"));
                                error!("{e}");
                                Ready(Some(Err(e)))
                            }
                        } else if let None = self.reads.front() {
                            debug!("empty read fut queue, end");
                            self.done = true;
                            continue;
                        } else {
                            trace!("read fut queue Pending");
                            Pending
                        }
                    }
                }
            };
        }
    }
}

enum FCS4 {
    Init,
    Setup,
    Reading,
}

pub struct FileContentStream4 {
    fcs: FCS4,
    file: Pin<Box<File>>,
    disk_io_tune: DiskIoTune,
    setup_fut:
        Option<Pin<Box<dyn Future<Output = Result<mpsc::Receiver<Result<read4::ReadResult, Error>>, Error>> + Send>>>,
    inp: Option<mpsc::Receiver<Result<read4::ReadResult, Error>>>,
    recv_fut: Pin<Box<dyn Future<Output = Option<Result<read4::ReadResult, Error>>> + Send>>,
    done: bool,
    complete: bool,
}

impl FileContentStream4 {
    pub fn new(file: File, disk_io_tune: DiskIoTune) -> Self {
        let file = Box::pin(file);
        Self {
            fcs: FCS4::Init,
            file,
            disk_io_tune,
            setup_fut: None,
            inp: None,
            recv_fut: Box::pin(futures_util::future::ready(Some(Err(Error::with_msg_no_trace(
                format!("dummy"),
            ))))),
            done: false,
            complete: false,
        }
    }
}

impl Stream for FileContentStream4 {
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
                match self.fcs {
                    FCS4::Init => {
                        let read4 = read4::Read4::get();
                        let fd = self.file.as_raw_fd();
                        let buflen = self.disk_io_tune.read_buffer_len as u64;
                        let fut = read4.read(fd, buflen, self.disk_io_tune.read_queue_len);
                        self.setup_fut = Some(Box::pin(fut) as _);
                        self.fcs = FCS4::Setup;
                        continue;
                    }
                    FCS4::Setup => match self.setup_fut.as_mut().unwrap().poll_unpin(cx) {
                        Ready(k) => match k {
                            Ok(k) => {
                                self.setup_fut = None;
                                self.fcs = FCS4::Reading;
                                self.inp = Some(k);
                                // TODO
                                let rm = self.inp.as_mut().unwrap();
                                let rm = unsafe {
                                    std::mem::transmute::<
                                        &mut mpsc::Receiver<Result<read4::ReadResult, Error>>,
                                        &'static mut mpsc::Receiver<Result<read4::ReadResult, Error>>,
                                    >(rm)
                                };
                                self.recv_fut = Box::pin(rm.recv()) as _;
                                continue;
                            }
                            Err(e) => {
                                self.done = true;
                                let e = Error::with_msg_no_trace(format!("init failed {e:?}"));
                                Ready(Some(Err(e)))
                            }
                        },
                        Pending => Pending,
                    },
                    FCS4::Reading => match self.recv_fut.poll_unpin(cx) {
                        Ready(k) => match k {
                            Some(k) => match k {
                                Ok(k) => {
                                    // TODO
                                    let rm = self.inp.as_mut().unwrap();
                                    let rm = unsafe {
                                        std::mem::transmute::<
                                            &mut mpsc::Receiver<Result<read4::ReadResult, Error>>,
                                            &'static mut mpsc::Receiver<Result<read4::ReadResult, Error>>,
                                        >(rm)
                                    };
                                    self.recv_fut = Box::pin(rm.recv()) as _;
                                    let item = FileChunkRead::with_buf(k.buf);
                                    Ready(Some(Ok(item)))
                                }
                                Err(e) => {
                                    self.done = true;
                                    let e = Error::with_msg_no_trace(format!("init failed {e:?}"));
                                    Ready(Some(Err(e)))
                                }
                            },
                            None => {
                                self.done = true;
                                continue;
                            }
                        },
                        Pending => Pending,
                    },
                }
            };
        }
    }
}

pub fn file_content_stream(
    file: File,
    disk_io_tune: DiskIoTune,
) -> Pin<Box<dyn Stream<Item = Result<FileChunkRead, Error>> + Send>> {
    debug!("file_content_stream  disk_io_tune {disk_io_tune:?}");
    match &disk_io_tune.read_sys {
        ReadSys::TokioAsyncRead => {
            let s = FileContentStream::new(file, disk_io_tune);
            Box::pin(s) as Pin<Box<dyn Stream<Item = _> + Send>>
        }
        ReadSys::Read2 => {
            let s = FileContentStream2::new(file, disk_io_tune);
            Box::pin(s) as _
        }
        ReadSys::Read3 => {
            let s = FileContentStream3::new(file, disk_io_tune);
            Box::pin(s) as _
        }
        ReadSys::Read4 => {
            let s = FileContentStream4::new(file, disk_io_tune);
            Box::pin(s) as _
        }
        ReadSys::Read5 => {
            let s = FileContentStream5::new(file, disk_io_tune).unwrap();
            Box::pin(s) as _
        }
    }
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
