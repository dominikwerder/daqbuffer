#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use std::task::{Context, Poll};
use std::pin::Pin;
use tokio::io::AsyncRead;
use std::future::Future;
use futures_core::Stream;
use futures_util::future::FusedFuture;
use bytes::Bytes;
use std::path::PathBuf;

pub async fn read_test_1(query: &netpod::AggQuerySingleChannel) -> Result<netpod::BodyStream, Error> {
    let pre = "/data/sf-databuffer/daq_swissfel";
    let path = format!("{}/{}_{}/byTime/{}/{:019}/{:010}/{:019}_00000_Data", pre, query.ksprefix, query.keyspace, query.channel.name(), query.timebin, query.split, query.tbsize);
    debug!("try path: {}", path);
    let fin = tokio::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .await?;
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

impl futures_core::Stream for FileReader {
    type Item = Result<bytes::Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let blen = self.buffer_size as usize;
        let mut buf2 = bytes::BytesMut::with_capacity(blen);
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
                }
                else {
                    if rlen != blen {
                        info!("short read  {} of {}", buf.filled().len(), blen);
                    }
                    self.nreads += 1;
                    Poll::Ready(Some(Ok(buf2.freeze())))
                }
            }
            Poll::Ready(Err(e)) => {
                Poll::Ready(Some(Err(Error::from(e))))
            }
            Poll::Pending => Poll::Pending
        }
    }

}


struct Ftmp {
    file: Option<Box<dyn FusedFuture<Output=Result<tokio::fs::File, std::io::Error>> + Send>>,
    file2: Option<Box<dyn FusedFuture<Output=Result<tokio::fs::File, std::io::Error>> + Send + Unpin>>,
}

impl Ftmp {
    pub fn new() -> Self {
        Ftmp {
            file: None,
            file2: None,
        }
    }
    pub fn open(&mut self, path: PathBuf) {
        /*{
            let ff = tokio::fs::File::open(path.clone()).fuse();
            futures_util::pin_mut!(ff);
            let u: Box<dyn FusedFuture<Output=Result<tokio::fs::File, std::io::Error>> + Send + Unpin> = Box::new(ff);
            self.file2.replace(u);
        }*/
        //let z = tokio::fs::OpenOptions::new().read(true).open(path);
        //let y = Box::new(z);
        use futures_util::FutureExt;
        self.file.replace(Box::new(tokio::fs::File::open(path).fuse()));
    }
    pub fn is_empty(&self) -> bool {
        todo!()
    }
}


pub fn raw_concat_channel_read_stream(query: &netpod::AggQuerySingleChannel) -> impl Stream<Item=Result<Bytes, Error>> {
    use futures_util::{StreamExt, FutureExt, pin_mut, select};
    let mut query = query.clone();
    async_stream::stream! {
        let mut i1 = 0;
        loop {
            let timebin = 18700 + i1;
            query.timebin = timebin;
            let s2 = raw_concat_channel_read_stream_timebin(&query);
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


pub fn raw_concat_channel_read_stream_try_open_in_background(query: &netpod::AggQuerySingleChannel) -> impl Stream<Item=Result<Bytes, Error>> {
    use futures_util::{StreamExt, FutureExt, pin_mut, select};
    let mut query = query.clone();
    let mut ftmp1 = Ftmp::new();
    let mut ftmp2 = Ftmp::new();
    async_stream::stream! {
        let mut i1 = 0;
        loop {
            if ftmp1.is_empty() {
                let p2 = datapath(&query);
                ftmp1.open(p2);
                query.timebin += 1;
            }
            let timebin = 18700 + i1;
            let b = Box::new("");
            fn lala<H: Unpin>(h: H) {}
            lala(b);
            //query.timebin = timebin;
            //let s2 = raw_concat_channel_read_stream_timebin(&query);
            //pin_mut!(s2);
            //while let Some(item) = s2.next().await {
            //    yield item;
            //}
            //let s2f = s2.next().fuse();
            //pin_mut!(s2f);
            //pin_mut!(f2);
            let ff2 = ftmp1.file.take().unwrap();
            pin_mut!(ff2);
            //let i: Box<dyn FusedFuture<Output=Result<tokio::fs::File, std::io::Error>> + Send + Unpin> = ff2;
            let ff3 = Box::pin(ff2);
            pin_mut!(ff3);
            //let z = select! { _ = ff3 => () };
            yield Ok(Bytes::new());
        }
    }
}

fn datapath(query: &netpod::AggQuerySingleChannel) -> PathBuf {
    let pre = "/data/sf-databuffer/daq_swissfel";
    let path = format!("{}/{}_{}/byTime/{}/{:019}/{:010}/{:019}_00000_Data", pre, query.ksprefix, query.keyspace, query.channel.name(), query.timebin, query.split, query.tbsize);
    path.into()
}

pub fn raw_concat_channel_read_stream_timebin(query: &netpod::AggQuerySingleChannel) -> impl Stream<Item=Result<Bytes, Error>> {
    let query = query.clone();
    let pre = "/data/sf-databuffer/daq_swissfel";
    let path = format!("{}/{}_{}/byTime/{}/{:019}/{:010}/{:019}_00000_Data", pre, query.ksprefix, query.keyspace, query.channel.name(), query.timebin, query.split, query.tbsize);
    async_stream::stream! {
        debug!("try path: {}", path);
        let mut fin = tokio::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .await?;
        let meta = fin.metadata().await?;
        debug!("file meta {:?}", meta);
        let blen = query.buffer_size as usize;
        use tokio::io::AsyncReadExt;
        loop {
            let mut buf = bytes::BytesMut::with_capacity(blen);
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


pub async fn raw_concat_channel_read(query: &netpod::AggQuerySingleChannel) -> Result<netpod::BodyStream, Error> {
    let _reader = RawConcatChannelReader {
        ksprefix: query.ksprefix.clone(),
        keyspace: query.keyspace,
        channel: query.channel.clone(),
        split: query.split,
        tbsize: query.tbsize,
        buffer_size: query.buffer_size,
        tb: 18714,
        //file_reader: None,
        fopen: None,
    };
    todo!()
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
    fopen: Option<Box<dyn Future<Output=Option<Result<Bytes, Error>>> + Send>>,
}

impl RawConcatChannelReader {

    pub fn read(self) -> Result<netpod::BodyStream, Error> {
        let res = netpod::BodyStream {
            inner: Box::new(self),
        };
        Ok(res)
    }

}

impl futures_core::Stream for RawConcatChannelReader {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }

}
