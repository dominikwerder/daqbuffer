use err::thiserror;
use err::ThisError;
use futures_util::FutureExt;
use futures_util::Stream;
use items_2::binsdim0::BinsDim0;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::TsNano;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct Reading {
    fut: Pin<Box<dyn Future<Output = Result<BinsDim0<f32>, Box<dyn std::error::Error>>> + Send>>,
}

impl Future for Reading {
    type Output = Result<BinsDim0<f32>, Box<dyn std::error::Error>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}

pub trait CacheReadProvider: Send {
    fn read(&self) -> Reading;
}

#[derive(Debug, ThisError)]
#[cstm(name = "BinCachedReader")]
pub enum Error {}

pub struct CachedReader {
    cache_read_provider: Box<dyn CacheReadProvider>,
}

impl CachedReader {
    pub fn new(
        series: u64,
        bin_len: DtMs,
        range: BinnedRange<TsNano>,
        cache_read_provider: Box<dyn CacheReadProvider>,
    ) -> Result<Self, Error> {
        let ret = Self { cache_read_provider };
        Ok(ret)
    }
}

impl Stream for CachedReader {
    type Item = Result<BinsDim0<f32>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        Ready(None)
    }
}
