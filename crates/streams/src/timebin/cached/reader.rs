use crate as streams;
use err::thiserror;
use err::ThisError;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::Sitemty;
use items_0::timebin::TimeBinnable;
use items_2::binsdim0::BinsDim0;
use items_2::channelevents::ChannelEvents;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::ChConf;
use netpod::DtMs;
use netpod::TsNano;
use query::api4::events::EventsSubQuery;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

pub struct EventsReading {
    stream: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>,
}

impl EventsReading {
    pub fn new(stream: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>) -> Self {
        Self { stream }
    }
}

impl Stream for EventsReading {
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

pub trait EventsReadProvider: Send + Sync {
    fn read(&self, evq: EventsSubQuery, chconf: ChConf) -> EventsReading;
}

pub struct CacheReading {
    fut: Pin<Box<dyn Future<Output = Result<BinsDim0<f32>, Box<dyn std::error::Error + Send>>> + Send>>,
}

impl Future for CacheReading {
    type Output = Result<BinsDim0<f32>, Box<dyn std::error::Error + Send>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}

pub struct CacheWriting {
    fut: Pin<Box<dyn Future<Output = Result<(), streams::timebin::cached::reader::Error>> + Send>>,
}

impl CacheWriting {
    pub fn new(fut: Pin<Box<dyn Future<Output = Result<(), streams::timebin::cached::reader::Error>> + Send>>) -> Self {
        Self { fut }
    }
}

impl Future for CacheWriting {
    type Output = Result<(), streams::timebin::cached::reader::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}

pub trait CacheReadProvider: Send + Sync {
    fn read(&self, series: u64, range: BinnedRange<TsNano>) -> CacheReading;
    fn write(&self, series: u64, bins: BinsDim0<f32>) -> CacheWriting;
}

#[derive(Debug, ThisError)]
#[cstm(name = "BinCachedReader")]
pub enum Error {
    TodoImpl,
    ChannelSend,
    ChannelRecv,
    Scylla(String),
}

pub struct CachedReader {
    cache_read_provider: Arc<dyn CacheReadProvider>,
}

impl CachedReader {
    pub fn new(
        series: u64,
        bin_len: DtMs,
        range: BinnedRange<TsNano>,
        cache_read_provider: Arc<dyn CacheReadProvider>,
    ) -> Result<Self, Error> {
        let ret = Self { cache_read_provider };
        Ok(ret)
    }
}

impl Stream for CachedReader {
    type Item = Result<BinsDim0<f32>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        // TODO
        // Must split over different msp (because pkey).
        // If we choose the partitioning length low enough, no need to issue multiple queries.
        // Change the worker interface:
        // We should already compute here the msp and off because we must here implement the loop logic.
        // Therefore worker interface should not accept BinnedRange, but msp and off range.
        error!("TODO CachedReader impl split reads over known ranges");
        // Ready(Some(Err(Error::TodoImpl)))
        Ready(None)
    }
}
