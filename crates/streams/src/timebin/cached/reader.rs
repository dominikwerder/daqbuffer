use crate as streams;
use err::thiserror;
use err::ThisError;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::Sitemty;
use items_0::timebin::BinsBoxed;
use items_2::channelevents::ChannelEvents;
use items_2::eventsdim0::EventsDim0;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::EnumVariant;
use netpod::TsNano;
use query::api4::events::EventsSubQuery;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

#[allow(unused)]
macro_rules! trace_emit { ($($arg:tt)*) => ( if true { trace!($($arg)*); } ) }

pub fn off_max() -> u64 {
    1000
}

pub fn part_len(bin_len: DtMs) -> DtMs {
    DtMs::from_ms_u64(bin_len.ms() * off_max())
}

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
        use Poll::*;
        match self.stream.poll_next_unpin(cx) {
            Ready(Some(item)) => {
                use items_0::streamitem::RangeCompletableItem::*;
                use items_0::streamitem::StreamItem::*;
                match &item {
                    Ok(DataItem(Data(cevs))) => match cevs {
                        ChannelEvents::Events(evs) => {
                            if let Some(evs) = evs.as_any_ref().downcast_ref::<EventsDim0<EnumVariant>>() {
                                use items_0::Empty;
                                let mut dst = EventsDim0::<u16>::empty();
                                for ((&ts, &pulse), val) in evs
                                    .tss()
                                    .iter()
                                    .zip(evs.pulses.iter())
                                    .zip(evs.private_values_ref().iter())
                                {
                                    dst.push_back(ts, pulse, val.ix());
                                }
                                let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                Ready(Some(item))
                            } else if let Some(evs) = evs.as_any_ref().downcast_ref::<EventsDim0<bool>>() {
                                use items_0::Empty;
                                let mut dst = EventsDim0::<u8>::empty();
                                for ((&ts, &pulse), &val) in evs
                                    .tss()
                                    .iter()
                                    .zip(evs.pulses.iter())
                                    .zip(evs.private_values_ref().iter())
                                {
                                    dst.push_back(ts, pulse, val as u8);
                                }
                                let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                Ready(Some(item))
                            } else if let Some(evs) = evs.as_any_ref().downcast_ref::<EventsDim0<String>>() {
                                use items_0::Empty;
                                let mut dst = EventsDim0::<u64>::empty();
                                for ((&ts, &pulse), _) in evs
                                    .tss()
                                    .iter()
                                    .zip(evs.pulses.iter())
                                    .zip(evs.private_values_ref().iter())
                                {
                                    dst.push_back(ts, pulse, 1);
                                }
                                let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                Ready(Some(item))
                            } else if let Some(evs) = evs.as_any_ref().downcast_ref::<EventsDim0<f64>>() {
                                let buf = std::fs::read("evmod").unwrap_or(Vec::new());
                                let s = String::from_utf8_lossy(&buf);
                                if s.contains("u8") {
                                    use items_0::Empty;
                                    let mut dst = EventsDim0::<u8>::empty();
                                    for (ts, val) in evs.tss().iter().zip(evs.private_values_ref().iter()) {
                                        let v = (val * 1e6) as u8;
                                        dst.push_back(*ts, 0, v);
                                    }
                                    let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                    Ready(Some(item))
                                } else if s.contains("i16") {
                                    use items_0::Empty;
                                    let mut dst = EventsDim0::<i16>::empty();
                                    for (ts, val) in evs.tss().iter().zip(evs.private_values_ref().iter()) {
                                        let v = (val * 1e6) as i16 - 50;
                                        dst.push_back(*ts, 0, v);
                                    }
                                    let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                    Ready(Some(item))
                                } else if s.contains("bool") {
                                    use items_0::Empty;
                                    let mut dst = EventsDim0::<bool>::empty();
                                    for (ts, val) in evs.tss().iter().zip(evs.private_values_ref().iter()) {
                                        let g = u64::from_ne_bytes(val.to_ne_bytes());
                                        let val = g % 2 == 0;
                                        dst.push_back(*ts, 0, val);
                                    }
                                    let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                    Ready(Some(item))
                                } else if s.contains("enum") {
                                    use items_0::Empty;
                                    let mut dst = EventsDim0::<EnumVariant>::empty();
                                    for (ts, val) in evs.tss().iter().zip(evs.private_values_ref().iter()) {
                                        let buf = val.to_ne_bytes();
                                        let h = buf[0] ^ buf[1] ^ buf[2] ^ buf[3] ^ buf[4] ^ buf[5] ^ buf[6] ^ buf[7];
                                        dst.push_back(*ts, 0, EnumVariant::new(h as u16, h.to_string()));
                                    }
                                    let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                    Ready(Some(item))
                                } else if s.contains("string") {
                                    use items_0::Empty;
                                    let mut dst = EventsDim0::<String>::empty();
                                    for (ts, val) in evs.tss().iter().zip(evs.private_values_ref().iter()) {
                                        dst.push_back(*ts, 0, val.to_string());
                                    }
                                    let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                    Ready(Some(item))
                                } else {
                                    Ready(Some(item))
                                }
                            } else {
                                Ready(Some(item))
                            }
                        }
                        ChannelEvents::Status(conn_status_event) => Ready(Some(item)),
                    },
                    _ => Ready(Some(item)),
                }
            }
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub trait EventsReadProvider: Send + Sync {
    fn read(&self, evq: EventsSubQuery) -> EventsReading;
}

pub struct CacheReading {
    fut: Pin<Box<dyn Future<Output = Result<Option<BinsBoxed>, streams::timebin::cached::reader::Error>> + Send>>,
}

impl CacheReading {
    pub fn new(
        fut: Pin<Box<dyn Future<Output = Result<Option<BinsBoxed>, streams::timebin::cached::reader::Error>> + Send>>,
    ) -> Self {
        Self { fut }
    }
}

impl Future for CacheReading {
    type Output = Result<Option<BinsBoxed>, streams::timebin::cached::reader::Error>;

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
    fn read(&self, series: u64, bin_len: DtMs, msp: u64, offs: Range<u32>) -> CacheReading;
    fn write(&self, series: u64, bins: BinsBoxed) -> CacheWriting;
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
    series: u64,
    range: BinnedRange<TsNano>,
    ts1next: TsNano,
    bin_len: DtMs,
    cache_read_provider: Arc<dyn CacheReadProvider>,
    reading: Option<Pin<Box<dyn Future<Output = Result<Option<BinsBoxed>, Error>> + Send>>>,
}

impl CachedReader {
    pub fn new(
        series: u64,
        range: BinnedRange<TsNano>,
        cache_read_provider: Arc<dyn CacheReadProvider>,
    ) -> Result<Self, Error> {
        let ret = Self {
            series,
            ts1next: range.nano_beg(),
            bin_len: range.bin_len.to_dt_ms(),
            range,
            cache_read_provider,
            reading: None,
        };
        Ok(ret)
    }
}

impl Stream for CachedReader {
    type Item = Result<BinsBoxed, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        // TODO
        // Must split over different msp (because pkey).
        // If we choose the partitioning length low enough, no need to issue multiple queries.
        // Change the worker interface:
        // We should already compute here the msp and off because we must here implement the loop logic.
        // Therefore worker interface should not accept BinnedRange, but msp and off range.
        loop {
            break if let Some(fut) = self.reading.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(x) => {
                        self.reading = None;
                        match x {
                            Ok(Some(bins)) => {
                                trace_emit!(
                                    "- - - - - - - - - - - -  emit cached bins  {}  bin_len {}",
                                    bins.len(),
                                    self.bin_len
                                );
                                Ready(Some(Ok(bins)))
                            }
                            Ok(None) => {
                                continue;
                            }
                            Err(e) => Ready(Some(Err(e))),
                        }
                    }
                    Pending => Pending,
                }
            } else {
                if self.ts1next < self.range.nano_end() {
                    let div = part_len(self.bin_len).ns();
                    let msp = self.ts1next.ns() / div;
                    let off = (self.ts1next.ns() - div * msp) / self.bin_len.ns();
                    let off2 = (self.range.nano_end().ns() - div * msp) / self.bin_len.ns();
                    let off2 = off2.min(off_max());
                    self.ts1next = TsNano::from_ns(self.bin_len.ns() * off2 + div * msp);
                    let offs = off as u32..off2 as u32;
                    let fut = self.cache_read_provider.read(self.series, self.bin_len, msp, offs);
                    self.reading = Some(Box::pin(fut));
                    continue;
                } else {
                    Ready(None)
                }
            };
        }
    }
}
