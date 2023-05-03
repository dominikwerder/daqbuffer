use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use items_0::container::ByteEstimate;
use items_0::streamitem::sitem_data;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::Appendable;
use items_0::Empty;
use items_0::WithLen;
use items_2::channelevents::ChannelEvents;
use items_2::eventsdim0::EventsDim0;
use items_2::eventsdim1::EventsDim1;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::DAY;
use netpod::timeunits::MS;
use std::f64::consts::PI;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

pub struct GenerateI32V00 {
    ts: u64,
    dts: u64,
    tsend: u64,
    #[allow(unused)]
    c1: u64,
    timeout: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    do_throttle: bool,
    done: bool,
    done_range_final: bool,
}

impl GenerateI32V00 {
    pub fn new(node_ix: u64, node_count: u64, range: SeriesRange, one_before_range: bool) -> Self {
        let range = match range {
            SeriesRange::TimeRange(k) => k,
            SeriesRange::PulseRange(_) => todo!(),
        };
        let ivl = MS * 1000;
        let dts = ivl * node_count as u64;
        let ts = (range.beg / ivl + node_ix - if one_before_range { 1 } else { 0 }) * ivl;
        let tsend = range.end;
        Self {
            ts,
            dts,
            tsend,
            c1: 0,
            timeout: None,
            do_throttle: false,
            done: false,
            done_range_final: false,
        }
    }

    fn make_batch(&mut self) -> Sitemty<ChannelEvents> {
        type T = i32;
        let mut item = EventsDim0::empty();
        let mut ts = self.ts;
        loop {
            if self.ts >= self.tsend || item.byte_estimate() > 200 {
                break;
            }
            let pulse = ts;
            let value = (ts / (MS * 100) % 1000) as T;
            item.push(ts, pulse, value);
            ts += self.dts;
        }
        self.ts = ts;
        let w = ChannelEvents::Events(Box::new(item) as _);
        let w = sitem_data(w);
        w
    }
}

impl Stream for GenerateI32V00 {
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.done {
                Ready(None)
            } else if self.ts >= self.tsend {
                self.done = true;
                self.done_range_final = true;
                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
            } else if !self.do_throttle {
                // To use the generator without throttling, use this scope
                Ready(Some(self.make_batch()))
            } else if let Some(fut) = self.timeout.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(()) => {
                        self.timeout = None;
                        Ready(Some(self.make_batch()))
                    }
                    Pending => Pending,
                }
            } else {
                self.timeout = Some(Box::pin(tokio::time::sleep(Duration::from_millis(2))));
                continue;
            };
        }
    }
}

pub struct GenerateI32V01 {
    ivl: u64,
    ts: u64,
    dts: u64,
    tsend: u64,
    #[allow(unused)]
    c1: u64,
    node_ix: u64,
    timeout: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    do_throttle: bool,
    have_range_final: bool,
    done: bool,
    done_range_final: bool,
}

impl GenerateI32V01 {
    pub fn new(node_ix: u64, node_count: u64, range: SeriesRange, one_before_range: bool) -> Self {
        let range = match range {
            SeriesRange::TimeRange(k) => k,
            SeriesRange::PulseRange(_) => todo!(),
        };
        let ivl = MS * 500;
        let dts = ivl * node_count as u64;
        let ts = (range.beg / ivl + node_ix - if one_before_range { 1 } else { 0 }) * ivl;
        let tsend = range.end.min(DAY);
        let have_range_final = range.end < (DAY - ivl);
        info!(
            "START GENERATOR GenerateI32V01  ivl {}  dts {}  ts {}  one_before_range {}",
            ivl, dts, ts, one_before_range
        );
        Self {
            ivl,
            ts,
            dts,
            tsend,
            c1: 0,
            node_ix,
            timeout: None,
            do_throttle: false,
            have_range_final,
            done: false,
            done_range_final: false,
        }
    }

    fn make_batch(&mut self) -> Sitemty<ChannelEvents> {
        type T = i32;
        let mut item = EventsDim0::empty();
        let mut ts = self.ts;
        loop {
            if self.ts >= self.tsend || item.byte_estimate() > 200 {
                break;
            }
            let pulse = ts;
            let value = (ts / self.ivl) as T;
            if false {
                info!(
                    "v01  node {}  made event  ts {}  pulse {}  value {}",
                    self.node_ix, ts, pulse, value
                );
            }
            item.push(ts, pulse, value);
            ts += self.dts;
        }
        self.ts = ts;
        let w = ChannelEvents::Events(Box::new(item) as _);
        let w = sitem_data(w);
        w
    }
}

impl Stream for GenerateI32V01 {
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.done {
                Ready(None)
            } else if self.ts >= self.tsend {
                self.done = true;
                self.done_range_final = true;
                if self.have_range_final {
                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                } else {
                    continue;
                }
            } else if !self.do_throttle {
                // To use the generator without throttling, use this scope
                Ready(Some(self.make_batch()))
            } else if let Some(fut) = self.timeout.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(()) => {
                        self.timeout = None;
                        Ready(Some(self.make_batch()))
                    }
                    Pending => Pending,
                }
            } else {
                self.timeout = Some(Box::pin(tokio::time::sleep(Duration::from_millis(2))));
                continue;
            };
        }
    }
}

pub struct GenerateF64V00 {
    ivl: u64,
    ts: u64,
    dts: u64,
    tsend: u64,
    node_ix: u64,
    timeout: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    do_throttle: bool,
    done: bool,
    done_range_final: bool,
}

impl GenerateF64V00 {
    pub fn new(node_ix: u64, node_count: u64, range: SeriesRange, one_before_range: bool) -> Self {
        let range = match range {
            SeriesRange::TimeRange(k) => k,
            SeriesRange::PulseRange(_) => todo!(),
        };
        let ivl = MS * 100;
        let dts = ivl * node_count as u64;
        let ts = (range.beg / ivl + node_ix - if one_before_range { 1 } else { 0 }) * ivl;
        let tsend = range.end;
        info!(
            "START GENERATOR GenerateF64V00  ivl {}  dts {}  ts {}  one_before_range {}",
            ivl, dts, ts, one_before_range
        );
        Self {
            ivl,
            ts,
            dts,
            tsend,
            node_ix,
            timeout: None,
            do_throttle: false,
            done: false,
            done_range_final: false,
        }
    }

    fn make_batch(&mut self) -> Sitemty<ChannelEvents> {
        type T = f64;
        let mut item = EventsDim1::empty();
        let mut ts = self.ts;
        loop {
            if self.ts >= self.tsend || item.byte_estimate() > 1024 * 4 {
                break;
            }
            let pulse = ts;
            let ampl = ((ts / self.ivl) as T).sin() + 2.;
            let mut value = Vec::new();
            let pi = PI;
            for i in 0..21 {
                let x = ((-pi + (2. * pi / 20.) * i as f64).cos() + 1.1) * ampl;
                value.push(x);
            }
            if false {
                info!(
                    "v01  node {}  made event  ts {}  pulse {}  value {:?}",
                    self.node_ix, ts, pulse, value
                );
            }
            item.push(ts, pulse, value);
            ts += self.dts;
        }
        self.ts = ts;
        info!("generated  len {}", item.len());
        let w = ChannelEvents::Events(Box::new(item) as _);
        let w = sitem_data(w);
        w
    }
}

impl Stream for GenerateF64V00 {
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.done {
                Ready(None)
            } else if self.ts >= self.tsend {
                self.done = true;
                self.done_range_final = true;
                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
            } else if !self.do_throttle {
                // To use the generator without throttling, use this scope
                Ready(Some(self.make_batch()))
            } else if let Some(fut) = self.timeout.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(()) => {
                        self.timeout = None;
                        Ready(Some(self.make_batch()))
                    }
                    Pending => Pending,
                }
            } else {
                self.timeout = Some(Box::pin(tokio::time::sleep(Duration::from_millis(2))));
                continue;
            };
        }
    }
}
