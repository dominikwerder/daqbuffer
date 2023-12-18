use crate::frames::inmem::BoxedBytesStream;
use crate::transform::build_event_transform;
use err::Error;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::container::ByteEstimate;
use items_0::on_sitemty_data;
use items_0::streamitem::sitem_data;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::Appendable;
use items_0::Empty;
use items_0::WithLen;
use items_2::channelevents::ChannelEvents;
use items_2::empty::empty_events_dyn_ev;
use items_2::eventsdim0::EventsDim0;
use items_2::eventsdim1::EventsDim1;
use items_2::framable::Framable;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::DAY;
use netpod::timeunits::MS;
use query::api4::events::EventsSubQuery;
use std::f64::consts::PI;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

pub fn make_test_channel_events_bytes_stream(
    subq: EventsSubQuery,
    node_count: u64,
    node_ix: u64,
) -> Result<BoxedBytesStream, Error> {
    if subq.is_event_blobs() {
        let e = Error::with_msg_no_trace("evq.is_event_blobs() not supported in this generator");
        error!("{e}");
        Err(e)
    } else {
        let mut tr = build_event_transform(subq.transform())?;
        let stream = make_test_channel_events_stream_data(subq, node_count, node_ix)?;
        let stream = stream.map(move |x| {
            on_sitemty_data!(x, |x: ChannelEvents| {
                match x {
                    ChannelEvents::Events(evs) => {
                        let evs = tr.0.transform(evs);
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(ChannelEvents::Events(
                            evs,
                        ))))
                    }
                    ChannelEvents::Status(x) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                        ChannelEvents::Status(x),
                    ))),
                }
            })
        });
        let stream = stream.map(|x| x.make_frame().map(|x| x.freeze()));
        let ret = Box::pin(stream);
        Ok(ret)
    }
}

// is also used from nodenet::conn
pub fn make_test_channel_events_stream_data(
    subq: EventsSubQuery,
    node_count: u64,
    node_ix: u64,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    let empty = empty_events_dyn_ev(subq.ch_conf().scalar_type(), subq.ch_conf().shape())?;
    let empty = sitem_data(ChannelEvents::Events(empty));
    let stream = make_test_channel_events_stream_data_inner(subq, node_count, node_ix)?;
    let ret = futures_util::stream::iter([empty]).chain(stream);
    let ret = Box::pin(ret);
    Ok(ret)
}

fn make_test_channel_events_stream_data_inner(
    subq: EventsSubQuery,
    node_count: u64,
    node_ix: u64,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    debug!("use test backend data");
    let chn = subq.name();
    let range = subq.range().clone();
    let one_before = subq.transform().need_one_before_range();
    if chn == "test-gen-i32-dim0-v00" {
        Ok(Box::pin(GenerateI32V00::new(node_ix, node_count, range, one_before)))
    } else if chn == "test-gen-i32-dim0-v01" {
        Ok(Box::pin(GenerateI32V01::new(node_ix, node_count, range, one_before)))
    } else if chn == "test-gen-f64-dim1-v00" {
        Ok(Box::pin(GenerateF64V00::new(node_ix, node_count, range, one_before)))
    } else {
        let na: Vec<_> = chn.split("-").collect();
        if na.len() != 3 {
            Err(Error::with_msg_no_trace(format!(
                "make_channel_events_stream_data can not understand test channel name: {chn:?}"
            )))
        } else {
            if na[0] != "inmem" {
                Err(Error::with_msg_no_trace(format!(
                    "make_channel_events_stream_data can not understand test channel name: {chn:?}"
                )))
            } else {
                let _range = subq.range().clone();
                if na[1] == "d0" {
                    if na[2] == "i32" {
                        //generator::generate_i32(node_ix, node_count, range)
                        panic!()
                    } else if na[2] == "f32" {
                        //generator::generate_f32(node_ix, node_count, range)
                        panic!()
                    } else {
                        Err(Error::with_msg_no_trace(format!(
                            "make_channel_events_stream_data can not understand test channel name: {chn:?}"
                        )))
                    }
                } else {
                    Err(Error::with_msg_no_trace(format!(
                        "make_channel_events_stream_data can not understand test channel name: {chn:?}"
                    )))
                }
            }
        }
    }
}

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
            if self.ts >= self.tsend || item.byte_estimate() > 100 {
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
        debug!(
            "GenerateI32V01::new  ivl {}  dts {}  ts {}  one_before_range {}",
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
            if self.ts >= self.tsend || item.byte_estimate() > 100 {
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
        debug!(
            "GenerateF64V00::new  ivl {}  dts {}  ts {}  one_before_range {}",
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
            if self.ts >= self.tsend || item.byte_estimate() > 400 {
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
        trace!("generated  len {}", item.len());
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
