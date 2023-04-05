use err::Error;
use futures_util::Stream;
use items_0::container::ByteEstimate;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::Appendable;
use items_0::Empty;
use items_0::WithLen;
use items_2::channelevents::ChannelEvents;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::MS;
use std::pin::Pin;

pub fn generate_i32(
    node_ix: u64,
    node_count: u64,
    range: SeriesRange,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    type T = i32;
    let mut items = Vec::new();
    match range {
        SeriesRange::TimeRange(range) => {
            let mut item = items_2::eventsdim0::EventsDim0::empty();
            let td = MS * 1000;
            let mut ts = (range.beg / td + node_ix) * td;
            loop {
                if ts >= range.end {
                    break;
                }
                let pulse = ts;
                item.push(ts, pulse, pulse as T);
                ts += td * node_count as u64;
                if item.byte_estimate() > 200 {
                    let w = ChannelEvents::Events(Box::new(item) as _);
                    let w = Ok::<_, Error>(StreamItem::DataItem(RangeCompletableItem::Data(w)));
                    items.push(w);
                    item = items_2::eventsdim0::EventsDim0::empty();
                }
            }
            if item.len() != 0 {
                let w = ChannelEvents::Events(Box::new(item) as _);
                let w = Ok::<_, Error>(StreamItem::DataItem(RangeCompletableItem::Data(w)));
                items.push(w);
            }
        }
        SeriesRange::PulseRange(_) => {
            error!("TODO generate test data by pulse id range");
        }
    }
    let stream = futures_util::stream::iter(items);
    Ok(Box::pin(stream))
}

pub fn generate_f32(
    node_ix: u64,
    node_count: u64,
    range: SeriesRange,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    let mut item = items_2::eventsdim0::EventsDim0::<f32>::empty();
    let td = MS * 10;
    for i in 0..20 {
        let ts = MS * 17 + td * node_ix as u64 + td * node_count as u64 * i;
        let pulse = 1 + node_ix as u64 + node_count as u64 * i;
        item.push(ts, pulse, ts as _);
    }
    let item = ChannelEvents::Events(Box::new(item) as _);
    let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
    let stream = futures_util::stream::iter([item]);
    Ok(Box::pin(stream))
}
