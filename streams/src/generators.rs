use err::Error;
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
use items_2::channelevents::ChannelEvents;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::MS;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

pub struct GenerateI32 {
    ts: u64,
    dts: u64,
    tsend: u64,
    timeout: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl GenerateI32 {
    pub fn new(node_ix: u64, node_count: u64, range: SeriesRange) -> Self {
        let range = match range {
            SeriesRange::TimeRange(k) => k,
            SeriesRange::PulseRange(_) => todo!(),
        };
        let dts = MS * 1000 * node_count as u64;
        let ts = (range.beg / dts + node_ix) * dts;
        let tsend = range.end;
        Self {
            ts,
            dts,
            tsend,
            timeout: None,
        }
    }

    fn make_batch(&mut self) -> Sitemty<ChannelEvents> {
        type T = i32;
        let mut item = items_2::eventsdim0::EventsDim0::empty();
        let mut ts = self.ts;
        loop {
            if self.ts >= self.tsend || item.byte_estimate() > 200 {
                break;
            }
            let pulse = ts;
            item.push(ts, pulse, pulse as T);
            ts += self.dts;
        }
        self.ts = ts;
        let w = ChannelEvents::Events(Box::new(item) as _);
        let w = sitem_data(w);
        w
    }
}

impl Stream for GenerateI32 {
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.ts >= self.tsend {
                Ready(None)
            } else if let Some(fut) = self.timeout.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(()) => {
                        self.timeout = None;
                        Ready(Some(self.make_batch()))
                    }
                    Pending => Pending,
                }
            } else {
                self.timeout = Some(Box::pin(tokio::time::sleep(Duration::from_millis(500))));
                continue;
            };
        }
    }
}
