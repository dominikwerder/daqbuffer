use super::cached::reader::EventsReadProvider;
use crate::events::convertforbinning::ConvertForBinning;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::timebin::BinsBoxed;
use items_2::binning::timeweight::timeweight_events_dyn::BinnedEventsTimeweightStream;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::TsNano;
use query::api4::events::EventsSubQuery;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

#[allow(unused)]
macro_rules! trace_emit { ($($arg:tt)*) => ( if true { trace!($($arg)*); } ) }

#[derive(Debug, ThisError)]
#[cstm(name = "ReadingBinnedFromEvents")]
pub enum Error {}

pub struct BinnedFromEvents {
    stream: Pin<Box<dyn Stream<Item = Sitemty<BinsBoxed>> + Send>>,
}

impl BinnedFromEvents {
    pub fn new(
        range: BinnedRange<TsNano>,
        evq: EventsSubQuery,
        do_time_weight: bool,
        read_provider: Arc<dyn EventsReadProvider>,
    ) -> Result<Self, Error> {
        if !evq.range().is_time() {
            panic!();
        }
        let stream = read_provider.read(evq);
        let stream = ConvertForBinning::new(Box::pin(stream));
        let stream = if do_time_weight {
            let stream = Box::pin(stream);
            BinnedEventsTimeweightStream::new(range, stream)
        } else {
            panic!("non-weighted TODO")
        };
        let stream = stream.map(|item| match item {
            Ok(x) => match x {
                StreamItem::DataItem(x) => match x {
                    RangeCompletableItem::Data(x) => {
                        debug!("see item {:?}", x);
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))
                    }
                    RangeCompletableItem::RangeComplete => {
                        info!("BinnedFromEvents  sees range final");
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                    }
                },
                StreamItem::Log(x) => Ok(StreamItem::Log(x)),
                StreamItem::Stats(x) => Ok(StreamItem::Stats(x)),
            },
            Err(e) => Err(e),
        });
        let ret = Self {
            stream: Box::pin(stream),
        };
        Ok(ret)
    }
}

impl Stream for BinnedFromEvents {
    type Item = Sitemty<BinsBoxed>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}
