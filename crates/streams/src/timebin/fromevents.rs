use super::cached::reader::EventsReadProvider;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::binsdim0::BinsDim0;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::ChConf;
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
    stream: Pin<Box<dyn Stream<Item = Sitemty<BinsDim0<f32>>> + Send>>,
}

impl BinnedFromEvents {
    pub fn new(
        range: BinnedRange<TsNano>,
        evq: EventsSubQuery,
        chconf: ChConf,
        do_time_weight: bool,
        read_provider: Arc<dyn EventsReadProvider>,
    ) -> Result<Self, Error> {
        if !evq.range().is_time() {
            panic!();
        }
        let stream = read_provider.read(evq, chconf);
        let stream = Box::pin(stream);
        let stream = super::basic::TimeBinnedStream::new(stream, netpod::BinnedRangeEnum::Time(range), do_time_weight);
        let stream = stream.map(|item| match item {
            Ok(x) => match x {
                StreamItem::DataItem(x) => match x {
                    RangeCompletableItem::Data(mut x) => {
                        // TODO need a typed time binner
                        if let Some(x) = x.as_any_mut().downcast_mut::<BinsDim0<f32>>() {
                            let y = x.clone();
                            use items_0::WithLen;
                            trace_emit!("===========  =========  emit from events {}", y.len());
                            Ok(StreamItem::DataItem(RangeCompletableItem::Data(y)))
                        } else {
                            Err(::err::Error::with_msg_no_trace(
                                "GapFill expects incoming BinsDim0<f32>",
                            ))
                        }
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
    type Item = Sitemty<BinsDim0<f32>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}
