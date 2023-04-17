use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::timebin::TimeBinnable;
use items_0::transform::CollectableStream;
use items_0::transform::EventStream;
use items_0::transform::EventStreamTrait;
use items_0::transform::TransformEvent;
use items_0::transform::WithTransformProperties;
use items_0::Events;
use items_2::transform::make_transform_identity;
use items_2::transform::make_transform_min_max_avg;
use items_2::transform::make_transform_pulse_id_diff;
use query::transform::EventTransformQuery;
use query::transform::TimeBinningTransformQuery;
use query::transform::TransformQuery;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub fn build_event_transform(tr: &TransformQuery, inp: EventStream) -> Result<TransformEvent, Error> {
    let trev = tr.get_tr_event();
    match trev {
        EventTransformQuery::ValueFull => Ok(make_transform_identity()),
        EventTransformQuery::MinMaxAvgDev => Ok(make_transform_min_max_avg()),
        EventTransformQuery::ArrayPick(..) => Err(Error::with_msg_no_trace(format!(
            "build_event_transform don't know what to do {trev:?}"
        ))),
        EventTransformQuery::PulseIdDiff => Ok(make_transform_pulse_id_diff()),
        EventTransformQuery::EventBlobsVerbatim => Err(Error::with_msg_no_trace(format!(
            "build_event_transform don't know what to do {trev:?}"
        ))),
        EventTransformQuery::EventBlobsUncompressed => Err(Error::with_msg_no_trace(format!(
            "build_event_transform don't know what to do {trev:?}"
        ))),
    }
}

pub struct EventsToTimeBinnable {
    inp: Pin<Box<dyn EventStreamTrait>>,
}

impl Stream for EventsToTimeBinnable {
    type Item = Sitemty<Box<dyn TimeBinnable>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => Ready(Some(match item {
                Ok(item) => Ok(match item {
                    StreamItem::DataItem(item) => StreamItem::DataItem(match item {
                        RangeCompletableItem::RangeComplete => RangeCompletableItem::RangeComplete,
                        RangeCompletableItem::Data(item) => RangeCompletableItem::Data(Box::new(item)),
                    }),
                    StreamItem::Log(item) => StreamItem::Log(item),
                    StreamItem::Stats(item) => StreamItem::Stats(item),
                }),
                Err(e) => Err(e),
            })),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

impl WithTransformProperties for EventsToTimeBinnable {
    fn query_transform_properties(&self) -> items_0::transform::TransformProperties {
        self.inp.query_transform_properties()
    }
}

pub fn build_full_transform_collectable(tr: &TransformQuery, inp: EventStream) -> Result<CollectableStream, Error> {
    // TODO this must return a Stream!
    //let evs = build_event_transform(tr, inp)?;
    let trtb = tr.get_tr_time_binning();
    use futures_util::Stream;
    use items_0::collect_s::Collectable;
    use items_0::streamitem::RangeCompletableItem;
    use items_0::streamitem::Sitemty;
    use items_0::streamitem::StreamItem;
    use std::pin::Pin;
    let a: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Collectable>>> + Send>> = Box::pin(inp.0.map(|item| match item {
        Ok(item) => match item {
            StreamItem::DataItem(item) => match item {
                RangeCompletableItem::Data(item) => {
                    let item: Box<dyn Collectable> = Box::new(item);
                    Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                }
                RangeCompletableItem::RangeComplete => Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)),
            },
            StreamItem::Log(item) => Ok(StreamItem::Log(item)),
            StreamItem::Stats(item) => Ok(StreamItem::Stats(item)),
        },
        Err(e) => Err(e),
    }));
    let stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Collectable>>> + Send>> =
        Box::pin(futures_util::stream::empty());
    match trtb {
        TimeBinningTransformQuery::None => Ok(CollectableStream(stream)),
        TimeBinningTransformQuery::TimeWeighted => todo!(),
        TimeBinningTransformQuery::Unweighted => todo!(),
    }
}
