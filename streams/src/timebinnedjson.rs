use crate::collect::Collect;
use crate::rangefilter2::RangeFilter2;
use crate::tcprawclient::open_tcp_streams;
use crate::timebin::TimeBinnedStream;
use crate::transform::build_merged_event_transform;
use crate::transform::EventsToTimeBinnable;
use err::Error;
use futures_util::future::BoxFuture;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::collect_s::Collectable;
use items_0::on_sitemty_data;
use items_0::streamitem::Sitemty;
use items_0::timebin::TimeBinned;
use items_0::transform::TimeBinnableStreamBox;
use items_0::transform::TimeBinnableStreamTrait;
use items_0::Events;
use items_2::channelevents::ChannelEvents;
use items_2::merger::Merger;
use items_2::streams::PlainEventStream;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::BinnedRangeEnum;
use netpod::ChConf;
use netpod::Cluster;
use query::api4::binned::BinnedQuery;
use query::api4::events::PlainEventsQuery;
use serde_json::Value as JsonValue;
use std::pin::Pin;
use std::time::Instant;

#[allow(unused)]
fn assert_stream_send<'u, R>(stream: impl 'u + Send + Stream<Item = R>) -> impl 'u + Send + Stream<Item = R> {
    stream
}

async fn timebinnable_stream(
    query: BinnedQuery,
    range: NanoRange,
    one_before_range: bool,
    cluster: Cluster,
) -> Result<TimeBinnableStreamBox, Error> {
    let evq = PlainEventsQuery::new(query.channel().clone(), range.clone()).for_time_weighted_scalar();
    let mut tr = build_merged_event_transform(evq.transform())?;

    let inps = open_tcp_streams::<_, ChannelEvents>(&evq, &cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader.
    // TODO use a mixture of count and byte-size as threshold.
    let stream = Merger::new(inps, query.merger_out_len_max());

    let stream = RangeFilter2::new(stream, range, one_before_range);

    let stream = stream.map(move |k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn Events> = Box::new(k);
            trace!("got len {}", k.len());
            let k = tr.0.transform(k);
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
        })
    });
    let stream = PlainEventStream::new(stream);
    let stream = EventsToTimeBinnable::new(stream);
    let stream = Box::pin(stream);
    Ok(TimeBinnableStreamBox(stream))
}

async fn timebinned_stream(
    query: BinnedQuery,
    binned_range: BinnedRangeEnum,
    cluster: Cluster,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinned>>> + Send>>, Error> {
    let range = binned_range.binned_range_time().to_nano_range();

    let do_time_weight = true;
    let one_before_range = true;

    let stream = timebinnable_stream(query.clone(), range, one_before_range, cluster).await?;
    let stream: Pin<Box<dyn TimeBinnableStreamTrait>> = stream.0;
    let stream = Box::pin(stream);
    // TODO rename TimeBinnedStream to make it more clear that it is the component which initiates the time binning.
    let stream = TimeBinnedStream::new(stream, binned_range, do_time_weight);
    let stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinned>>> + Send>> = Box::pin(stream);
    Ok(stream)
}

fn timebinned_to_collectable(
    stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinned>>> + Send>>,
) -> Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Collectable>>> + Send>> {
    let stream = stream.map(|k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn Collectable> = Box::new(k);
            trace!("got len {}", k.len());
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
        })
    });
    let stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Collectable>>> + Send>> = Box::pin(stream);
    stream
}

pub async fn timebinned_json(query: BinnedQuery, _chconf: ChConf, cluster: Cluster) -> Result<JsonValue, Error> {
    let deadline = Instant::now().checked_add(query.timeout_value()).unwrap();
    let binned_range = BinnedRangeEnum::covering_range(query.range().clone(), query.bin_count())?;
    let collect_max = 10000;
    let stream = timebinned_stream(query.clone(), binned_range.clone(), cluster).await?;
    let stream = timebinned_to_collectable(stream);
    let collected = Collect::new(stream, deadline, collect_max, None, Some(binned_range));
    let collected: BoxFuture<_> = Box::pin(collected);
    let collected = collected.await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
