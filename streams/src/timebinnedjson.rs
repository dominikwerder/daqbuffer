use crate::collect::Collect;
use crate::rangefilter2::RangeFilter2;
use crate::tcprawclient::open_tcp_streams;
use crate::timebin::TimeBinnedStream;
use crate::transform::build_merged_event_transform;
use crate::transform::EventsToTimeBinnable;
use crate::transform::TimeBinnableToCollectable;
use err::Error;
use futures_util::future::BoxFuture;
use futures_util::stream;
use futures_util::stream::BoxStream;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::collect_s::Collectable;
use items_0::on_sitemty_data;
use items_0::streamitem::sitem_data;
use items_0::streamitem::Sitemty;
use items_0::timebin::TimeBinnable;
use items_0::timebin::TimeBinned;
use items_0::transform::CollectableStreamBox;
use items_0::transform::TimeBinnableStreamBox;
use items_0::transform::TimeBinnableStreamTrait;
use items_0::Events;
use items_2::channelevents::ChannelEvents;
use items_2::merger::Merger;
use items_2::streams::PlainEventStream;
use items_2::streams::PlainTimeBinnableStream;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
use netpod::BinnedRange;
use netpod::BinnedRangeEnum;
use netpod::ChConf;
use netpod::Cluster;
use query::api4::binned::BinnedQuery;
use query::api4::events::PlainEventsQuery;
use serde_json::Value as JsonValue;
use std::pin::Pin;
use std::time::Instant;

fn assert_stream_send<'u, R>(stream: impl 'u + Send + Stream<Item = R>) -> impl 'u + Send + Stream<Item = R> {
    stream
}

async fn timebinnable_stream(
    query: BinnedQuery,
    chconf: ChConf,
    range: NanoRange,
    one_before_range: bool,
    cluster: Cluster,
) -> Result<TimeBinnableStreamBox, Error> {
    let evq = PlainEventsQuery::new(query.channel().clone(), query.range().clone()).for_time_weighted_scalar();
    let mut tr = build_merged_event_transform(evq.transform())?;

    let inps = open_tcp_streams::<_, ChannelEvents>(&evq, &cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader.
    // TODO use a mixture of count and byte-size as threshold.
    let stream = Merger::new(inps, evq.merger_out_len_max());

    let stream = RangeFilter2::new(stream, range, one_before_range);

    let stream = stream.map(move |k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn Events> = Box::new(k);
            info!("-------------------------\ngot len {}", k.len());
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
    chconf: ChConf,
    cluster: Cluster,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinned>>> + Send>>, Error> {
    let deadline = Instant::now();
    let range: NanoRange = query.range().try_into()?;

    //let binned_range = BinnedRangeEnum::covering_range(SeriesRange::TimeRange(NanoRange { beg: 123, end: 456 }), 10)?;
    let binned_range = BinnedRangeEnum::covering_range(query.range().clone(), query.bin_count())?;
    let do_time_weight = true;
    let one_before_range = true;

    let stream = timebinnable_stream(query.clone(), chconf, range, one_before_range, cluster).await?;
    let stream: Pin<Box<dyn TimeBinnableStreamTrait>> = stream.0;
    let stream = Box::pin(stream);
    let stream = TimeBinnedStream::new(stream, binned_range, do_time_weight, deadline);
    let stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinned>>> + Send>> = Box::pin(stream);
    Ok(stream)
}

fn timebinned_to_collectable(
    stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinned>>> + Send>>,
) -> Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Collectable>>> + Send>> {
    let stream = stream.map(|k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn Collectable> = Box::new(k);
            info!("-------------------------\ngot len {}", k.len());
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
        })
    });
    let stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Collectable>>> + Send>> = Box::pin(stream);
    stream
}

pub async fn timebinned_json(query: BinnedQuery, chconf: ChConf, cluster: Cluster) -> Result<JsonValue, Error> {
    let stream = timebinned_stream(query.clone(), chconf.clone(), cluster).await?;
    let deadline = Instant::now();
    let evq = PlainEventsQuery::new(query.channel().clone(), query.range().clone()).for_time_weighted_scalar();
    let collect_range = evq.range().clone();
    let events_max = evq.events_max();
    let range: NanoRange = query.range().try_into()?;

    let binned_range = BinnedRangeEnum::covering_range(SeriesRange::TimeRange(NanoRange { beg: 123, end: 456 }), 10)?;
    let do_time_weight = true;
    let one_before_range = true;

    let stream = timebinned_to_collectable(stream);

    /*
    let stream = stream.map(|k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn Collectable> = Box::new(k);
            info!("-------------------------\ngot len {}", k.len());
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
        })
    });
    let stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Collectable>>> + Send>> = Box::pin(stream);
    */

    let collected = Collect::new(stream, deadline, events_max, Some(collect_range), None);
    let collected: BoxFuture<_> = Box::pin(collected);
    let collected = collected.await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}

pub async fn timebinned_json_2(query: BinnedQuery, chconf: ChConf, cluster: Cluster) -> Result<JsonValue, Error> {
    warn!("TODO add with_deadline to PlainEventsQuery");
    let deadline = Instant::now() + query.timeout_value();
    // TODO RangeFilter2 must accept SeriesRange
    let range: NanoRange = query.range().try_into()?;
    // TODO
    let do_time_weight = true;
    let one_before_range = true;
    // TODO construct the events query in a better way.
    let evq = PlainEventsQuery::new(query.channel().clone(), query.range().clone()).for_time_weighted_scalar();
    let binned_range = BinnedRangeEnum::covering_range(query.range().clone(), query.bin_count())?;
    let bins_max = 10000;
    let events_max = evq.events_max();
    let collect_range = evq.range().clone();
    let mut tr = build_merged_event_transform(evq.transform())?;

    /*
    let inps = open_tcp_streams::<_, ChannelEvents>(&evq, &cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader.
    // TODO use a mixture of count and byte-size as threshold.
    let stream = Merger::new(inps, evq.merger_out_len_max());

    let stream = RangeFilter2::new(stream, range, one_before_range);

    let stream = stream.map(move |k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn Events> = Box::new(k);
            info!("-------------------------\ngot len {}", k.len());
            let k = tr.0.transform(k);
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
        })
    });
    let stream = PlainEventStream::new(stream);
    let stream = EventsToTimeBinnable::new(stream);
    let stream = Box::pin(stream);
    */

    let stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinnable>>> + Send>> = todo!();

    // TODO TimeBinnedStream must accept types bin edges.
    // Maybe even take a BinnedRangeEnum?
    let stream = TimeBinnedStream::new(stream, binned_range, do_time_weight, deadline);
    /*
    let stream = stream.map(|k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn TimeBinnable> = Box::new(k);
            sitem_data(k)
        })
    });
    let stream = PlainTimeBinnableStream::new(stream);
    //let stream = Box::pin(stream);
    //let stream = TimeBinnableStreamBox(stream);

    let stream = TimeBinnableToCollectable::new(stream);
    let stream = Box::pin(stream);
    */

    // TODO collect should not have to accept two ranges, instead, generalize over it.
    //let collected = crate::collect::collect(stream, deadline, bins_max, None, Some(binned_range.clone())).await?;
    let stream = futures_util::stream::empty();
    let stream = CollectableStreamBox(Box::pin(stream));
    /*let collected = Collect::new(stream, deadline, events_max, Some(collect_range), None);
    let collected: BoxFuture<_> = Box::pin(collected);
    let collected = collected.await?;
    */
    let collected = "";
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
