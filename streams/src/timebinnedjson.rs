use crate::collect::Collect;
use crate::rangefilter2::RangeFilter2;
use crate::tcprawclient::open_tcp_streams;
use crate::timebin::TimeBinnedStream;
use crate::transform::build_merged_event_transform;
use crate::transform::EventsToTimeBinnable;
use crate::transform::TimeBinnableToCollectable;
use err::Error;
use futures_util::stream;
use futures_util::StreamExt;
use items_0::on_sitemty_data;
use items_0::streamitem::sitem_data;
use items_0::streamitem::Sitemty;
use items_0::timebin::TimeBinnable;
use items_0::timebin::TimeBinned;
use items_0::transform::TimeBinnableStreamBox;
use items_0::Events;
use items_2::channelevents::ChannelEvents;
use items_2::merger::Merger;
use items_2::streams::PlainEventStream;
use items_2::streams::PlainTimeBinnableStream;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::BinnedRangeEnum;
use netpod::ChConf;
use netpod::Cluster;
use query::api4::binned::BinnedQuery;
use query::api4::events::PlainEventsQuery;
use serde_json::Value as JsonValue;
use std::time::Instant;

pub async fn timebinned_json(query: &BinnedQuery, chconf: &ChConf, cluster: &Cluster) -> Result<JsonValue, Error> {
    let binned_range = BinnedRangeEnum::covering_range(query.range().clone(), query.bin_count())?;
    let bins_max = 10000;
    warn!("TODO add with_deadline to PlainEventsQuery");
    let deadline = Instant::now() + query.timeout_value();
    // TODO construct the events query in a better way.
    let evq = PlainEventsQuery::new(query.channel().clone(), query.range().clone()).for_time_weighted_scalar();
    let mut tr = build_merged_event_transform(evq.transform())?;
    let inps = open_tcp_streams::<_, ChannelEvents>(&evq, cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader.
    // TODO use a mixture of count and byte-size as threshold.
    let stream = Merger::new(inps, evq.merger_out_len_max());

    // TODO
    let do_time_weight = true;
    let one_before_range = true;

    // TODO RangeFilter2 must accept SeriesRange
    let range = query.range().try_into()?;

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

    // TODO TimeBinnedStream must accept types bin edges.
    // Maybe even take a BinnedRangeEnum?
    let stream = TimeBinnedStream::new(stream, binned_range, do_time_weight, deadline);
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

    // TODO collect should not have to accept two ranges, instead, generalize over it.
    //let collected = crate::collect::collect(stream, deadline, bins_max, None, Some(binned_range.clone())).await?;
    let stream = futures_util::stream::empty();
    let collected = Collect::new(stream, deadline, evq.events_max(), Some(evq.range().clone()), None).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
