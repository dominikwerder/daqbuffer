use crate::tcprawclient::open_tcp_streams;
use err::Error;
use futures_util::stream;
use futures_util::StreamExt;
use items_2::channelevents::ChannelEvents;
#[allow(unused)]
use netpod::log::*;
use netpod::query::BinnedQuery;
use netpod::query::PlainEventsQuery;
use netpod::BinnedRange;
use netpod::ChConf;
use netpod::Cluster;
use serde_json::Value as JsonValue;
use std::time::Duration;
use std::time::Instant;

pub async fn timebinned_json(query: &BinnedQuery, chconf: &ChConf, cluster: &Cluster) -> Result<JsonValue, Error> {
    let binned_range = BinnedRange::covering_range(query.range().clone(), query.bin_count())?;
    let bins_max = 10000;
    let do_time_weight = query.agg_kind().do_time_weighted();
    let timeout = Duration::from_millis(7500);
    let deadline = Instant::now() + timeout;
    let _empty = items::empty_events_dyn(&chconf.scalar_type, &chconf.shape, query.agg_kind());
    let _empty = items_2::empty_events_dyn(&chconf.scalar_type, &chconf.shape, query.agg_kind());
    let empty = items_2::empty_events_dyn_2(&chconf.scalar_type, &chconf.shape, query.agg_kind());
    let empty = ChannelEvents::Events(empty);
    let empty = items::sitem_data(empty);
    let rawquery = PlainEventsQuery::new(
        query.channel().clone(),
        query.range().clone(),
        query.agg_kind().clone(),
        timeout,
        None,
        true,
    );
    let inps = open_tcp_streams::<_, items_2::channelevents::ChannelEvents>(&rawquery, cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader:
    netpod::log::info!("timebinned_json with empty item {empty:?}");
    let stream = items_2::merger::Merger::new(inps, 128);
    let stream = stream::iter([empty]).chain(stream);
    let stream = Box::pin(stream);
    let stream = crate::timebin::TimeBinnedStream::new(stream, binned_range.edges(), do_time_weight, deadline);
    if false {
        let mut stream = stream;
        let _: Option<items::Sitemty<Box<dyn items_0::TimeBinned>>> = stream.next().await;
        panic!()
    }
    let collected = crate::collect::collect(stream, deadline, bins_max, None, Some(binned_range.clone())).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
