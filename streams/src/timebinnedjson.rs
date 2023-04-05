use crate::rangefilter2::RangeFilter2;
use crate::tcprawclient::open_tcp_streams;
use crate::timebin::TimeBinnedStream;
use err::Error;
use futures_util::stream;
use futures_util::StreamExt;
use items_0::streamitem::sitem_data;
use items_0::streamitem::Sitemty;
use items_0::timebin::TimeBinned;
use items_2::channelevents::ChannelEvents;
use items_2::merger::Merger;
use netpod::log::*;
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
    let empty = items_2::empty::empty_events_dyn_ev(&chconf.scalar_type, &chconf.shape)?;
    warn!("TODO feed through transform chain");
    let empty = ChannelEvents::Events(empty);
    let empty = sitem_data(empty);

    // TODO
    let evquery = PlainEventsQuery::new(query.channel().clone(), query.range().clone()).for_time_weighted_scalar();
    let inps = open_tcp_streams::<_, items_2::channelevents::ChannelEvents>(&evquery, cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader:
    info!("timebinned_json with empty item {empty:?}");
    let stream = Merger::new(inps, 1024);
    let stream = stream::iter([empty]).chain(stream);

    // TODO
    let do_time_weight = true;
    let one_before_range = true;

    // TODO RangeFilter2 must accept SeriesRange
    let range = query.range().try_into()?;

    let stream = RangeFilter2::new(stream, range, one_before_range);
    let stream = Box::pin(stream);

    // TODO TimeBinnedStream must accept types bin edges.
    // Maybe even take a BinnedRangeEnum?
    let stream = TimeBinnedStream::new(stream, todo!(), do_time_weight, deadline);
    if false {
        let mut stream = stream;
        let _: Option<Sitemty<Box<dyn TimeBinned>>> = stream.next().await;
        panic!()
    }

    // TODO collect should not have to accept two ranges, instead, generalize over it.
    let collected = crate::collect::collect(stream, deadline, bins_max, None, Some(binned_range.clone())).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
