use crate::rangefilter2::RangeFilter2;
use crate::tcprawclient::open_tcp_streams;
use crate::timebin::TimeBinnedStream;
use err::Error;
use futures_util::stream;
use futures_util::StreamExt;
use items_0::streamitem::sitem_data;
use items_0::streamitem::Sitemty;
use items_2::channelevents::ChannelEvents;
use items_2::merger::Merger;
#[allow(unused)]
use netpod::log::*;
use netpod::query::BinnedQuery;
use netpod::query::PlainEventsQuery;
use netpod::BinnedRangeEnum;
use netpod::ChConf;
use netpod::Cluster;
use serde_json::Value as JsonValue;
use std::time::Instant;

pub async fn timebinned_json(query: &BinnedQuery, chconf: &ChConf, cluster: &Cluster) -> Result<JsonValue, Error> {
    let binned_range = BinnedRangeEnum::covering_range(query.range().clone(), query.bin_count())?;
    let bins_max = 10000;
    //let do_time_weight = query.agg_kind().do_time_weighted();
    let deadline = Instant::now() + query.timeout_value();
    let empty = items_2::empty::empty_events_dyn_ev(&chconf.scalar_type, &chconf.shape)?;
    error!("TODO feed through transform chain");
    let empty = ChannelEvents::Events(empty);
    let empty = sitem_data(empty);
    error!("TODO add with_deadline to PlainEventsQuery");
    todo!();
    let evquery = PlainEventsQuery::new(query.channel().clone(), query.range().clone());
    let inps = open_tcp_streams::<_, items_2::channelevents::ChannelEvents>(&evquery, cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader:
    info!("timebinned_json with empty item {empty:?}");
    let stream = Merger::new(inps, 128);
    let stream = stream::iter([empty]).chain(stream);
    let stream = RangeFilter2::new(stream, todo!(), evquery.one_before_range());
    let stream = Box::pin(stream);
    let do_time_weight = todo!();
    let stream = TimeBinnedStream::new(stream, todo!(), do_time_weight, deadline);
    if false {
        let mut stream = stream;
        let _: Option<Sitemty<Box<dyn items_0::TimeBinned>>> = stream.next().await;
        panic!()
    }
    let collected = crate::collect::collect(stream, deadline, bins_max, None, Some(binned_range.clone())).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
