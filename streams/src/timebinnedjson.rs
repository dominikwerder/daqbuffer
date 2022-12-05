use crate::tcprawclient::open_tcp_streams;
use err::Error;
use futures_util::StreamExt;
#[allow(unused)]
use netpod::log::*;
use netpod::query::{BinnedQuery, RawEventsQuery};
use netpod::{BinnedRange, Cluster};
use serde_json::Value as JsonValue;
use std::time::{Duration, Instant};

pub async fn timebinned_json(query: &BinnedQuery, cluster: &Cluster) -> Result<JsonValue, Error> {
    let binned_range = BinnedRange::covering_range(query.range().clone(), query.bin_count())?;
    let events_max = 10000;
    let do_time_weight = query.agg_kind().do_time_weighted();
    let deadline = Instant::now() + Duration::from_millis(7500);
    let rawquery = RawEventsQuery::new(query.channel().clone(), query.range().clone(), query.agg_kind().clone());
    let inps = open_tcp_streams::<_, items_2::channelevents::ChannelEvents>(&rawquery, cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader:
    let stream = { items_2::merger::Merger::new(inps, 1) };
    let stream = Box::pin(stream);
    let stream = crate::timebin::TimeBinnedStream::new(stream, binned_range.edges(), do_time_weight, deadline);
    if false {
        let mut stream = stream;
        let _: Option<items::Sitemty<Box<dyn items_0::TimeBinned>>> = stream.next().await;
        panic!()
    }
    let collected = crate::collect::collect(stream, deadline, events_max).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
