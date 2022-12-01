use crate::tcprawclient::open_tcp_streams;
use err::Error;
use futures_util::StreamExt;
#[allow(unused)]
use netpod::log::*;
use netpod::Cluster;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::time::{Duration, Instant};

pub async fn timebinned_json<SER>(query: SER, cluster: &Cluster) -> Result<JsonValue, Error>
where
    SER: Serialize,
{
    // TODO should be able to ask for data-events only, instead of mixed data and status events.
    let inps = open_tcp_streams::<_, items_2::channelevents::ChannelEvents>(&query, cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader:
    let stream = { items_2::merger::Merger::new(inps, 1) };
    let events_max = 10000;
    let do_time_weight = true;
    let deadline = Instant::now() + Duration::from_millis(7500);
    let stream = Box::pin(stream);
    let stream = crate::timebin::TimeBinnedStream::new(stream, Vec::new(), do_time_weight, deadline);
    if false {
        let mut stream = stream;
        let _: Option<items::Sitemty<Box<dyn items_0::TimeBinned>>> = stream.next().await;
        panic!()
    }
    let collected = crate::collect::collect(stream, deadline, events_max).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
