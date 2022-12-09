use crate::tcprawclient::open_tcp_streams;
use err::Error;
#[allow(unused)]
use netpod::log::*;
use netpod::query::PlainEventsQuery;
use netpod::Cluster;
use serde_json::Value as JsonValue;
use std::time::Instant;

pub async fn plain_events_json(query: &PlainEventsQuery, cluster: &Cluster) -> Result<JsonValue, Error> {
    let deadline = Instant::now() + query.timeout();
    let events_max = query.events_max().unwrap_or(1024 * 32);
    // TODO should be able to ask for data-events only, instead of mixed data and status events.
    let inps = open_tcp_streams::<_, items_2::channelevents::ChannelEvents>(&query, cluster).await?;
    //let inps = open_tcp_streams::<_, Box<dyn items_2::Events>>(&query, cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader:
    #[cfg(NOTHING)]
    let stream = {
        let mut it = inps.into_iter();
        let inp0 = it.next().unwrap();
        let inp1 = it.next().unwrap();
        let inp2 = it.next().unwrap();
        let stream = inp0.chain(inp1).chain(inp2);
        stream
    };
    let stream = { items_2::merger::Merger::new(inps, 512) };
    let collected = crate::collect::collect(stream, deadline, events_max).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
