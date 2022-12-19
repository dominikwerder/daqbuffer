use crate::tcprawclient::open_tcp_streams;
use err::Error;
use futures_util::stream;
use futures_util::StreamExt;
use items_2::channelevents::ChannelEvents;
use netpod::query::PlainEventsQuery;
use netpod::ChConf;
use netpod::Cluster;
use serde_json::Value as JsonValue;
use std::time::Duration;
use std::time::Instant;

pub async fn plain_events_json(
    query: &PlainEventsQuery,
    chconf: &ChConf,
    cluster: &Cluster,
) -> Result<JsonValue, Error> {
    // TODO remove magic constant
    let deadline = Instant::now() + query.timeout() + Duration::from_millis(1000);
    let events_max = query.events_max();
    let _empty = items::empty_events_dyn(&chconf.scalar_type, &chconf.shape, query.agg_kind());
    let _empty = items_2::empty_events_dyn(&chconf.scalar_type, &chconf.shape, query.agg_kind());
    let empty = items_2::empty_events_dyn_2(&chconf.scalar_type, &chconf.shape, query.agg_kind());
    let empty = ChannelEvents::Events(empty);
    let empty = items::sitem_data(empty);
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
    let stream = { items_2::merger::Merger::new(inps, 1) };
    let stream = stream::iter([empty]).chain(stream);
    let collected = crate::collect::collect(stream, deadline, events_max, Some(query.range().clone()), None).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
