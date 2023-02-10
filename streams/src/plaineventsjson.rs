use crate::tcprawclient::open_tcp_streams;
use err::Error;
use futures_util::stream;
use futures_util::StreamExt;
use items_2::channelevents::ChannelEvents;
use netpod::log::*;
use netpod::query::PlainEventsQuery;
use netpod::AggKind;
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
    if query.channel().name() == "wasm-test-01" {
        use wasmer::Value;
        let wasm = query.channel().name().as_bytes();
        let mut store = wasmer::Store::default();
        let module = wasmer::Module::new(&store, wasm).unwrap();
        let import_object = wasmer::imports! {};
        let instance = wasmer::Instance::new(&mut store, &module, &import_object).unwrap();
        let add_one = instance.exports.get_function("event_transform").unwrap();
        let result = add_one.call(&mut store, &[Value::I32(42)]).unwrap();
        assert_eq!(result[0], Value::I32(43));
    }
    // TODO remove magic constant
    let deadline = Instant::now() + query.timeout() + Duration::from_millis(1000);
    let events_max = query.events_max();
    let evquery = query.clone();
    info!("plain_events_json  evquery {:?}", evquery);
    let ev_agg_kind = evquery.agg_kind().as_ref().map_or(AggKind::Plain, |x| x.clone());
    info!("plain_events_json  ev_agg_kind {:?}", ev_agg_kind);
    let empty = items_2::empty_events_dyn_ev(&chconf.scalar_type, &chconf.shape, &ev_agg_kind)?;
    info!("plain_events_json  with empty item {}", empty.type_name());
    let empty = ChannelEvents::Events(empty);
    let empty = items::sitem_data(empty);
    // TODO should be able to ask for data-events only, instead of mixed data and status events.
    let inps = open_tcp_streams::<_, items_2::channelevents::ChannelEvents>(&evquery, cluster).await?;
    //let inps = open_tcp_streams::<_, Box<dyn items_2::Events>>(&query, cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader:
    let stream = items_2::merger::Merger::new(inps, 1024);
    let stream = crate::rangefilter2::RangeFilter2::new(stream, query.range().clone(), evquery.one_before_range());
    let stream = stream::iter([empty]).chain(stream);
    let collected = crate::collect::collect(stream, deadline, events_max, Some(query.range().clone()), None).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
