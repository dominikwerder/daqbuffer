use crate::rangefilter2::RangeFilter2;
use crate::tcprawclient::open_tcp_streams;
use err::Error;
use futures_util::stream;
use futures_util::StreamExt;
use items_0::on_sitemty_data;
use items_0::streamitem::sitem_data;
use items_0::Empty;
use items_2::channelevents::ChannelEvents;
use items_2::merger::Merger;
use netpod::log::*;
use netpod::ChConf;
use netpod::Cluster;
use query::api4::events::PlainEventsQuery;
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
    //let ev_agg_kind = evquery.agg_kind().as_ref().map_or(AggKind::Plain, |x| x.clone());
    //info!("plain_events_json  ev_agg_kind {:?}", ev_agg_kind);
    warn!("TODO feed through transform chain");
    let empty = if query.transform().is_pulse_id_diff() {
        use items_0::Empty;
        Box::new(items_2::eventsdim0::EventsDim0::<i64>::empty())
    } else {
        items_2::empty::empty_events_dyn_ev(&chconf.scalar_type, &chconf.shape)?
    };
    info!("plain_events_json  with empty item {}", empty.type_name());
    let empty = ChannelEvents::Events(empty);
    let empty = sitem_data(empty);
    // TODO should be able to ask for data-events only, instead of mixed data and status events.
    let inps = open_tcp_streams::<_, items_2::channelevents::ChannelEvents>(&evquery, cluster).await?;
    //let inps = open_tcp_streams::<_, Box<dyn items_2::Events>>(&query, cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader:
    let stream = Merger::new(inps, 1024);

    // Transforms that keep state between batches of events, usually only useful after merge.
    // Example: pulse-id-diff
    use futures_util::Stream;
    use items_0::streamitem::Sitemty;
    use std::pin::Pin;
    let stream: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>> = if query.transform().is_pulse_id_diff() {
        Box::pin(stream.map(|item| {
            let mut pulse_last = None;
            on_sitemty_data!(item, move |item| {
                use items_0::streamitem::RangeCompletableItem;
                use items_0::streamitem::StreamItem;
                use items_0::Appendable;
                let x = match item {
                    ChannelEvents::Events(item) => {
                        let (tss, pulses) = items_0::EventsNonObj::into_tss_pulses(item);
                        let mut item = items_2::eventsdim0::EventsDim0::empty();
                        for (ts, pulse) in tss.into_iter().zip(pulses) {
                            let value = if let Some(last) = pulse_last {
                                pulse as i64 - last as i64
                            } else {
                                0
                            };
                            item.push(ts, pulse, value);
                            pulse_last = Some(pulse);
                        }
                        ChannelEvents::Events(Box::new(item))
                    }
                    ChannelEvents::Status(x) => ChannelEvents::Status(x),
                };
                Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))
            })
        }))
    } else {
        Box::pin(stream)
    };

    let stream = stream.map(|item| {
        //info!("item after merge: {item:?}");
        item
    });
    let stream = RangeFilter2::new(stream, query.range().try_into()?, evquery.one_before_range());
    let stream = stream.map(|item| {
        //info!("item after rangefilter: {item:?}");
        item
    });
    let stream = stream::iter([empty]).chain(stream);
    let collected = crate::collect::collect(stream, deadline, events_max, Some(query.range().clone()), None).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
