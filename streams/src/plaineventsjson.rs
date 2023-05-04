use crate::collect::Collect;
use crate::tcprawclient::open_tcp_streams;
use crate::transform::build_merged_event_transform;
use crate::transform::EventsToTimeBinnable;
use crate::transform::TimeBinnableToCollectable;
use err::Error;
use futures_util::StreamExt;
use items_0::collect_s::Collectable;
use items_0::on_sitemty_data;
use items_0::Events;
use items_2::channelevents::ChannelEvents;
use items_2::merger::Merger;
use items_2::streams::PlainEventStream;
use netpod::log::*;
use netpod::ChConf;
use netpod::Cluster;
use query::api4::events::PlainEventsQuery;
use serde_json::Value as JsonValue;
use std::time::Instant;

pub async fn plain_events_json(
    evq: &PlainEventsQuery,
    _chconf: &ChConf,
    cluster: &Cluster,
) -> Result<JsonValue, Error> {
    info!("plain_events_json  evquery {:?}", evq);
    // TODO remove magic constant
    let deadline = Instant::now() + evq.timeout();
    let mut tr = build_merged_event_transform(evq.transform())?;
    // TODO make sure the empty container arrives over the network.
    let inps = open_tcp_streams::<_, ChannelEvents>(&evq, cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader.
    // TODO use a mixture of count and byte-size as threshold.
    let stream = Merger::new(inps, evq.merger_out_len_max());
    #[cfg(DISABLED)]
    let stream = stream.map(|item| {
        info!("item after merge: {item:?}");
        item
    });
    //#[cfg(DISABLED)]
    let stream = crate::rangefilter2::RangeFilter2::new(stream, evq.range().try_into()?, evq.one_before_range());
    #[cfg(DISABLED)]
    let stream = stream.map(|item| {
        info!("item after rangefilter: {item:?}");
        item
    });
    let stream = stream.map(move |k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn Events> = Box::new(k);
            trace!("got len {}", k.len());
            let k = tr.0.transform(k);
            let k: Box<dyn Collectable> = Box::new(k);
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
        })
    });
    //let stream = PlainEventStream::new(stream);
    //let stream = EventsToTimeBinnable::new(stream);
    //let stream = TimeBinnableToCollectable::new(stream);
    let stream = Box::pin(stream);
    let collected = Collect::new(stream, deadline, evq.events_max(), Some(evq.range().clone()), None).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
