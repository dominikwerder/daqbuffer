use crate::collect::Collect;
use crate::plaineventsstream::dyn_events_stream;
use crate::tcprawclient::OpenBoxedBytesStreamsBox;
use err::Error;
use futures_util::StreamExt;
use items_0::collect_s::Collectable;
use items_0::on_sitemty_data;
use netpod::log::*;
use netpod::ChannelTypeConfigGen;
use netpod::Cluster;
use netpod::ReqCtx;
use query::api4::events::PlainEventsQuery;
use serde_json::Value as JsonValue;
use std::time::Instant;

pub async fn plain_events_json(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    _cluster: &Cluster,
    open_bytes: OpenBoxedBytesStreamsBox,
) -> Result<JsonValue, Error> {
    info!("plain_events_json  evquery {:?}", evq);
    let deadline = Instant::now() + evq.timeout();

    let stream = dyn_events_stream(evq, ch_conf, ctx, open_bytes).await?;

    let stream = stream.map(move |k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn Collectable> = Box::new(k);
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
        })
    });

    //let stream = PlainEventStream::new(stream);
    //let stream = EventsToTimeBinnable::new(stream);
    //let stream = TimeBinnableToCollectable::new(stream);
    let stream = Box::pin(stream);
    info!("plain_events_json  boxed stream created");
    let collected = Collect::new(
        stream,
        deadline,
        evq.events_max(),
        evq.bytes_max(),
        Some(evq.range().clone()),
        None,
    )
    .await?;
    info!("plain_events_json  collected");
    let jsval = serde_json::to_value(&collected)?;
    info!("plain_events_json  json serialized");
    Ok(jsval)
}
