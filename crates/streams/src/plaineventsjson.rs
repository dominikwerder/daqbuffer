use crate::collect::Collect;
use crate::plaineventsstream::dyn_events_stream;
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
    cluster: &Cluster,
) -> Result<JsonValue, Error> {
    info!("plain_events_json  evquery {:?}", evq);
    let deadline = Instant::now() + evq.timeout();

    let stream = dyn_events_stream(evq, ch_conf, ctx, cluster).await?;

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
    let collected = Collect::new(stream, deadline, evq.events_max(), Some(evq.range().clone()), None).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
