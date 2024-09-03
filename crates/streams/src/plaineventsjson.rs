use crate::collect::Collect;
use crate::firsterr::non_empty;
use crate::firsterr::only_first_err;
use crate::json_stream::events_stream_to_json_stream;
use crate::json_stream::JsonStream;
use crate::plaineventsstream::dyn_events_stream;
use crate::tcprawclient::OpenBoxedBytesStreamsBox;
use err::thiserror;
use err::ThisError;
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

#[derive(Debug, ThisError)]
#[cstm(name = "PlainEventsJson")]
pub enum Error {
    Stream(#[from] crate::plaineventsstream::Error),
    Collect(err::Error),
    Json(#[from] serde_json::Error),
}

pub async fn plain_events_json(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    _cluster: &Cluster,
    open_bytes: OpenBoxedBytesStreamsBox,
) -> Result<JsonValue, Error> {
    debug!("plain_events_json  evquery {:?}", evq);
    let deadline = Instant::now() + evq.timeout();

    let stream = dyn_events_stream(evq, ch_conf, ctx, open_bytes).await?;

    let stream = stream.map(move |k| {
        on_sitemty_data!(k, |mut k: Box<dyn items_0::Events>| {
            if let Some(j) = k.as_any_mut().downcast_mut::<items_2::channelevents::ChannelEvents>() {
                use items_0::AsAnyMut;
                match j {
                    items_2::channelevents::ChannelEvents::Events(m) => {
                        if let Some(g) = m
                            .as_any_mut()
                            .downcast_mut::<items_2::eventsdim0::EventsDim0<netpod::EnumVariant>>()
                        {
                            trace!("consider container EnumVariant");
                            let mut out = items_2::eventsdim0::EventsDim0Enum::new();
                            for (&ts, val) in g.tss.iter().zip(g.values.iter()) {
                                out.push_back(ts, val.ix(), val.name_string());
                            }
                            let k: Box<dyn Collectable> = Box::new(out);
                            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
                        } else {
                            trace!("consider container channel events other events  {}", k.type_name());
                            let k: Box<dyn Collectable> = Box::new(k);
                            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
                        }
                    }
                    items_2::channelevents::ChannelEvents::Status(_) => {
                        trace!("consider container channel events status  {}", k.type_name());
                        let k: Box<dyn Collectable> = Box::new(k);
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
                    }
                }
            } else {
                trace!("consider container else  {}", k.type_name());
                let k: Box<dyn Collectable> = Box::new(k);
                Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
            }
        })
    });

    //let stream = PlainEventStream::new(stream);
    //let stream = EventsToTimeBinnable::new(stream);
    //let stream = TimeBinnableToCollectable::new(stream);
    let stream = Box::pin(stream);
    debug!("plain_events_json  boxed stream created");
    let collected = Collect::new(
        stream,
        deadline,
        evq.events_max(),
        evq.bytes_max(),
        Some(evq.range().clone()),
        None,
    )
    .await
    .map_err(Error::Collect)?;
    debug!("plain_events_json  collected");
    let jsval = serde_json::to_value(&collected)?;
    debug!("plain_events_json  json serialized");
    Ok(jsval)
}

pub async fn plain_events_json_stream(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    open_bytes: OpenBoxedBytesStreamsBox,
) -> Result<JsonStream, Error> {
    trace!("build stream");
    let stream = dyn_events_stream(evq, ch_conf, ctx, open_bytes).await?;
    let stream = events_stream_to_json_stream(stream);
    let stream = non_empty(stream);
    let stream = only_first_err(stream);
    Ok(Box::pin(stream))
}
