use crate::plaineventsstream::dyn_events_stream;
use bytes::Bytes;
use err::Error;
use futures_util::future;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::StreamItem;
use netpod::log::Level;
use netpod::log::*;
use netpod::ChannelTypeConfigGen;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use query::api4::events::PlainEventsQuery;
use std::pin::Pin;

pub struct CborBytes(Bytes);

impl CborBytes {
    pub fn into_inner(self) -> Bytes {
        self.0
    }
}

pub type CborStream = Pin<Box<dyn Stream<Item = Result<CborBytes, Error>> + Send>>;

pub async fn plain_events_cbor(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> Result<CborStream, Error> {
    let stream = dyn_events_stream(evq, ch_conf, ctx, &ncc.node_config.cluster).await?;
    let stream = stream
        .map(|x| match x {
            Ok(x) => match x {
                StreamItem::DataItem(x) => match x {
                    RangeCompletableItem::Data(evs) => {
                        if false {
                            use items_0::AsAnyRef;
                            // TODO impl generically on EventsDim0 ?
                            if let Some(evs) = evs.as_any_ref().downcast_ref::<items_2::eventsdim0::EventsDim0<f64>>() {
                                let mut buf = Vec::new();
                                ciborium::into_writer(evs, &mut buf)
                                    .map_err(|e| Error::with_msg_no_trace(format!("{e}")))?;
                                let bytes = Bytes::from(buf);
                                let _item = CborBytes(bytes);
                                // Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                            } else {
                                let _item = LogItem::from_node(0, Level::DEBUG, format!("cbor stream discarded item"));
                                // Ok(StreamItem::Log(item))
                            };
                        }
                        let buf = evs.to_cbor_vec_u8();
                        let bytes = Bytes::from(buf);
                        let item = CborBytes(bytes);
                        Ok(item)
                    }
                    RangeCompletableItem::RangeComplete => {
                        use ciborium::cbor;
                        let item = cbor!({
                            "rangeFinal" => true,
                        })
                        .map_err(Error::from_string)?;
                        let mut buf = Vec::with_capacity(64);
                        ciborium::into_writer(&item, &mut buf).map_err(Error::from_string)?;
                        let bytes = Bytes::from(buf);
                        let item = CborBytes(bytes);
                        Ok(item)
                    }
                },
                StreamItem::Log(item) => {
                    info!("{item:?}");
                    let item = CborBytes(Bytes::new());
                    Ok(item)
                }
                StreamItem::Stats(item) => {
                    info!("{item:?}");
                    let item = CborBytes(Bytes::new());
                    Ok(item)
                }
            },
            Err(e) => {
                use ciborium::cbor;
                let item = cbor!({
                    "error" => e.to_string(),
                })
                .map_err(Error::from_string)?;
                let mut buf = Vec::with_capacity(64);
                ciborium::into_writer(&item, &mut buf).map_err(Error::from_string)?;
                let bytes = Bytes::from(buf);
                let item = CborBytes(bytes);
                Ok(item)
            }
        })
        .filter(|x| {
            future::ready(match x {
                Ok(x) => x.0.len() > 0,
                Err(_) => true,
            })
        })
        .take_while({
            let mut state = true;
            move |x| {
                let ret = state;
                if x.is_err() {
                    state = false;
                }
                future::ready(ret)
            }
        });
    Ok(Box::pin(stream))
}
