use crate::cbor_stream::SitemtyDynEventsStream;
use bytes::Bytes;
use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::StreamItem;
use items_0::Events;
use items_0::WithLen;
use netpod::log::*;
use std::pin::Pin;
use std::time::Duration;

pub struct JsonBytes(Bytes);

impl JsonBytes {
    pub fn into_inner(self) -> Bytes {
        self.0
    }

    pub fn len(&self) -> u32 {
        self.0.len() as _
    }
}

impl WithLen for JsonBytes {
    fn len(&self) -> usize {
        self.len() as usize
    }
}

impl From<JsonBytes> for Bytes {
    fn from(value: JsonBytes) -> Self {
        value.0
    }
}

pub type JsonStream = Pin<Box<dyn Stream<Item = Result<JsonBytes, Error>> + Send>>;

pub fn events_stream_to_json_stream(stream: SitemtyDynEventsStream) -> impl Stream<Item = Result<JsonBytes, Error>> {
    let interval = tokio::time::interval(Duration::from_millis(4000));
    let stream = tokio_stream::StreamExt::timeout_repeating(stream, interval).map(|x| match x {
        Ok(x) => map_events(x),
        Err(_) => make_keepalive(),
    });
    let prepend = {
        let item = make_keepalive();
        futures_util::stream::iter([item])
    };
    prepend.chain(stream)
}

fn map_events(x: Result<StreamItem<RangeCompletableItem<Box<dyn Events>>>, Error>) -> Result<JsonBytes, Error> {
    match x {
        Ok(x) => match x {
            StreamItem::DataItem(x) => match x {
                RangeCompletableItem::Data(evs) => {
                    let buf = evs.to_json_vec_u8();
                    let bytes = Bytes::from(buf);
                    let item = JsonBytes(bytes);
                    Ok(item)
                }
                RangeCompletableItem::RangeComplete => {
                    let item = serde_json::json!({
                        "rangeFinal": true,
                    });
                    let buf = serde_json::to_vec(&item)?;
                    let bytes = Bytes::from(buf);
                    let item = JsonBytes(bytes);
                    Ok(item)
                }
            },
            StreamItem::Log(item) => {
                info!("{item:?}");
                let item = JsonBytes(Bytes::new());
                Ok(item)
            }
            StreamItem::Stats(item) => {
                info!("{item:?}");
                let item = JsonBytes(Bytes::new());
                Ok(item)
            }
        },
        Err(e) => {
            let item = serde_json::json!({
                "error": e.to_string(),
            });
            let buf = serde_json::to_vec(&item)?;
            let bytes = Bytes::from(buf);
            let item = JsonBytes(bytes);
            Ok(item)
        }
    }
}

fn make_keepalive() -> Result<JsonBytes, Error> {
    let item = serde_json::json!({
        "type": "keepalive",
    });
    let buf = serde_json::to_vec(&item).unwrap();
    let bytes = Bytes::from(buf);
    let item = Ok(JsonBytes(bytes));
    item
}
