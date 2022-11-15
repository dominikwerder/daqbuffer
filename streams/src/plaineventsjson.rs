use crate::merge::open_tcp_streams;
use bytes::Bytes;
use err::Error;
use futures_util::{future, stream, FutureExt, Stream, StreamExt};
use items::streams::collect_plain_events_json;
use items::{sitem_data, RangeCompletableItem, Sitemty, StreamItem};
use items_2::ChannelEvents;
use netpod::log::*;
use netpod::Cluster;
use serde::Serialize;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub struct BytesStream(Pin<Box<dyn Stream<Item = Sitemty<Bytes>> + Send>>);

impl Stream for BytesStream {
    type Item = Sitemty<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        StreamExt::poll_next_unpin(&mut self, cx)
    }
}

pub async fn plain_events_json<SER>(query: SER, cluster: &Cluster) -> Result<BytesStream, Error>
where
    SER: Serialize,
{
    let inps = open_tcp_streams(&query, cluster).await?;
    let mut merged = items_2::merger::ChannelEventsMerger::new(inps);
    let timeout = Duration::from_millis(2000);
    let events_max = 100;
    let do_log = false;
    let mut coll = None;
    while let Some(item) = merged.next().await {
        let item = item?;
        match item {
            StreamItem::DataItem(item) => match item {
                RangeCompletableItem::RangeComplete => todo!(),
                RangeCompletableItem::Data(item) => match item {
                    ChannelEvents::Events(mut item) => {
                        if coll.is_none() {
                            coll = Some(item.new_collector());
                        }
                        let coll = coll
                            .as_mut()
                            .ok_or_else(|| Error::with_msg_no_trace(format!("no collector")))?;
                        coll.ingest(&mut item);
                    }
                    ChannelEvents::Status(_) => todo!(),
                },
            },
            StreamItem::Log(item) => {
                info!("log {item:?}");
            }
            StreamItem::Stats(item) => {
                info!("stats {item:?}");
            }
        }
    }
    // TODO compare with
    // streams::collect::collect_plain_events_json
    // and remove duplicate functionality.
    let mut coll = coll.ok_or_else(|| Error::with_msg_no_trace(format!("no collector created")))?;
    let res = coll.result()?;
    // TODO factor the conversion of the result out to a higher level.
    // The output of this function should again be collectable, maybe even binnable and otherwise processable.
    let js = serde_json::to_vec(&res)?;
    let item = sitem_data(Bytes::from(js));
    let stream = stream::once(future::ready(item));
    let stream = BytesStream(Box::pin(stream));
    Ok(stream)
}
