use crate::tcprawclient::open_tcp_streams;
use bytes::Bytes;
use err::Error;
use futures_util::{Stream, StreamExt};
use items::Sitemty;
#[allow(unused)]
use netpod::log::*;
use netpod::Cluster;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub struct BytesStream(Pin<Box<dyn Stream<Item = Sitemty<Bytes>> + Send>>);

impl Stream for BytesStream {
    type Item = Sitemty<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        StreamExt::poll_next_unpin(&mut self, cx)
    }
}

// TODO remove?
pub async fn plain_events_json<SER>(query: SER, cluster: &Cluster) -> Result<JsonValue, Error>
where
    SER: Serialize,
{
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
    let deadline = Instant::now() + Duration::from_millis(8000);
    let events_max = 100;
    let collected = crate::collect::collect(stream, deadline, events_max).await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
