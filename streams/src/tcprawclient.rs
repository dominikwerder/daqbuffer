/*!
Delivers event data.

Delivers event data (not yet time-binned) from local storage and provides client functions
to request such data from nodes.
*/

use crate::frames::eventsfromframes::EventsFromFrames;
use crate::frames::inmem::InMemoryFrameAsyncReadStream;
use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items::eventfull::EventFull;
use items::frame::make_frame;
use items::frame::make_term_frame;
use items::sitem_data;
use items::EventQueryJsonStringFrame;
use items::RangeCompletableItem;
use items::Sitemty;
use items::StreamItem;
use netpod::log::*;
use netpod::query::PlainEventsQuery;
use netpod::Cluster;
use netpod::Node;
use netpod::PerfOpts;
use std::fmt;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub async fn x_processed_event_blobs_stream_from_node(
    query: PlainEventsQuery,
    perf_opts: PerfOpts,
    node: Node,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    debug!(
        "x_processed_event_blobs_stream_from_node  to: {}:{}",
        node.host, node.port_raw
    );
    let net = TcpStream::connect(format!("{}:{}", node.host, node.port_raw)).await?;
    let qjs = serde_json::to_string(&query)?;
    let (netin, mut netout) = net.into_split();
    let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(
        EventQueryJsonStringFrame(qjs),
    )));
    let buf = make_frame(&item)?;
    netout.write_all(&buf).await?;
    let buf = make_term_frame()?;
    netout.write_all(&buf).await?;
    netout.flush().await?;
    netout.forget();
    let frames = InMemoryFrameAsyncReadStream::new(netin, perf_opts.inmem_bufcap);
    let frames = Box::pin(frames) as _;
    let items = EventsFromFrames::new(frames);
    Ok(Box::pin(items))
}

pub type BoxedStream<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;

pub async fn open_tcp_streams<Q, T>(query: Q, cluster: &Cluster) -> Result<Vec<BoxedStream<T>>, Error>
where
    Q: serde::Serialize,
    // Group bounds in new trait
    T: items::FrameTypeInnerStatic + serde::de::DeserializeOwned + Send + Unpin + fmt::Debug + 'static,
{
    // TODO when unit tests established, change to async connect:
    let mut streams = Vec::new();
    for node in &cluster.nodes {
        debug!("open_tcp_streams  to: {}:{}", node.host, node.port_raw);
        let net = TcpStream::connect(format!("{}:{}", node.host, node.port_raw)).await?;
        let qjs = serde_json::to_string(&query)?;
        let (netin, mut netout) = net.into_split();
        let item = EventQueryJsonStringFrame(qjs);
        let item = sitem_data(item);
        let buf = make_frame(&item)?;
        netout.write_all(&buf).await?;
        let buf = make_term_frame()?;
        netout.write_all(&buf).await?;
        netout.flush().await?;
        netout.forget();
        // TODO for images, we need larger buffer capacity
        let frames = InMemoryFrameAsyncReadStream::new(netin, 1024 * 1024 * 2);
        let frames = Box::pin(frames) as _;
        let stream = EventsFromFrames::<T>::new(frames);
        let stream = stream.map(|x| {
            info!("tcp stream recv sees item {x:?}");
            x
        });
        streams.push(Box::pin(stream) as _);
    }
    Ok(streams)
}
