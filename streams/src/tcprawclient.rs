/*!
Delivers event data.

Delivers event data (not yet time-binned) from local storage and provides client functions
to request such data from nodes.
*/

use crate::frames::eventsfromframes::EventsFromFrames;
use crate::frames::inmem::InMemoryFrameAsyncReadStream;
use err::Error;
use futures_util::Stream;
use items_0::framable::FrameTypeInnerStatic;
use items_0::streamitem::sitem_data;
use items_0::streamitem::Sitemty;
use items_2::eventfull::EventFull;
use items_2::framable::EventQueryJsonStringFrame;
use items_2::framable::Framable;
use items_2::frame::make_term_frame;
use netpod::log::*;
use netpod::ChannelTypeConfigGen;
use netpod::Cluster;
use netpod::Node;
use netpod::PerfOpts;
use query::api4::events::PlainEventsQuery;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub async fn x_processed_event_blobs_stream_from_node(
    query: PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    perf_opts: PerfOpts,
    node: Node,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    let addr = format!("{}:{}", node.host, node.port_raw);
    debug!("x_processed_event_blobs_stream_from_node  to: {addr}",);
    let net = TcpStream::connect(addr.clone()).await?;
    let qjs = serde_json::to_string(&query)?;
    let (netin, mut netout) = net.into_split();

    let item = sitem_data(EventQueryJsonStringFrame(qjs));
    let buf = item.make_frame()?;
    netout.write_all(&buf).await?;

    let s = serde_json::to_string(&ch_conf)?;
    let item = sitem_data(EventQueryJsonStringFrame(s));
    let buf = item.make_frame()?;
    netout.write_all(&buf).await?;

    let buf = make_term_frame()?;
    netout.write_all(&buf).await?;
    netout.flush().await?;
    netout.forget();
    let frames = InMemoryFrameAsyncReadStream::new(netin, perf_opts.inmem_bufcap);
    let frames = Box::pin(frames);
    let items = EventsFromFrames::new(frames, addr);
    Ok(Box::pin(items))
}

pub type BoxedStream<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;

pub async fn open_tcp_streams<Q, T>(
    query: Q,
    ch_conf: &ChannelTypeConfigGen,
    cluster: &Cluster,
) -> Result<Vec<BoxedStream<T>>, Error>
where
    Q: Serialize,
    // Group bounds in new trait
    T: FrameTypeInnerStatic + DeserializeOwned + Send + Unpin + fmt::Debug + 'static,
{
    // TODO when unit tests established, change to async connect:
    let mut streams = Vec::new();
    for node in &cluster.nodes {
        let addr = format!("{}:{}", node.host, node.port_raw);
        debug!("open_tcp_streams  to: {addr}");
        let net = TcpStream::connect(addr.clone()).await?;
        let qjs = serde_json::to_string(&query)?;
        let (netin, mut netout) = net.into_split();

        let item = sitem_data(EventQueryJsonStringFrame(qjs));
        let buf = item.make_frame()?;
        netout.write_all(&buf).await?;

        let s = serde_json::to_string(ch_conf)?;
        let item = sitem_data(EventQueryJsonStringFrame(s));
        let buf = item.make_frame()?;
        netout.write_all(&buf).await?;

        let buf = make_term_frame()?;
        netout.write_all(&buf).await?;
        netout.flush().await?;
        netout.forget();
        // TODO for images, we need larger buffer capacity
        let perf_opts = PerfOpts::default();
        let frames = InMemoryFrameAsyncReadStream::new(netin, perf_opts.inmem_bufcap);
        let frames = Box::pin(frames);
        let stream = EventsFromFrames::<T>::new(frames, addr);
        streams.push(Box::pin(stream) as _);
    }
    Ok(streams)
}
