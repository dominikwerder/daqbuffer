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
use netpod::Cluster;
use netpod::Node;
use query::api4::events::EventsSubQuery;
use query::api4::events::Frame1Parts;
use serde::de::DeserializeOwned;
use std::fmt;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub fn make_node_command_frame(query: EventsSubQuery) -> Result<EventQueryJsonStringFrame, Error> {
    let obj = Frame1Parts::new(query);
    let ret = serde_json::to_string(&obj)?;
    Ok(EventQueryJsonStringFrame(ret))
}

pub async fn x_processed_event_blobs_stream_from_node(
    subq: EventsSubQuery,
    node: Node,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    let addr = format!("{}:{}", node.host, node.port_raw);
    debug!("x_processed_event_blobs_stream_from_node  to: {addr}",);
    let frame1 = make_node_command_frame(subq.clone())?;
    let net = TcpStream::connect(addr.clone()).await?;
    let (netin, mut netout) = net.into_split();
    let item = sitem_data(frame1);
    let buf = item.make_frame()?;
    netout.write_all(&buf).await?;
    let buf = make_term_frame()?;
    netout.write_all(&buf).await?;
    netout.flush().await?;
    netout.forget();
    let frames = InMemoryFrameAsyncReadStream::new(netin, subq.inmem_bufcap());
    let frames = Box::pin(frames);
    let items = EventsFromFrames::new(frames, addr);
    Ok(Box::pin(items))
}

pub type BoxedStream<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;

pub async fn open_tcp_streams<T>(subq: EventsSubQuery, cluster: &Cluster) -> Result<Vec<BoxedStream<T>>, Error>
where
    // Group bounds in new trait
    T: FrameTypeInnerStatic + DeserializeOwned + Send + Unpin + fmt::Debug + 'static,
{
    // TODO when unit tests established, change to async connect:
    let frame1 = make_node_command_frame(subq.clone())?;
    let mut streams = Vec::new();
    for node in &cluster.nodes {
        let addr = format!("{}:{}", node.host, node.port_raw);
        debug!("open_tcp_streams  to: {addr}");
        let net = TcpStream::connect(addr.clone()).await?;
        let (netin, mut netout) = net.into_split();
        let item = sitem_data(frame1.clone());
        let buf = item.make_frame()?;
        netout.write_all(&buf).await?;
        let buf = make_term_frame()?;
        netout.write_all(&buf).await?;
        netout.flush().await?;
        netout.forget();
        // TODO for images, we need larger buffer capacity
        let frames = InMemoryFrameAsyncReadStream::new(netin, subq.inmem_bufcap());
        let frames = Box::pin(frames);
        let stream = EventsFromFrames::<T>::new(frames, addr);
        streams.push(Box::pin(stream) as _);
    }
    Ok(streams)
}
