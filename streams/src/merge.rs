// Sets up the raw tcp connections: disk::merge::mergedfromremotes::MergedFromRemotes
// and then sets up a disk::merge::MergedStream

pub mod mergedstream;

use crate::frames::eventsfromframes::EventsFromFrames;
use crate::frames::inmem::InMemoryFrameAsyncReadStream;
use err::Error;
use futures_util::Stream;
use items::frame::make_frame;
use items::frame::make_term_frame;
use items::sitem_data;
use items::EventQueryJsonStringFrame;
use items::Sitemty;
use items_2::ChannelEvents;
use netpod::log::*;
use netpod::Cluster;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub type ChannelEventsBoxedStream = Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>;

pub async fn open_tcp_streams(
    query: &dyn erased_serde::Serialize,
    cluster: &Cluster,
) -> Result<Vec<ChannelEventsBoxedStream>, Error> {
    // TODO when unit tests established, change to async connect:
    let mut streams = Vec::new();
    for node in &cluster.nodes {
        debug!("x_processed_stream_from_node  to: {}:{}", node.host, node.port_raw);
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
        let frames = InMemoryFrameAsyncReadStream::new(netin, 1024 * 128);
        type ITEM = ChannelEvents;
        let stream = EventsFromFrames::<_, ITEM>::new(frames);
        streams.push(Box::pin(stream) as _);
    }
    Ok(streams)
}
