/*!
Delivers event data.

Delivers event data (not yet time-binned) from local storage and provides client functions
to request such data from nodes.
*/

use crate::eventchunker::EventFull;
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::raw::eventsfromframes::EventsFromFrames;
use err::Error;
use futures_core::Stream;
use items::frame::{make_frame, make_term_frame};
use items::{EventsNodeProcessor, FrameType, RangeCompletableItem, Sitemty, StreamItem};
use netpod::query::RawEventsQuery;
use netpod::{EventQueryJsonStringFrame, Node, PerfOpts};
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub async fn x_processed_stream_from_node<ENP>(
    query: RawEventsQuery,
    perf_opts: PerfOpts,
    node: Node,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<<ENP as EventsNodeProcessor>::Output>> + Send>>, Error>
where
    ENP: EventsNodeProcessor,
    <ENP as EventsNodeProcessor>::Output: Unpin + 'static,
    Result<StreamItem<RangeCompletableItem<<ENP as EventsNodeProcessor>::Output>>, err::Error>: FrameType,
{
    netpod::log::info!("x_processed_stream_from_node  to: {}:{}", node.host, node.port_raw);
    let net = TcpStream::connect(format!("{}:{}", node.host, node.port_raw)).await?;
    let qjs = serde_json::to_string(&query)?;
    let (netin, mut netout) = net.into_split();
    let buf = make_frame(&EventQueryJsonStringFrame(qjs))?;
    netout.write_all(&buf).await?;
    let buf = make_term_frame();
    netout.write_all(&buf).await?;
    netout.flush().await?;
    netout.forget();
    let frames = InMemoryFrameAsyncReadStream::new(netin, perf_opts.inmem_bufcap);
    let items = EventsFromFrames::new(frames);
    Ok(Box::pin(items))
}

pub async fn x_processed_event_blobs_stream_from_node(
    query: RawEventsQuery,
    perf_opts: PerfOpts,
    node: Node,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    netpod::log::info!(
        "x_processed_event_blobs_stream_from_node  to: {}:{}",
        node.host,
        node.port_raw
    );
    let net = TcpStream::connect(format!("{}:{}", node.host, node.port_raw)).await?;
    let qjs = serde_json::to_string(&query)?;
    let (netin, mut netout) = net.into_split();
    let buf = make_frame(&EventQueryJsonStringFrame(qjs))?;
    netout.write_all(&buf).await?;
    let buf = make_term_frame();
    netout.write_all(&buf).await?;
    netout.flush().await?;
    netout.forget();
    let frames = InMemoryFrameAsyncReadStream::new(netin, perf_opts.inmem_bufcap);
    let items = EventsFromFrames::new(frames);
    Ok(Box::pin(items))
}
