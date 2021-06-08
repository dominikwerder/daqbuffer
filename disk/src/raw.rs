/*!
Delivers event data.

Delivers event data (not yet time-binned) from local storage and provides client functions
to request such data from nodes.
*/

use crate::agg::streams::StreamItem;
use crate::binned::{EventsNodeProcessor, RangeCompletableItem, StreamKind};
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::{make_frame, make_term_frame, FrameType};
use crate::raw::eventsfromframes::EventsFromFrames;
use crate::Sitemty;
use err::Error;
use futures_core::Stream;
use netpod::{AggKind, Channel, NanoRange, Node, PerfOpts};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
#[allow(unused_imports)]
use tracing::{debug, error, info, span, trace, warn, Level};

pub mod conn;
pub mod eventsfromframes;

/**
Query parameters to request (optionally) X-processed, but not T-processed events.
*/
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventsQuery {
    pub channel: Channel,
    pub range: NanoRange,
    pub agg_kind: AggKind,
}

#[derive(Serialize, Deserialize)]
pub struct EventQueryJsonStringFrame(String);

// TODO remove after refactor.
pub async fn x_processed_stream_from_node<SK>(
    query: EventsQuery,
    perf_opts: PerfOpts,
    node: Node,
    stream_kind: SK,
) -> Result<
    Pin<
        Box<
            dyn Stream<Item = Result<StreamItem<RangeCompletableItem<<SK as StreamKind>::XBinnedEvents>>, Error>>
                + Send,
        >,
    >,
    Error,
>
where
    SK: StreamKind,
    Result<StreamItem<RangeCompletableItem<<SK as StreamKind>::XBinnedEvents>>, err::Error>: FrameType,
{
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
    //let items = EventsFromFrames::new(frames);
    //Ok(Box::pin(items))
    Ok(err::todoval())
}

pub async fn x_processed_stream_from_node2<ENP>(
    query: EventsQuery,
    perf_opts: PerfOpts,
    node: Node,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<<ENP as EventsNodeProcessor>::Output>> + Send>>, Error>
where
    ENP: EventsNodeProcessor,
    <ENP as EventsNodeProcessor>::Output: Unpin + 'static,
    Result<StreamItem<RangeCompletableItem<<ENP as EventsNodeProcessor>::Output>>, err::Error>: FrameType,
{
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

pub fn crchex<T>(t: T) -> String
where
    T: AsRef<[u8]>,
{
    let mut h = crc32fast::Hasher::new();
    h.update(t.as_ref());
    let crc = h.finalize();
    format!("{:08x}", crc)
}
