/*!
Delivers event data.

Delivers event data (not yet time-binned) from local storage and provides client functions
to request such data from nodes.
*/

use crate::agg::{MinMaxAvgScalarBinBatch, MinMaxAvgScalarEventBatch};
use bytes::{Bytes, BytesMut};
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, StreamExt};
use netpod::{AggKind, Channel, NanoRange, Node, NodeConfig};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

/**
Query parameters to request (optionally) X-processed, but not T-processed events.
*/
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventsQuery {
    pub channel: Channel,
    pub range: NanoRange,
    pub agg_kind: AggKind,
}

pub async fn x_processed_stream_from_node(
    query: Arc<EventsQuery>,
    node: Arc<Node>,
) -> Result<Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarEventBatch, Error>> + Send>>, Error> {
    let mut net = TcpStream::connect(format!("{}:{}", node.host, node.port_raw)).await?;
    let qjs = serde_json::to_vec(query.as_ref())?;
    net.write_u32_le(qjs.len() as u32).await?;
    net.write_all(&qjs).await?;
    net.flush().await?;
    let s2 = MinMaxAvgScalarEventBatchStreamFromTcp { inp: net };
    let s3: Pin<Box<dyn Stream<Item = Result<_, Error>> + Send>> = Box::pin(s2);
    Ok(s3)
}

pub struct MinMaxAvgScalarEventBatchStreamFromTcp {
    inp: TcpStream,
}

impl Stream for MinMaxAvgScalarEventBatchStreamFromTcp {
    type Item = Result<MinMaxAvgScalarEventBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            // TODO make capacity configurable.
            // TODO reuse buffer if not full.
            let mut buf = BytesMut::with_capacity(1024 * 2);
            let mut buf2 = ReadBuf::new(buf.as_mut());
            let j = &mut self.inp;
            pin_mut!(j);
            break match AsyncRead::poll_read(j, cx, &mut buf2) {
                Ready(Ok(_)) => {
                    if buf.len() == 0 {
                        Ready(None)
                    } else {
                        error!("got input from remote  {} bytes", buf.len());
                        Ready(Some(Ok(err::todoval())))
                    }
                }
                Ready(Err(e)) => Ready(Some(Err(e.into()))),
                Pending => Pending,
            };
        }
    }
}

/**
Interprets a byte stream as length-delimited frames.

Emits each frame as a single item. Therefore, each item must fit easily into memory.
*/
pub struct InMemoryFrameAsyncReadStream<T> {
    inp: T,
    buf: Option<BytesMut>,
}

impl<T> InMemoryFrameAsyncReadStream<T> {
    fn new(inp: T) -> Self {
        let mut t = BytesMut::with_capacity(1024);

        TODO
        // how to prepare the buffer so that ReadBuf has space to write?



        Self {
            inp,
            // TODO make start cap adjustable
            buf: Some(BytesMut::with_capacity(1024)),
        }
    }
}

impl<T> Stream for InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            let mut buf0 = self.buf.take().unwrap();
            let mut buf2 = ReadBuf::new(buf0.as_mut());
            assert!(buf2.capacity() > 0);
            assert!(buf2.remaining() > 0);
            let r1 = buf2.remaining();
            let j = &mut self.inp;
            pin_mut!(j);
            break match AsyncRead::poll_read(j, cx, &mut buf2) {
                Ready(Ok(())) => {
                    if buf2.remaining() == r1 {
                        // TODO re-init self.buf ?
                        // TODO end of input.
                        err::todoval()
                    } else {
                        // TODO re-init self.buf ?
                        // TODO how to reflect the write position in the underlying BytesMut???
                        err::todoval()
                    }
                }
                Ready(Err(e)) => Ready(Some(Err(e.into()))),
                Pending => Pending,
            };
        }
    }
}

// TODO build a stream from disk data to batched event data.
#[allow(dead_code)]
async fn local_unpacked_test() {
    let query = err::todoval();
    let node = err::todoval();
    // TODO open and parse the channel config.
    // TODO find the matching config entry. (bonus: fuse consecutive compatible entries)
    use crate::agg::IntoDim1F32Stream;
    let _stream = crate::EventBlobsComplete::new(&query, query.channel_config.clone(), node).into_dim_1_f32_stream();
}

/**
Can be serialized as a length-delimited frame.
*/
pub trait Frameable {}

pub async fn raw_service(node_config: Arc<NodeConfig>) -> Result<(), Error> {
    let addr = format!("0.0.0.0:{}", node_config.node.port_raw);
    let lis = tokio::net::TcpListener::bind(addr).await?;
    loop {
        match lis.accept().await {
            Ok((stream, addr)) => {
                taskrun::spawn(raw_conn_handler(stream, addr));
            }
            Err(e) => Err(e)?,
        }
    }
}

async fn raw_conn_handler(stream: TcpStream, addr: SocketAddr) -> Result<(), Error> {
    info!("raw_conn_handler   SPAWNED   for {:?}", addr);
    let (netin, mut netout) = stream.into_split();
    let mut h = InMemoryFrameAsyncReadStream::new(netin);
    while let Some(k) = h.next().await {
        warn!("raw_conn_handler  FRAME RECV  {}", k.is_ok());
    }
    netout.write_i32_le(123).await?;
    netout.flush().await?;
    netout.forget();
    Ok(())
}
