/*!
Delivers event data.

Delivers event data (not yet time-binned) from local storage and provides client functions
to request such data from nodes.
*/

use crate::agg::MinMaxAvgScalarEventBatch;
use bytes::{BufMut, Bytes, BytesMut};
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
    debug!("x_processed_stream_from_node   ENTER");
    let mut net = TcpStream::connect(format!("{}:{}", node.host, node.port_raw)).await?;
    debug!("x_processed_stream_from_node   CONNECTED");
    let qjs = serde_json::to_vec(query.as_ref())?;
    debug!("x_processed_stream_from_node   qjs len {}", qjs.len());
    net.write_u32_le(qjs.len() as u32).await?;
    net.write_all(&qjs).await?;
    debug!("x_processed_stream_from_node   WRITTEN");
    net.flush().await?;
    let s2 = MinMaxAvgScalarEventBatchStreamFromTcp { inp: net };
    debug!("x_processed_stream_from_node   HAVE STREAM INSTANCE");
    let s3: Pin<Box<dyn Stream<Item = Result<_, Error>> + Send>> = Box::pin(s2);
    debug!("x_processed_stream_from_node   RETURN");
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
    buf: BytesMut,
    wp: usize,
}

impl<T> InMemoryFrameAsyncReadStream<T> {
    fn new(inp: T) -> Self {
        // TODO make start cap adjustable
        let mut buf = BytesMut::with_capacity(1024);
        buf.resize(buf.capacity(), 0);
        Self { inp, buf, wp: 0 }
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
            info!("PREPARE BUFFER FOR READING");
            let mut buf0 = std::mem::replace(&mut self.buf, BytesMut::new());
            if buf0.as_mut().len() != buf0.capacity() {
                error!("-------   {}  {}", buf0.as_mut().len(), buf0.capacity());
                panic!();
            }
            let mut buf2 = ReadBuf::new(buf0.as_mut()[self.wp..].as_mut());
            assert!(buf2.filled().len() == 0);
            assert!(buf2.capacity() > 0);
            assert!(buf2.remaining() > 0);
            let r1 = buf2.remaining();
            let j = &mut self.inp;
            pin_mut!(j);
            break match AsyncRead::poll_read(j, cx, &mut buf2) {
                Ready(Ok(())) => {
                    let r2 = buf2.remaining();
                    if r2 == r1 {
                        info!("InMemoryFrameAsyncReadStream  END OF INPUT");
                        if self.wp != 0 {
                            error!("self.wp != 0   {}", self.wp);
                        }
                        assert!(self.wp == 0);
                        Ready(None)
                    } else {
                        let n = buf2.filled().len();
                        self.wp += n;
                        info!("InMemoryFrameAsyncReadStream  read  n {}  wp {}", n, self.wp);
                        if self.wp >= 4 {
                            let len = u32::from_le_bytes(*arrayref::array_ref![buf0.as_mut(), 0, 4]);
                            info!("InMemoryFrameAsyncReadStream  len: {}", len);
                            assert!(len > 0 && len < 1024 * 512);
                            let nl = len as usize + 4;
                            if buf0.capacity() < nl {
                                buf0.resize(nl, 0);
                            } else {
                                // nothing to do
                            }
                            if self.wp >= nl {
                                info!("InMemoryFrameAsyncReadStream  Have whole frame");
                                let mut buf3 = BytesMut::with_capacity(buf0.capacity());
                                // TODO make stats of copied bytes and warn if ratio is too bad.
                                buf3.put(buf0.as_ref()[nl..self.wp].as_ref());
                                buf3.resize(buf3.capacity(), 0);
                                self.wp = self.wp - nl;
                                self.buf = buf3;
                                use bytes::Buf;
                                buf0.truncate(nl);
                                buf0.advance(4);
                                Ready(Some(Ok(buf0.freeze())))
                            } else {
                                self.buf = buf0;
                                continue 'outer;
                            }
                        } else {
                            info!("InMemoryFrameAsyncReadStream  not yet enough for len");
                            self.buf = buf0;
                            continue 'outer;
                        }
                    }
                }
                Ready(Err(e)) => Ready(Some(Err(e.into()))),
                Pending => {
                    self.buf = buf0;
                    Pending
                }
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
    let addr = format!("{}:{}", node_config.node.listen, node_config.node.port_raw);
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
