/*!
Delivers event data.

Delivers event data (not yet time-binned) from local storage and provides client functions
to request such data from nodes.
*/

use crate::agg::{IntoBinnedXBins1, IntoDim1F32Stream, MinMaxAvgScalarEventBatch};
use crate::cache::BinnedBytesForHttpStreamFrame;
use bytes::{BufMut, Bytes, BytesMut};
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, StreamExt};
use netpod::timeunits::DAY;
use netpod::{AggKind, Channel, NanoRange, Node, NodeConfig, ScalarType, Shape};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tracing::Instrument;
#[allow(unused_imports)]
use tracing::{debug, error, info, span, trace, warn, Level};

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

pub async fn x_processed_stream_from_node(
    query: Arc<EventsQuery>,
    node: Arc<Node>,
) -> Result<Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarEventBatch, Error>> + Send>>, Error> {
    let net = TcpStream::connect(format!("{}:{}", node.host, node.port_raw)).await?;
    let qjs = serde_json::to_string(query.as_ref())?;
    let (netin, mut netout) = net.into_split();
    let buf = make_frame(&EventQueryJsonStringFrame(qjs))?;
    netout.write_all(&buf).await?;
    let buf = make_term_frame();
    netout.write_all(&buf).await?;
    netout.flush().await?;
    netout.forget();
    let frames = InMemoryFrameAsyncReadStream::new(netin);
    let s2 = MinMaxAvgScalarEventBatchStreamFromFrames::new(frames);
    let s3: Pin<Box<dyn Stream<Item = Result<_, Error>> + Send>> = Box::pin(s2);
    Ok(s3)
}

pub struct MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    inp: InMemoryFrameAsyncReadStream<T>,
}

impl<T> MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    pub fn new(inp: InMemoryFrameAsyncReadStream<T>) -> Self {
        Self { inp }
    }
}

impl<T> Stream for MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<MinMaxAvgScalarEventBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let j = &mut self.inp;
            pin_mut!(j);
            break match j.poll_next(cx) {
                Ready(Some(Ok(frame))) => {
                    type ExpectedType = RawConnOut;
                    info!(
                        "MinMaxAvgScalarEventBatchStreamFromFrames  got full frame buf  {}",
                        frame.buf().len()
                    );
                    assert!(frame.tyid() == <ExpectedType as FrameType>::FRAME_TYPE_ID);
                    match bincode::deserialize::<ExpectedType>(frame.buf()) {
                        Ok(item) => match item {
                            Ok(item) => Ready(Some(Ok(item))),
                            Err(e) => Ready(Some(Err(e))),
                        },
                        Err(e) => {
                            trace!(
                                "MinMaxAvgScalarEventBatchStreamFromFrames  ~~~~~~~~   ERROR on frame payload {}",
                                frame.buf().len(),
                            );
                            Ready(Some(Err(e.into())))
                        }
                    }
                }
                Ready(Some(Err(e))) => Ready(Some(Err(e))),
                Ready(None) => Ready(None),
                Pending => Pending,
            };
        }
    }
}

pub const INMEM_FRAME_HEAD: usize = 16;
pub const INMEM_FRAME_MAGIC: u32 = 0xc6c3b73d;

/**
Interprets a byte stream as length-delimited frames.

Emits each frame as a single item. Therefore, each item must fit easily into memory.
*/
pub struct InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    inp: T,
    buf: BytesMut,
    bufcap: usize,
    wp: usize,
    tryparse: bool,
    errored: bool,
    completed: bool,
    inp_bytes_consumed: u64,
}

impl<T> InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    pub fn new(inp: T) -> Self {
        // TODO make capacity adjustable.
        let bufcap = 512;
        let mut t = Self {
            inp,
            buf: BytesMut::new(),
            bufcap: bufcap,
            wp: 0,
            tryparse: false,
            errored: false,
            completed: false,
            inp_bytes_consumed: 0,
        };
        t.buf = t.empty_buf();
        t
    }

    fn empty_buf(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.bufcap);
        buf.resize(buf.capacity(), 0);
        buf
    }

    fn poll_upstream(&mut self, cx: &mut Context) -> Poll<Result<usize, Error>> {
        if self.wp > 0 {
            // TODO copy only if we gain capacity in the current buffer.
            // Also copy if the bufcap got increased: how to find out with BytesMut?  Question about how capacity is defined exactly...
            // Avoid copies after e.g. after a previous Pending.
            let mut bnew = self.empty_buf();
            assert!(self.buf.len() >= self.wp);
            assert!(bnew.capacity() >= self.wp);
            info!(
                "InMemoryFrameAsyncReadStream  re-use {} bytes from previous i/o",
                self.wp
            );
            bnew.put(&self.buf[..self.wp]);
            self.buf = bnew;
        }
        info!(
            "..............   PREPARE READ FROM  wp {}  self.buf.len() {}",
            self.wp,
            self.buf.len(),
        );
        let gg = self.buf.len() - self.wp;
        let mut buf2 = ReadBuf::new(&mut self.buf[self.wp..]);
        assert!(gg > 0);
        assert!(buf2.remaining() == gg);
        assert!(buf2.capacity() == gg);
        assert!(buf2.filled().len() == 0);
        let j = &mut self.inp;
        pin_mut!(j);
        use Poll::*;
        match AsyncRead::poll_read(j, cx, &mut buf2) {
            Ready(Ok(_)) => {
                let n1 = buf2.filled().len();
                info!("InMemoryFrameAsyncReadStream  READ {} FROM UPSTREAM", n1);
                Ready(Ok(n1))
            }
            Ready(Err(e)) => Ready(Err(e.into())),
            Pending => Pending,
        }
    }

    fn tryparse(
        &mut self,
        buf: BytesMut,
        wp: usize,
    ) -> (Option<Option<Result<InMemoryFrame, Error>>>, BytesMut, usize) {
        info!(
            "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++   tryparse with  buf.len() {}  wp {}",
            buf.len(),
            wp
        );
        const HEAD: usize = INMEM_FRAME_HEAD;
        let mut buf = buf;
        let nb = wp;
        if nb >= HEAD {
            let magic = u32::from_le_bytes(*arrayref::array_ref![buf, 0, 4]);
            let encid = u32::from_le_bytes(*arrayref::array_ref![buf, 4, 4]);
            let tyid = u32::from_le_bytes(*arrayref::array_ref![buf, 8, 4]);
            let len = u32::from_le_bytes(*arrayref::array_ref![buf, 12, 4]);
            if magic != INMEM_FRAME_MAGIC {
                error!("InMemoryFrameAsyncReadStream  tryparse  incorrect magic: {}", magic);
                return (
                    Some(Some(Err(Error::with_msg(format!(
                        "InMemoryFrameAsyncReadStream  tryparse  incorrect magic: {}",
                        magic
                    ))))),
                    buf,
                    wp,
                );
            }
            info!("\n\ntryparse len {}\n\n", len);
            if len == 0 {
                if nb != HEAD {
                    return (
                        Some(Some(Err(Error::with_msg(format!(
                            "InMemoryFrameAsyncReadStream  tryparse  unexpected amount left {}",
                            nb
                        ))))),
                        buf,
                        wp,
                    );
                }
                (Some(None), buf, wp)
            } else {
                if len > 1024 * 32 {
                    warn!("InMemoryFrameAsyncReadStream  big len received  {}", len);
                }
                if len > 1024 * 1024 * 2 {
                    error!("InMemoryFrameAsyncReadStream  too long len {}", len);
                    return (
                        Some(Some(Err(Error::with_msg(format!(
                            "InMemoryFrameAsyncReadStream  tryparse  huge buffer  len {}  self.inp_bytes_consumed {}",
                            len, self.inp_bytes_consumed
                        ))))),
                        buf,
                        wp,
                    );
                }
                if len == 0 && len > 1024 * 512 {
                    return (
                        Some(Some(Err(Error::with_msg(format!(
                            "InMemoryFrameAsyncReadStream  tryparse  len {}  self.inp_bytes_consumed {}",
                            len, self.inp_bytes_consumed
                        ))))),
                        buf,
                        wp,
                    );
                }
                let nl = len as usize + HEAD;
                if self.bufcap < nl {
                    // TODO count cases in production
                    self.bufcap += 2 * nl;
                }
                if nb >= nl {
                    use bytes::Buf;
                    let mut buf3 = buf.split_to(nl);
                    buf3.advance(HEAD);
                    self.inp_bytes_consumed += nl as u64;
                    let ret = InMemoryFrame {
                        len,
                        tyid,
                        encid,
                        buf: buf3.freeze(),
                    };
                    (Some(Some(Ok(ret))), buf, wp - nl)
                } else {
                    (None, buf, wp)
                }
            }
        } else {
            (None, buf, wp)
        }
    }
}

pub struct InMemoryFrame {
    encid: u32,
    tyid: u32,
    len: u32,
    buf: Bytes,
}

impl InMemoryFrame {
    pub fn encid(&self) -> u32 {
        self.encid
    }
    pub fn tyid(&self) -> u32 {
        self.tyid
    }
    pub fn len(&self) -> u32 {
        self.len
    }
    pub fn buf(&self) -> &Bytes {
        &self.buf
    }
}

impl<T> Stream for InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<InMemoryFrame, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        info!("InMemoryFrameAsyncReadStream  poll_next");
        use Poll::*;
        assert!(!self.completed);
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        'outer: loop {
            if self.tryparse {
                let r = {
                    let buf = std::mem::replace(&mut self.buf, BytesMut::new());
                    let wp = self.wp;
                    let (r, buf, wp) = self.tryparse(buf, wp);
                    self.buf = buf;
                    self.wp = wp;
                    r
                };
                break match r {
                    None => {
                        self.tryparse = false;
                        continue 'outer;
                    }
                    Some(None) => {
                        self.tryparse = false;
                        self.completed = true;
                        Ready(None)
                    }
                    Some(Some(Ok(k))) => Ready(Some(Ok(k))),
                    Some(Some(Err(e))) => {
                        self.tryparse = false;
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                };
            } else {
                let r = self.poll_upstream(cx);
                break match r {
                    Ready(Ok(n1)) => {
                        info!("poll_upstream  GIVES  Ready  {}", n1);
                        self.wp += n1;
                        if n1 == 0 {
                            let n2 = self.buf.len();
                            if n2 != 0 {
                                warn!(
                                    "InMemoryFrameAsyncReadStream  n2 != 0  n2 {}  consumed {}  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~",
                                    n2, self.inp_bytes_consumed
                                );
                            }
                            self.completed = true;
                            Ready(None)
                        } else {
                            self.tryparse = true;
                            continue 'outer;
                        }
                    }
                    Ready(Err(e)) => {
                        info!("poll_upstream  GIVES  Error");
                        self.errored = true;
                        Ready(Some(Err(e.into())))
                    }
                    Pending => {
                        info!("poll_upstream  GIVES  Pending");
                        Pending
                    }
                };
            }
        }
    }
}

pub trait FrameType {
    const FRAME_TYPE_ID: u32;
}

impl FrameType for BinnedBytesForHttpStreamFrame {
    const FRAME_TYPE_ID: u32 = 0x02;
}

impl FrameType for EventQueryJsonStringFrame {
    const FRAME_TYPE_ID: u32 = 0x03;
}

impl FrameType for RawConnOut {
    const FRAME_TYPE_ID: u32 = 0x04;
}

pub fn make_frame<FT>(item: &FT) -> Result<BytesMut, Error>
where
    FT: FrameType + Serialize,
{
    match bincode::serialize(item) {
        Ok(enc) => {
            if enc.len() > u32::MAX as usize {
                return Err(Error::with_msg(format!("too long payload {}", enc.len())));
            }
            let encid = 0x12121212;
            let mut buf = BytesMut::with_capacity(enc.len() + INMEM_FRAME_HEAD);
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(encid);
            buf.put_u32_le(FT::FRAME_TYPE_ID);
            buf.put_u32_le(enc.len() as u32);
            buf.put(enc.as_ref());
            Ok(buf)
        }
        Err(e) => Err(e)?,
    }
}

pub fn make_term_frame() -> BytesMut {
    let encid = 0x12121313;
    let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD);
    buf.put_u32_le(INMEM_FRAME_MAGIC);
    buf.put_u32_le(encid);
    buf.put_u32_le(0x01);
    buf.put_u32_le(0);
    buf
}

pub async fn raw_service(node_config: Arc<NodeConfig>) -> Result<(), Error> {
    let addr = format!("{}:{}", node_config.node.listen, node_config.node.port_raw);
    let lis = tokio::net::TcpListener::bind(addr).await?;
    loop {
        match lis.accept().await {
            Ok((stream, addr)) => {
                taskrun::spawn(raw_conn_handler(stream, addr, node_config.clone()));
            }
            Err(e) => Err(e)?,
        }
    }
}

async fn raw_conn_handler(stream: TcpStream, addr: SocketAddr, node_config: Arc<NodeConfig>) -> Result<(), Error> {
    //use tracing_futures::Instrument;
    let span1 = span!(Level::INFO, "raw::raw_conn_handler");
    raw_conn_handler_inner(stream, addr, node_config)
        .instrument(span1)
        .await
}

type RawConnOut = Result<MinMaxAvgScalarEventBatch, Error>;

async fn raw_conn_handler_inner(
    stream: TcpStream,
    addr: SocketAddr,
    node_config: Arc<NodeConfig>,
) -> Result<(), Error> {
    match raw_conn_handler_inner_try(stream, addr, node_config).await {
        Ok(_) => (),
        Err(mut ce) => {
            error!("raw_conn_handler_inner  CAUGHT ERROR AND TRY TO SEND OVER TCP");
            let buf = make_frame::<RawConnOut>(&Err(ce.err))?;
            match ce.netout.write(&buf).await {
                Ok(_) => (),
                Err(e) => return Err(e)?,
            }
        }
    }
    Ok(())
}

struct ConnErr {
    err: Error,
    netout: OwnedWriteHalf,
}

impl<E: Into<Error>> From<(E, OwnedWriteHalf)> for ConnErr {
    fn from((err, netout): (E, OwnedWriteHalf)) -> Self {
        Self {
            err: err.into(),
            netout,
        }
    }
}

async fn raw_conn_handler_inner_try(
    stream: TcpStream,
    addr: SocketAddr,
    node_config: Arc<NodeConfig>,
) -> Result<(), ConnErr> {
    info!("raw_conn_handler   SPAWNED   for {:?}", addr);
    let (netin, mut netout) = stream.into_split();
    let mut h = InMemoryFrameAsyncReadStream::new(netin);
    let mut frames = vec![];
    while let Some(k) = h
        .next()
        .instrument(span!(Level::INFO, "raw_conn_handler  INPUT STREAM READ"))
        .await
    {
        match k {
            Ok(k) => {
                info!(". . . . . . . . . . . . . . . . . . . . . . . . . .   raw_conn_handler  FRAME RECV");
                frames.push(k);
            }
            Err(e) => {
                return Err((e, netout))?;
            }
        }
    }
    if frames.len() != 1 {
        error!("expect a command frame");
        return Err((Error::with_msg("expect a command frame"), netout))?;
    }
    let qitem = match bincode::deserialize::<EventQueryJsonStringFrame>(frames[0].buf()) {
        Ok(k) => k,
        Err(e) => return Err((e, netout))?,
    };
    trace!("json: {}", qitem.0);
    let res: Result<EventsQuery, _> = serde_json::from_str(&qitem.0);
    let evq = match res {
        Ok(k) => k,
        Err(e) => {
            error!("can not parse json {:?}", e);
            return Err((Error::with_msg("can not parse request json"), netout))?;
        }
    };
    error!(
        "TODO decide on response content based on the parsed json query\n{:?}",
        evq
    );
    let query = netpod::AggQuerySingleChannel {
        channel_config: netpod::ChannelConfig {
            channel: netpod::Channel {
                backend: "test1".into(),
                name: "wave1".into(),
            },
            keyspace: 3,
            time_bin_size: DAY,
            shape: Shape::Wave(1024),
            scalar_type: ScalarType::F64,
            big_endian: true,
            array: true,
            compression: true,
        },
        // TODO use a NanoRange and search for matching files
        timebin: 0,
        tb_file_count: 1,
        // TODO use the requested buffer size
        buffer_size: 1024 * 4,
    };
    let mut s1 = crate::EventBlobsComplete::new(&query, query.channel_config.clone(), node_config.node.clone())
        .into_dim_1_f32_stream()
        .take(10)
        .into_binned_x_bins_1();
    while let Some(item) = s1.next().await {
        if let Ok(k) = &item {
            info!("????????????????   emit item  ts0: {:?}", k.tss.first());
        }
        match make_frame::<RawConnOut>(&item) {
            Ok(buf) => match netout.write(&buf).await {
                Ok(_) => {}
                Err(e) => return Err((e, netout))?,
            },
            Err(e) => {
                return Err((e, netout))?;
            }
        }
    }
    if false {
        // Manual test batch.
        let mut batch = MinMaxAvgScalarEventBatch::empty();
        batch.tss.push(42);
        batch.tss.push(43);
        batch.mins.push(7.1);
        batch.mins.push(7.2);
        batch.maxs.push(8.3);
        batch.maxs.push(8.4);
        batch.avgs.push(9.5);
        batch.avgs.push(9.6);
        let mut s1 = futures_util::stream::iter(vec![batch]).map(Result::Ok);
        while let Some(item) = s1.next().await {
            match make_frame::<RawConnOut>(&item) {
                Ok(buf) => match netout.write(&buf).await {
                    Ok(_) => {}
                    Err(e) => return Err((e, netout))?,
                },
                Err(e) => {
                    return Err((e, netout))?;
                }
            }
        }
    }
    let buf = make_term_frame();
    match netout.write(&buf).await {
        Ok(_) => (),
        Err(e) => return Err((e, netout))?,
    }
    match netout.flush().await {
        Ok(_) => (),
        Err(e) => return Err((e, netout))?,
    }
    Ok(())
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
