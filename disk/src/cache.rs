use crate::agg::{IntoBinnedT, MinMaxAvgScalarBinBatch, MinMaxAvgScalarEventBatch};
use crate::merge::MergedMinMaxAvgScalarStream;
use crate::raw::{EventsQuery, InMemoryFrameAsyncReadStream};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, FutureExt, StreamExt, TryStreamExt};
use hyper::Response;
use netpod::{
    AggKind, BinSpecDimT, Channel, Cluster, NanoRange, NodeConfig, PreBinnedPatchCoord, PreBinnedPatchIterator,
    PreBinnedPatchRange, RetStreamExt, ToNanos,
};
use serde::{Deserialize, Serialize};
use std::future::{ready, Future};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tiny_keccak::Hasher;
use tokio::io::{AsyncRead, ReadBuf};
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Query {
    range: NanoRange,
    count: u64,
    agg_kind: AggKind,
    channel: Channel,
}

impl Query {
    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let beg_date = params
            .get("beg_date")
            .ok_or_else(|| Error::with_msg("missing beg_date"))?;
        let end_date = params
            .get("end_date")
            .ok_or_else(|| Error::with_msg("missing end_date"))?;
        let ret = Query {
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            count: params
                .get("bin_count")
                .ok_or_else(|| Error::with_msg("missing beg_date"))?
                .parse()
                .unwrap(),
            agg_kind: AggKind::DimXBins1,
            channel: Channel {
                backend: params.get("channel_backend").unwrap().into(),
                name: params.get("channel_name").unwrap().into(),
            },
        };
        Ok(ret)
    }
}

pub async fn binned_bytes_for_http(
    node_config: Arc<NodeConfig>,
    query: &Query,
) -> Result<BinnedBytesForHttpStream, Error> {
    let agg_kind = AggKind::DimXBins1;

    let channel_config = super::channelconfig::read_local_config(&query.channel, node_config.clone()).await?;
    let entry;
    {
        let mut ixs = vec![];
        for i1 in 0..channel_config.entries.len() {
            let e1 = &channel_config.entries[i1];
            if i1 + 1 < channel_config.entries.len() {
                let e2 = &channel_config.entries[i1 + 1];
                if e1.ts < query.range.end && e2.ts >= query.range.beg {
                    ixs.push(i1);
                }
            } else {
                if e1.ts < query.range.end {
                    ixs.push(i1);
                }
            }
        }
        if ixs.len() == 0 {
            return Err(Error::with_msg(format!("no config entries found")));
        } else if ixs.len() > 1 {
            return Err(Error::with_msg(format!("too many config entries found: {}", ixs.len())));
        }
        entry = &channel_config.entries[ixs[0]];
    }

    info!("found config entry {:?}", entry);

    let grid = PreBinnedPatchRange::covering_range(query.range.clone(), query.count);
    match grid {
        Some(spec) => {
            info!("GOT  PreBinnedPatchGridSpec:    {:?}", spec);
            warn!("Pass here to BinnedStream what kind of Agg, range, ...");
            let s1 = BinnedStream::new(
                PreBinnedPatchIterator::from_range(spec),
                query.channel.clone(),
                agg_kind,
                node_config.clone(),
            );
            let ret = BinnedBytesForHttpStream::new(s1);
            Ok(ret)
        }
        None => {
            // Merge raw data
            error!("binned_bytes_for_http   TODO   merge raw data");
            todo!()
        }
    }
}

pub type BinnedBytesForHttpStreamFrame = <BinnedStream as Stream>::Item;

pub struct BinnedBytesForHttpStream {
    inp: BinnedStream,
    errored: bool,
    completed: bool,
}

impl BinnedBytesForHttpStream {
    pub fn new(inp: BinnedStream) -> Self {
        Self {
            inp,
            errored: false,
            completed: false,
        }
    }
}

impl Stream for BinnedBytesForHttpStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("BinnedBytesForHttpStream  poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => {
                match bincode::serialize::<BinnedBytesForHttpStreamFrame>(&item) {
                    Ok(enc) => {
                        // TODO optimize this...
                        const HEAD: usize = super::raw::INMEM_FRAME_HEAD;
                        let mut buf = BytesMut::with_capacity(enc.len() + HEAD);
                        buf.put_u32_le(super::raw::INMEM_FRAME_MAGIC);
                        buf.put_u32_le(enc.len() as u32);
                        buf.put_u32_le(0);
                        buf.put(enc.as_ref());
                        Ready(Some(Ok(buf.freeze())))
                    }
                    Err(e) => {
                        self.errored = true;
                        Ready(Some(Err(e.into())))
                    }
                }
            }
            Ready(None) => {
                self.completed = true;
                Ready(None)
            }
            Pending => Pending,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PreBinnedQuery {
    pub patch: PreBinnedPatchCoord,
    agg_kind: AggKind,
    channel: Channel,
}

impl PreBinnedQuery {
    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let ret = PreBinnedQuery {
            patch: PreBinnedPatchCoord::from_query_params(&params),
            agg_kind: AggKind::DimXBins1,
            channel: Channel {
                backend: params.get("channel_backend").unwrap().into(),
                name: params.get("channel_name").unwrap().into(),
            },
        };
        info!("PreBinnedQuery::from_request  {:?}", ret);
        Ok(ret)
    }
}

// NOTE  This answers a request for a single valid pre-binned patch.
// A user must first make sure that the grid spec is valid, and that this node is responsible for it.
// Otherwise it is an error.
pub fn pre_binned_bytes_for_http(
    node_config: Arc<NodeConfig>,
    query: &PreBinnedQuery,
) -> Result<PreBinnedValueByteStream, Error> {
    info!("pre_binned_bytes_for_http  {:?}  {:?}", query, node_config.node);
    let ret = PreBinnedValueByteStream::new(
        query.patch.clone(),
        query.channel.clone(),
        query.agg_kind.clone(),
        node_config,
    );
    Ok(ret)
}

pub struct PreBinnedValueByteStream {
    inp: PreBinnedValueStream,
    errored: bool,
    completed: bool,
}

impl PreBinnedValueByteStream {
    pub fn new(patch: PreBinnedPatchCoord, channel: Channel, agg_kind: AggKind, node_config: Arc<NodeConfig>) -> Self {
        warn!("PreBinnedValueByteStream");
        Self {
            inp: PreBinnedValueStream::new(patch, channel, agg_kind, node_config),
            errored: false,
            completed: false,
        }
    }
}

impl Stream for PreBinnedValueByteStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => {
                // TODO optimize this
                const HEAD: usize = super::raw::INMEM_FRAME_HEAD;
                match bincode::serialize::<PreBinnedHttpFrame>(&item) {
                    Ok(enc) => {
                        let mut buf = BytesMut::with_capacity(enc.len() + HEAD);
                        buf.put_u32_le(super::raw::INMEM_FRAME_MAGIC);
                        buf.put_u32_le(enc.len() as u32);
                        buf.put_u32_le(0);
                        buf.put(enc.as_ref());
                        Ready(Some(Ok(buf.freeze())))
                    }
                    Err(e) => {
                        self.errored = true;
                        Ready(Some(Err(e.into())))
                    }
                }
            }
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub struct PreBinnedValueStream {
    patch_coord: PreBinnedPatchCoord,
    channel: Channel,
    agg_kind: AggKind,
    node_config: Arc<NodeConfig>,
    open_check_local_file: Option<Pin<Box<dyn Future<Output = Result<tokio::fs::File, std::io::Error>> + Send>>>,
    fut2: Option<Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarBinBatch, Error>> + Send>>>,
}

impl PreBinnedValueStream {
    pub fn new(
        patch_coord: PreBinnedPatchCoord,
        channel: Channel,
        agg_kind: AggKind,
        node_config: Arc<NodeConfig>,
    ) -> Self {
        let node_ix = node_ix_for_patch(&patch_coord, &channel, &node_config.cluster);
        assert!(node_ix == node_config.node.id);
        Self {
            patch_coord,
            channel,
            agg_kind,
            node_config,
            open_check_local_file: None,
            fut2: None,
        }
    }

    fn try_setup_fetch_prebinned_higher_res(&mut self) {
        info!("try to find a next better granularity for {:?}", self.patch_coord);
        let g = self.patch_coord.bin_t_len();
        let range = NanoRange {
            beg: self.patch_coord.patch_beg(),
            end: self.patch_coord.patch_end(),
        };
        match PreBinnedPatchRange::covering_range(range, self.patch_coord.bin_count() + 1) {
            Some(range) => {
                let h = range.grid_spec.bin_t_len();
                info!(
                    "FOUND NEXT GRAN  g {}  h {}  ratio {}  mod {}  {:?}",
                    g,
                    h,
                    g / h,
                    g % h,
                    range
                );
                assert!(g / h > 1);
                assert!(g / h < 20);
                assert!(g % h == 0);
                let bin_size = range.grid_spec.bin_t_len();
                let channel = self.channel.clone();
                let agg_kind = self.agg_kind.clone();
                let node_config = self.node_config.clone();
                let patch_it = PreBinnedPatchIterator::from_range(range);
                let s = futures_util::stream::iter(patch_it)
                    .map(move |coord| {
                        PreBinnedValueFetchedStream::new(coord, channel.clone(), agg_kind.clone(), node_config.clone())
                    })
                    .flatten()
                    .map(move |k| {
                        error!("NOTE NOTE NOTE  try_setup_fetch_prebinned_higher_res   ITEM  from sub res bin_size {}   {:?}", bin_size, k);
                        k
                    });
                self.fut2 = Some(Box::pin(s));
            }
            None => {
                warn!("no better resolution found for  g {}", g);
                let evq = EventsQuery {
                    channel: self.channel.clone(),
                    range: NanoRange {
                        beg: self.patch_coord.patch_beg(),
                        end: self.patch_coord.patch_end(),
                    },
                    agg_kind: self.agg_kind.clone(),
                };
                assert!(self.patch_coord.patch_t_len() % self.patch_coord.bin_t_len() == 0);
                let count = self.patch_coord.patch_t_len() / self.patch_coord.bin_t_len();
                let spec = BinSpecDimT {
                    bs: self.patch_coord.bin_t_len(),
                    ts1: self.patch_coord.patch_beg(),
                    ts2: self.patch_coord.patch_end(),
                    count,
                };
                let evq = Arc::new(evq);
                error!("try_setup_fetch_prebinned_higher_res  apply all requested transformations and T-binning");
                let s1 = MergedFromRemotes::new(evq, self.node_config.cluster.clone());
                let s2 = s1
                    .map(|k| {
                        if k.is_err() {
                            error!("\n\n\n..................   try_setup_fetch_prebinned_higher_res  got ERROR");
                        } else {
                            trace!("try_setup_fetch_prebinned_higher_res  got some item from  MergedFromRemotes");
                        }
                        k
                    })
                    .into_binned_t(spec)
                    .map_ok({
                        let mut a = MinMaxAvgScalarBinBatch::empty();
                        move |k| {
                            a.push_single(&k);
                            if a.len() > 0 {
                                let z = std::mem::replace(&mut a, MinMaxAvgScalarBinBatch::empty());
                                Some(z)
                            } else {
                                None
                            }
                        }
                    })
                    .filter_map(|k| {
                        let g = match k {
                            Ok(Some(k)) => Some(Ok(k)),
                            Ok(None) => None,
                            Err(e) => Some(Err(e)),
                        };
                        ready(g)
                    })
                    .take_while({
                        let mut run = true;
                        move |k| {
                            if !run {
                                ready(false)
                            } else {
                                if k.is_err() {
                                    run = false;
                                }
                                ready(true)
                            }
                        }
                    });
                self.fut2 = Some(Box::pin(s2));
            }
        }
    }
}

impl Stream for PreBinnedValueStream {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = Result<MinMaxAvgScalarBinBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if let Some(fut) = self.fut2.as_mut() {
                fut.poll_next_unpin(cx)
            } else if let Some(fut) = self.open_check_local_file.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(Ok(_file)) => err::todoval(),
                    Ready(Err(e)) => match e.kind() {
                        std::io::ErrorKind::NotFound => {
                            error!("TODO LOCAL CACHE FILE NOT FOUND");
                            self.try_setup_fetch_prebinned_higher_res();
                            continue 'outer;
                        }
                        _ => {
                            error!("File I/O error: {:?}", e);
                            Ready(Some(Err(e.into())))
                        }
                    },
                    Pending => Pending,
                }
            } else {
                #[allow(unused_imports)]
                use std::os::unix::fs::OpenOptionsExt;
                let mut opts = std::fs::OpenOptions::new();
                opts.read(true);
                let fut = async { tokio::fs::OpenOptions::from(opts).open("/DOESNOTEXIST").await };
                self.open_check_local_file = Some(Box::pin(fut));
                continue 'outer;
            };
        }
    }
}

pub struct PreBinnedValueFetchedStream {
    uri: http::Uri,
    resfut: Option<hyper::client::ResponseFuture>,
    res: Option<InMemoryFrameAsyncReadStream<HttpBodyAsAsyncRead>>,
}

impl PreBinnedValueFetchedStream {
    pub fn new(
        patch_coord: PreBinnedPatchCoord,
        channel: Channel,
        agg_kind: AggKind,
        node_config: Arc<NodeConfig>,
    ) -> Self {
        let nodeix = node_ix_for_patch(&patch_coord, &channel, &node_config.cluster);
        let node = &node_config.cluster.nodes[nodeix as usize];
        warn!("TODO defining property of a PreBinnedPatchCoord? patchlen + ix? binsize + patchix? binsize + patchsize + patchix?");
        // TODO encapsulate uri creation, how to express aggregation kind?
        let uri: hyper::Uri = format!(
            "http://{}:{}/api/1/prebinned?{}&channel_backend={}&channel_name={}&agg_kind={:?}",
            node.host,
            node.port,
            patch_coord.to_url_params_strings(),
            channel.backend,
            channel.name,
            agg_kind,
        )
        .parse()
        .unwrap();
        Self {
            uri,
            resfut: None,
            res: None,
        }
    }
}

pub type PreBinnedHttpFrame = Result<MinMaxAvgScalarBinBatch, Error>;

impl Stream for PreBinnedValueFetchedStream {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = PreBinnedHttpFrame;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if let Some(res) = self.res.as_mut() {
                pin_mut!(res);
                match res.poll_next(cx) {
                    Ready(Some(Ok(buf))) => match bincode::deserialize::<PreBinnedHttpFrame>(&buf) {
                        Ok(item) => Ready(Some(item)),
                        Err(e) => Ready(Some(Err(e.into()))),
                    },
                    Ready(Some(Err(e))) => Ready(Some(Err(e.into()))),
                    Ready(None) => Ready(None),
                    Pending => Pending,
                }
            } else if let Some(resfut) = self.resfut.as_mut() {
                match resfut.poll_unpin(cx) {
                    Ready(res) => match res {
                        Ok(res) => {
                            info!("PreBinnedValueFetchedStream  GOT result from SUB REQUEST: {:?}", res);
                            let s1 = HttpBodyAsAsyncRead::new(res);
                            let s2 = InMemoryFrameAsyncReadStream::new(s1);
                            self.res = Some(s2);
                            continue 'outer;
                        }
                        Err(e) => {
                            error!("PreBinnedValueStream  error in stream {:?}", e);
                            Ready(Some(Err(e.into())))
                        }
                    },
                    Pending => Pending,
                }
            } else {
                let req = hyper::Request::builder()
                    .method(http::Method::GET)
                    .uri(&self.uri)
                    .body(hyper::Body::empty())?;
                let client = hyper::Client::new();
                info!("PreBinnedValueFetchedStream  START REQUEST FOR {:?}", req);
                self.resfut = Some(client.request(req));
                continue 'outer;
            };
        }
    }
}

pub struct HttpBodyAsAsyncRead {
    inp: Response<hyper::Body>,
    left: Bytes,
    rp: usize,
}

impl HttpBodyAsAsyncRead {
    pub fn new(inp: hyper::Response<hyper::Body>) -> Self {
        Self {
            inp,
            left: Bytes::new(),
            rp: 0,
        }
    }
}

impl AsyncRead for HttpBodyAsAsyncRead {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut ReadBuf) -> Poll<std::io::Result<()>> {
        use hyper::body::HttpBody;
        use Poll::*;
        if self.left.len() != 0 {
            let n1 = buf.remaining();
            let n2 = self.left.len() - self.rp;
            if n2 <= n1 {
                buf.put_slice(self.left[self.rp..].as_ref());
                self.left = Bytes::new();
                self.rp = 0;
                Ready(Ok(()))
            } else {
                buf.put_slice(self.left[self.rp..(self.rp + n2)].as_ref());
                self.rp += n2;
                Ready(Ok(()))
            }
        } else {
            let f = &mut self.inp;
            pin_mut!(f);
            match f.poll_data(cx) {
                Ready(Some(Ok(k))) => {
                    let n1 = buf.remaining();
                    if k.len() <= n1 {
                        buf.put_slice(k.as_ref());
                        Ready(Ok(()))
                    } else {
                        buf.put_slice(k[..n1].as_ref());
                        self.left = k;
                        self.rp = n1;
                        Ready(Ok(()))
                    }
                }
                Ready(Some(Err(e))) => Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    Error::with_msg(format!("Received by HttpBodyAsAsyncRead: {:?}", e)),
                ))),
                Ready(None) => Ready(Ok(())),
                Pending => Pending,
            }
        }
    }
}

type T001 = Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarEventBatch, Error>> + Send>>;
type T002 = Pin<Box<dyn Future<Output = Result<T001, Error>> + Send>>;
pub struct MergedFromRemotes {
    tcp_establish_futs: Vec<T002>,
    nodein: Vec<Option<T001>>,
    merged: Option<T001>,
    completed: bool,
    errored: bool,
}

impl MergedFromRemotes {
    pub fn new(evq: Arc<EventsQuery>, cluster: Arc<Cluster>) -> Self {
        let mut tcp_establish_futs = vec![];
        for node in &cluster.nodes {
            let f = super::raw::x_processed_stream_from_node(evq.clone(), node.clone());
            let f: T002 = Box::pin(f);
            tcp_establish_futs.push(f);
        }
        let n = tcp_establish_futs.len();
        Self {
            tcp_establish_futs,
            nodein: (0..n).into_iter().map(|_| None).collect(),
            merged: None,
            completed: false,
            errored: false,
        }
    }
}

impl Stream for MergedFromRemotes {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = Result<MinMaxAvgScalarEventBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("MergedFromRemotes  ✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗  poll_next on completed");
        }
        if self.errored {
            warn!("MergedFromRemotes  return None after Err");
            self.completed = true;
            return Ready(None);
        }
        'outer: loop {
            break if let Some(fut) = &mut self.merged {
                match fut.poll_next_unpin(cx) {
                    Ready(Some(Ok(k))) => Ready(Some(Ok(k))),
                    Ready(Some(Err(e))) => {
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Ready(None) => {
                        self.completed = true;
                        Ready(None)
                    }
                    Pending => Pending,
                }
            } else {
                let mut pend = false;
                let mut c1 = 0;
                for i1 in 0..self.tcp_establish_futs.len() {
                    if self.nodein[i1].is_none() {
                        let f = &mut self.tcp_establish_futs[i1];
                        pin_mut!(f);
                        match f.poll(cx) {
                            Ready(Ok(k)) => {
                                info!("MergedFromRemotes  tcp_establish_futs  ESTABLISHED INPUT {}", i1);
                                self.nodein[i1] = Some(k);
                            }
                            Ready(Err(e)) => {
                                self.errored = true;
                                return Ready(Some(Err(e)));
                            }
                            Pending => {
                                pend = true;
                            }
                        }
                    } else {
                        c1 += 1;
                    }
                }
                if pend {
                    Pending
                } else {
                    if c1 == self.tcp_establish_futs.len() {
                        debug!("MergedFromRemotes  SETTING UP MERGED STREAM");
                        let inps = self.nodein.iter_mut().map(|k| k.take().unwrap()).collect();
                        let s1 = MergedMinMaxAvgScalarStream::new(inps);
                        self.merged = Some(Box::pin(s1));
                    } else {
                        info!(
                            "MergedFromRemotes  conn / estab  {}  {}",
                            c1,
                            self.tcp_establish_futs.len()
                        );
                    }
                    continue 'outer;
                }
            };
        }
    }
}

pub struct BinnedStream {
    inp: Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarBinBatch, Error>> + Send>>,
}

impl BinnedStream {
    pub fn new(
        patch_it: PreBinnedPatchIterator,
        channel: Channel,
        agg_kind: AggKind,
        node_config: Arc<NodeConfig>,
    ) -> Self {
        warn!("BinnedStream will open a PreBinnedValueStream");
        let inp = futures_util::stream::iter(patch_it)
            .map(move |coord| {
                PreBinnedValueFetchedStream::new(coord, channel.clone(), agg_kind.clone(), node_config.clone())
            })
            .flatten()
            .only_first_error()
            .map(|k| {
                match k {
                    Ok(ref k) => {
                        info!("BinnedStream  got good item {:?}", k);
                    }
                    Err(_) => {
                        error!("\n\n-----------------------------------------------------   BinnedStream  got error")
                    }
                }
                k
            });
        Self { inp: Box::pin(inp) }
    }
}

impl Stream for BinnedStream {
    // TODO make this generic over all possible things
    type Item = Result<MinMaxAvgScalarBinBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => Ready(Some(Ok(k))),
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub struct SomeReturnThing {}

impl From<SomeReturnThing> for Bytes {
    fn from(_k: SomeReturnThing) -> Self {
        error!("TODO convert result to octets");
        todo!("TODO convert result to octets")
    }
}

pub fn node_ix_for_patch(patch_coord: &PreBinnedPatchCoord, channel: &Channel, cluster: &Cluster) -> u32 {
    let mut hash = tiny_keccak::Sha3::v256();
    hash.update(channel.backend.as_bytes());
    hash.update(channel.name.as_bytes());
    hash.update(&patch_coord.patch_beg().to_le_bytes());
    hash.update(&patch_coord.patch_end().to_le_bytes());
    hash.update(&patch_coord.bin_t_len().to_le_bytes());
    let mut out = [0; 32];
    hash.finalize(&mut out);
    let a = [out[0], out[1], out[2], out[3]];
    let ix = u32::from_le_bytes(a) % cluster.nodes.len() as u32;
    info!("node_ix_for_patch  {}", ix);
    ix
}
