use crate::agg::binnedt::IntoBinnedT;
use crate::agg::eventbatch::MinMaxAvgScalarEventBatchStreamItem;
use crate::agg::scalarbinbatch::{MinMaxAvgScalarBinBatch, MinMaxAvgScalarBinBatchStreamItem};
use crate::binnedstream::{BinnedStream, BinnedStreamFromMerged};
use crate::cache::pbv::PreBinnedValueByteStream;
use crate::cache::pbvfs::PreBinnedItem;
use crate::channelconfig::{extract_matching_config_entry, read_local_config};
use crate::frame::makeframe::make_frame;
use crate::merge::MergedMinMaxAvgScalarStream;
use crate::raw::EventsQuery;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, StreamExt};
use hyper::Response;
use netpod::{
    AggKind, BinnedRange, ByteSize, Channel, Cluster, NanoRange, NodeConfigCached, PerfOpts, PreBinnedPatchCoord,
    PreBinnedPatchIterator, PreBinnedPatchRange, ToNanos,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tiny_keccak::Hasher;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

pub mod pbv;
pub mod pbvfs;

#[derive(Clone, Debug)]
pub enum CacheUsage {
    Use,
    Ignore,
    Recreate,
}

impl CacheUsage {
    pub fn query_param_value(&self) -> String {
        match self {
            CacheUsage::Use => "use",
            CacheUsage::Ignore => "ignore",
            CacheUsage::Recreate => "recreate",
        }
        .into()
    }
}

#[derive(Clone, Debug)]
pub struct BinnedQuery {
    range: NanoRange,
    bin_count: u64,
    agg_kind: AggKind,
    channel: Channel,
    cache_usage: CacheUsage,
    disk_stats_every: ByteSize,
}

impl BinnedQuery {
    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let beg_date = params.get("beg_date").ok_or(Error::with_msg("missing beg_date"))?;
        let end_date = params.get("end_date").ok_or(Error::with_msg("missing end_date"))?;
        let disk_stats_every = params
            .get("disk_stats_every_kb")
            .ok_or(Error::with_msg("missing disk_stats_every_kb"))?;
        let disk_stats_every = disk_stats_every
            .parse()
            .map_err(|e| Error::with_msg(format!("can not parse disk_stats_every_kb {:?}", e)))?;
        let ret = BinnedQuery {
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            bin_count: params
                .get("bin_count")
                .ok_or(Error::with_msg("missing bin_count"))?
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse bin_count {:?}", e)))?,
            agg_kind: AggKind::DimXBins1,
            channel: channel_from_params(&params)?,
            cache_usage: cache_usage_from_params(&params)?,
            disk_stats_every: ByteSize::kb(disk_stats_every),
        };
        info!("BinnedQuery::from_request  {:?}", ret);
        Ok(ret)
    }
}

#[derive(Clone, Debug)]
pub struct PreBinnedQuery {
    patch: PreBinnedPatchCoord,
    agg_kind: AggKind,
    channel: Channel,
    cache_usage: CacheUsage,
    disk_stats_every: ByteSize,
}

impl PreBinnedQuery {
    pub fn new(
        patch: PreBinnedPatchCoord,
        channel: Channel,
        agg_kind: AggKind,
        cache_usage: CacheUsage,
        disk_stats_every: ByteSize,
    ) -> Self {
        Self {
            patch,
            agg_kind,
            channel,
            cache_usage,
            disk_stats_every,
        }
    }

    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let patch_ix = params
            .get("patch_ix")
            .ok_or(Error::with_msg("missing patch_ix"))?
            .parse()?;
        let bin_t_len = params
            .get("bin_t_len")
            .ok_or(Error::with_msg("missing bin_t_len"))?
            .parse()?;
        let disk_stats_every = params
            .get("disk_stats_every_kb")
            .ok_or(Error::with_msg("missing disk_stats_every_kb"))?;
        let disk_stats_every = disk_stats_every
            .parse()
            .map_err(|e| Error::with_msg(format!("can not parse disk_stats_every_kb {:?}", e)))?;
        let ret = PreBinnedQuery {
            patch: PreBinnedPatchCoord::new(bin_t_len, patch_ix),
            agg_kind: AggKind::DimXBins1,
            channel: channel_from_params(&params)?,
            cache_usage: cache_usage_from_params(&params)?,
            disk_stats_every: ByteSize::kb(disk_stats_every),
        };
        Ok(ret)
    }

    pub fn make_query_string(&self) -> String {
        let cache_usage = match self.cache_usage {
            CacheUsage::Use => "use",
            CacheUsage::Ignore => "ignore",
            CacheUsage::Recreate => "recreate",
        };
        format!(
            "{}&channel_backend={}&channel_name={}&agg_kind={:?}&cache_usage={}&disk_stats_every_kb={}",
            self.patch.to_url_params_strings(),
            self.channel.backend,
            self.channel.name,
            self.agg_kind,
            cache_usage,
            self.disk_stats_every.bytes() / 1024,
        )
    }

    pub fn patch(&self) -> &PreBinnedPatchCoord {
        &self.patch
    }
}

fn channel_from_params(params: &BTreeMap<String, String>) -> Result<Channel, Error> {
    let ret = Channel {
        backend: params
            .get("channel_backend")
            .ok_or(Error::with_msg("missing channel_backend"))?
            .into(),
        name: params
            .get("channel_name")
            .ok_or(Error::with_msg("missing channel_name"))?
            .into(),
    };
    Ok(ret)
}

fn cache_usage_from_params(params: &BTreeMap<String, String>) -> Result<CacheUsage, Error> {
    let ret = params.get("cache_usage").map_or(Ok::<_, Error>(CacheUsage::Use), |k| {
        if k == "use" {
            Ok(CacheUsage::Use)
        } else if k == "ignore" {
            Ok(CacheUsage::Ignore)
        } else if k == "recreate" {
            Ok(CacheUsage::Recreate)
        } else {
            Err(Error::with_msg(format!("unexpected cache_usage {:?}", k)))?
        }
    })?;
    Ok(ret)
}

type BinnedStreamBox = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

pub async fn binned_bytes_for_http(
    node_config: &NodeConfigCached,
    query: &BinnedQuery,
) -> Result<BinnedStreamBox, Error> {
    if query.channel.backend != node_config.node.backend {
        let err = Error::with_msg(format!(
            "backend mismatch  node: {}  requested: {}",
            node_config.node.backend, query.channel.backend
        ));
        return Err(err);
    }
    let range = &query.range;
    let channel_config = read_local_config(&query.channel, &node_config.node).await?;
    let entry = extract_matching_config_entry(range, &channel_config);
    info!("binned_bytes_for_http  found config entry {:?}", entry);
    let range = BinnedRange::covering_range(range.clone(), query.bin_count).ok_or(Error::with_msg(format!(
        "binned_bytes_for_http  BinnedRange::covering_range returned None"
    )))?;
    let perf_opts = PerfOpts { inmem_bufcap: 512 };
    match PreBinnedPatchRange::covering_range(query.range.clone(), query.bin_count) {
        Some(pre_range) => {
            info!("binned_bytes_for_http  found pre_range: {:?}", pre_range);
            if range.grid_spec.bin_t_len() < pre_range.grid_spec.bin_t_len() {
                let msg = format!(
                    "binned_bytes_for_http  incompatible ranges:\npre_range: {:?}\nrange: {:?}",
                    pre_range, range
                );
                return Err(Error::with_msg(msg));
            }
            let s1 = BinnedStream::new(
                PreBinnedPatchIterator::from_range(pre_range),
                query.channel.clone(),
                range,
                query.agg_kind.clone(),
                query.cache_usage.clone(),
                node_config,
                query.disk_stats_every.clone(),
            )?;
            let ret = BinnedBytesForHttpStream::new(s1);
            Ok(Box::pin(ret))
        }
        None => {
            info!(
                "binned_bytes_for_http  no covering range for prebinned, merge from remotes instead {:?}",
                range
            );
            let evq = EventsQuery {
                channel: query.channel.clone(),
                range: query.range.clone(),
                agg_kind: query.agg_kind.clone(),
            };
            // TODO do I need to set up more transformations or binning to deliver the requested data?
            let s1 = MergedFromRemotes::new(evq, perf_opts, node_config.node_config.cluster.clone());
            let s1 = s1.into_binned_t(range);
            let s1 = BinnedStreamFromMerged::new(Box::pin(s1))?;
            let ret = BinnedBytesForHttpStream::new(s1);
            Ok(Box::pin(ret))
        }
    }
}

pub type BinnedBytesForHttpStreamFrame = <BinnedStream as Stream>::Item;

pub struct BinnedBytesForHttpStream<S> {
    inp: S,
    errored: bool,
    completed: bool,
}

impl<S> BinnedBytesForHttpStream<S> {
    pub fn new(inp: S) -> Self {
        Self {
            inp,
            errored: false,
            completed: false,
        }
    }
}

impl<S> Stream for BinnedBytesForHttpStream<S>
where
    S: Stream<Item = Result<MinMaxAvgScalarBinBatchStreamItem, Error>> + Unpin,
{
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
            Ready(Some(item)) => match make_frame::<BinnedBytesForHttpStreamFrame>(&item) {
                Ok(buf) => Ready(Some(Ok(buf.freeze()))),
                Err(e) => {
                    self.errored = true;
                    Ready(Some(Err(e.into())))
                }
            },
            Ready(None) => {
                self.completed = true;
                Ready(None)
            }
            Pending => Pending,
        }
    }
}

// NOTE  This answers a request for a single valid pre-binned patch.
// A user must first make sure that the grid spec is valid, and that this node is responsible for it.
// Otherwise it is an error.
pub fn pre_binned_bytes_for_http(
    node_config: &NodeConfigCached,
    query: &PreBinnedQuery,
) -> Result<PreBinnedValueByteStream, Error> {
    if query.channel.backend != node_config.node.backend {
        let err = Error::with_msg(format!(
            "backend mismatch  node: {}  requested: {}",
            node_config.node.backend, query.channel.backend
        ));
        return Err(err);
    }
    let patch_node_ix = node_ix_for_patch(&query.patch, &query.channel, &node_config.node_config.cluster);
    if node_config.ix as u32 != patch_node_ix {
        Err(Error::with_msg(format!(
            "pre_binned_bytes_for_http node mismatch  node_config.ix {}  patch_node_ix {}",
            node_config.ix, patch_node_ix
        )))
    } else {
        let ret = super::cache::pbv::pre_binned_value_byte_stream_new(query, node_config);
        Ok(ret)
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

type T001 = Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarEventBatchStreamItem, Error>> + Send>>;
type T002 = Pin<Box<dyn Future<Output = Result<T001, Error>> + Send>>;
pub struct MergedFromRemotes {
    tcp_establish_futs: Vec<T002>,
    nodein: Vec<Option<T001>>,
    merged: Option<T001>,
    completed: bool,
    errored: bool,
}

impl MergedFromRemotes {
    pub fn new(evq: EventsQuery, perf_opts: PerfOpts, cluster: Cluster) -> Self {
        let mut tcp_establish_futs = vec![];
        for node in &cluster.nodes {
            let f = super::raw::x_processed_stream_from_node(evq.clone(), perf_opts.clone(), node.clone());
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
    type Item = Result<MinMaxAvgScalarEventBatchStreamItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("MergedFromRemotes  poll_next on completed");
        }
        if self.errored {
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
                        debug!("MergedFromRemotes  setting up merged stream");
                        let inps = self.nodein.iter_mut().map(|k| k.take().unwrap()).collect();
                        let s1 = MergedMinMaxAvgScalarStream::new(inps);
                        self.merged = Some(Box::pin(s1));
                    } else {
                        debug!(
                            "MergedFromRemotes  raw / estab  {}  {}",
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

pub struct BytesWrap {}

impl From<BytesWrap> for Bytes {
    fn from(_k: BytesWrap) -> Self {
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
    ix
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheFileDesc {
    // What identifies a cached file?
    channel: Channel,
    agg_kind: AggKind,
    patch: PreBinnedPatchCoord,
}

impl CacheFileDesc {
    pub fn hash(&self) -> String {
        let mut h = tiny_keccak::Sha3::v256();
        h.update(b"V000");
        h.update(self.channel.backend.as_bytes());
        h.update(self.channel.name.as_bytes());
        h.update(format!("{:?}", self.agg_kind).as_bytes());
        h.update(&self.patch.spec().bin_t_len().to_le_bytes());
        h.update(&self.patch.spec().patch_t_len().to_le_bytes());
        h.update(&self.patch.ix().to_le_bytes());
        let mut buf = [0; 32];
        h.finalize(&mut buf);
        hex::encode(&buf)
    }

    pub fn hash_channel(&self) -> String {
        let mut h = tiny_keccak::Sha3::v256();
        h.update(b"V000");
        h.update(self.channel.backend.as_bytes());
        h.update(self.channel.name.as_bytes());
        let mut buf = [0; 32];
        h.finalize(&mut buf);
        hex::encode(&buf)
    }

    pub fn path(&self, node_config: &NodeConfigCached) -> PathBuf {
        let hash = self.hash();
        let hc = self.hash_channel();
        node_config
            .node
            .data_base_path
            .join("cache")
            .join(&hc[0..3])
            .join(&hc[3..6])
            .join(&self.channel.name)
            .join(format!("{:?}", self.agg_kind))
            .join(format!("{:019}", self.patch.spec().bin_t_len()))
            .join(&hash[0..2])
            .join(format!("{:019}", self.patch.ix()))
    }
}

pub async fn write_pb_cache_min_max_avg_scalar(
    values: MinMaxAvgScalarBinBatch,
    patch: PreBinnedPatchCoord,
    agg_kind: AggKind,
    channel: Channel,
    node_config: NodeConfigCached,
) -> Result<(), Error> {
    let cfd = CacheFileDesc {
        channel: channel.clone(),
        patch: patch.clone(),
        agg_kind: agg_kind.clone(),
    };
    let path = cfd.path(&node_config);
    let enc = serde_cbor::to_vec(&values)?;
    info!("Writing cache file  size {}\n{:?}\npath: {:?}", enc.len(), cfd, path);
    tokio::fs::create_dir_all(path.parent().unwrap()).await?;
    tokio::task::spawn_blocking({
        let path = path.clone();
        move || {
            use fs2::FileExt;
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)?;
            f.lock_exclusive()?;
            f.write_all(&enc)?;
            f.unlock()?;
            Ok::<_, Error>(())
        }
    })
    .await??;
    Ok(())
}

pub async fn read_pbv(mut file: File) -> Result<PreBinnedItem, Error> {
    let mut buf = vec![];
    file.read_to_end(&mut buf).await?;
    trace!("Read cached file  len {}", buf.len());
    let dec: MinMaxAvgScalarBinBatch = serde_cbor::from_slice(&buf)?;
    Ok(PreBinnedItem::Batch(dec))
}
