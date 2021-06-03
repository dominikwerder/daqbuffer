use crate::agg::streams::StreamItem;
use crate::binned::{RangeCompletableItem, StreamKind};
use crate::cache::pbv::PreBinnedValueByteStream;
use crate::frame::makeframe::FrameType;
use crate::merge::MergedStream;
use crate::raw::EventsQuery;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, StreamExt};
use hyper::{Body, Response};
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{
    AggKind, ByteSize, Channel, Cluster, HostPort, NanoRange, NodeConfigCached, PerfOpts, PreBinnedPatchCoord, ToNanos,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tiny_keccak::Hasher;
use tokio::io::{AsyncRead, ReadBuf};

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

    pub fn from_params(params: &BTreeMap<String, String>) -> Result<Self, Error> {
        let ret = params.get("cacheUsage").map_or(Ok::<_, Error>(CacheUsage::Use), |k| {
            if k == "use" {
                Ok(CacheUsage::Use)
            } else if k == "ignore" {
                Ok(CacheUsage::Ignore)
            } else if k == "recreate" {
                Ok(CacheUsage::Recreate)
            } else {
                Err(Error::with_msg(format!("unexpected cacheUsage {:?}", k)))?
            }
        })?;
        Ok(ret)
    }

    pub fn from_string(s: &str) -> Result<Self, Error> {
        let ret = if s == "ignore" {
            CacheUsage::Ignore
        } else if s == "recreate" {
            CacheUsage::Recreate
        } else if s == "use" {
            CacheUsage::Use
        } else {
            return Err(Error::with_msg(format!("can not interpret cache usage string: {}", s)));
        };
        Ok(ret)
    }
}

impl Display for CacheUsage {
    fn fmt(&self, fmt: &mut Formatter) -> std::fmt::Result {
        write!(fmt, "{}", self.query_param_value())
    }
}

#[derive(Clone, Debug)]
pub struct BinnedQuery {
    channel: Channel,
    range: NanoRange,
    bin_count: u32,
    agg_kind: AggKind,
    cache_usage: CacheUsage,
    disk_stats_every: ByteSize,
    report_error: bool,
}

impl BinnedQuery {
    pub fn new(channel: Channel, range: NanoRange, bin_count: u32, agg_kind: AggKind) -> BinnedQuery {
        BinnedQuery {
            channel,
            range,
            bin_count,
            agg_kind,
            cache_usage: CacheUsage::Use,
            disk_stats_every: ByteSize(1024 * 1024 * 4),
            report_error: false,
        }
    }

    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let beg_date = params.get("begDate").ok_or(Error::with_msg("missing begDate"))?;
        let end_date = params.get("endDate").ok_or(Error::with_msg("missing endDate"))?;
        let disk_stats_every = params.get("diskStatsEveryKb").map_or("2000", |k| k);
        let disk_stats_every = disk_stats_every
            .parse()
            .map_err(|e| Error::with_msg(format!("can not parse diskStatsEveryKb {:?}", e)))?;
        let ret = BinnedQuery {
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            bin_count: params
                .get("binCount")
                .ok_or(Error::with_msg("missing binCount"))?
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse binCount {:?}", e)))?,
            agg_kind: params
                .get("aggKind")
                .map_or("DimXBins1", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse aggKind {:?}", e)))?,
            channel: channel_from_params(&params)?,
            cache_usage: CacheUsage::from_params(&params)?,
            disk_stats_every: ByteSize::kb(disk_stats_every),
            report_error: params
                .get("reportError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse reportError {:?}", e)))?,
        };
        info!("BinnedQuery::from_request  {:?}", ret);
        Ok(ret)
    }

    pub fn range(&self) -> &NanoRange {
        &self.range
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn bin_count(&self) -> u32 {
        self.bin_count
    }

    pub fn agg_kind(&self) -> &AggKind {
        &self.agg_kind
    }

    pub fn cache_usage(&self) -> &CacheUsage {
        &self.cache_usage
    }

    pub fn disk_stats_every(&self) -> &ByteSize {
        &self.disk_stats_every
    }

    pub fn report_error(&self) -> bool {
        self.report_error
    }

    pub fn set_cache_usage(&mut self, k: CacheUsage) {
        self.cache_usage = k;
    }

    pub fn set_disk_stats_every(&mut self, k: ByteSize) {
        self.disk_stats_every = k;
    }

    // TODO the BinnedQuery itself should maybe already carry the full HostPort?
    // On the other hand, want to keep the flexibility for the fail over possibility..
    pub fn url(&self, host: &HostPort) -> String {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
        format!(
            "http://{}:{}/api/4/binned?cacheUsage={}&channelBackend={}&channelName={}&binCount={}&begDate={}&endDate={}&diskStatsEveryKb={}",
            host.host,
            host.port,
            self.cache_usage,
            self.channel.backend,
            self.channel.name,
            self.bin_count,
            Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt),
            Utc.timestamp_nanos(self.range.end as i64).format(date_fmt),
            self.disk_stats_every.bytes() / 1024,
        )
    }
}

#[derive(Clone, Debug)]
pub struct PreBinnedQuery {
    patch: PreBinnedPatchCoord,
    agg_kind: AggKind,
    channel: Channel,
    cache_usage: CacheUsage,
    disk_stats_every: ByteSize,
    report_error: bool,
}

impl PreBinnedQuery {
    pub fn new(
        patch: PreBinnedPatchCoord,
        channel: Channel,
        agg_kind: AggKind,
        cache_usage: CacheUsage,
        disk_stats_every: ByteSize,
        report_error: bool,
    ) -> Self {
        Self {
            patch,
            agg_kind,
            channel,
            cache_usage,
            disk_stats_every,
            report_error,
        }
    }

    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let patch_ix = params
            .get("patchIx")
            .ok_or(Error::with_msg("missing patchIx"))?
            .parse()?;
        let bin_t_len = params
            .get("binTlen")
            .ok_or(Error::with_msg("missing binTlen"))?
            .parse()?;
        let patch_t_len = params
            .get("patchTlen")
            .ok_or(Error::with_msg("missing patchTlen"))?
            .parse()?;
        let disk_stats_every = params
            .get("diskStatsEveryKb")
            .ok_or(Error::with_msg("missing diskStatsEveryKb"))?;
        let disk_stats_every = disk_stats_every
            .parse()
            .map_err(|e| Error::with_msg(format!("can not parse diskStatsEveryKb {:?}", e)))?;
        let ret = PreBinnedQuery {
            patch: PreBinnedPatchCoord::new(bin_t_len, patch_t_len, patch_ix),
            agg_kind: params
                .get("aggKind")
                .map_or(&format!("{}", AggKind::DimXBins1), |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse aggKind {:?}", e)))?,
            channel: channel_from_params(&params)?,
            cache_usage: CacheUsage::from_params(&params)?,
            disk_stats_every: ByteSize::kb(disk_stats_every),
            report_error: params
                .get("reportError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse reportError {:?}", e)))?,
        };
        Ok(ret)
    }

    pub fn make_query_string(&self) -> String {
        format!(
            "{}&channelBackend={}&channelName={}&aggKind={}&cacheUsage={}&diskStatsEveryKb={}&reportError={}",
            self.patch.to_url_params_strings(),
            self.channel.backend,
            self.channel.name,
            self.agg_kind,
            self.cache_usage,
            self.disk_stats_every.bytes() / 1024,
            self.report_error(),
        )
    }

    pub fn patch(&self) -> &PreBinnedPatchCoord {
        &self.patch
    }

    pub fn report_error(&self) -> bool {
        self.report_error
    }
}

fn channel_from_params(params: &BTreeMap<String, String>) -> Result<Channel, Error> {
    let ret = Channel {
        backend: params
            .get("channelBackend")
            .ok_or(Error::with_msg("missing channelBackend"))?
            .into(),
        name: params
            .get("channelName")
            .ok_or(Error::with_msg("missing channelName"))?
            .into(),
    };
    Ok(ret)
}

// NOTE  This answers a request for a single valid pre-binned patch.
// A user must first make sure that the grid spec is valid, and that this node is responsible for it.
// Otherwise it is an error.
pub fn pre_binned_bytes_for_http<SK>(
    node_config: &NodeConfigCached,
    query: &PreBinnedQuery,
    stream_kind: SK,
) -> Result<PreBinnedValueByteStream<SK>, Error>
where
    SK: StreamKind,
    Result<StreamItem<RangeCompletableItem<SK::TBinnedBins>>, err::Error>: FrameType,
{
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
        let ret = crate::cache::pbv::pre_binned_value_byte_stream_new(query, node_config, stream_kind);
        Ok(ret)
    }
}

pub struct HttpBodyAsAsyncRead {
    inp: Response<Body>,
    left: Bytes,
    rp: usize,
}

impl HttpBodyAsAsyncRead {
    pub fn new(inp: Response<Body>) -> Self {
        Self {
            inp,
            left: Bytes::new(),
            rp: 0,
        }
    }
}

impl AsyncRead for HttpBodyAsAsyncRead {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut ReadBuf) -> Poll<io::Result<()>> {
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
                Ready(Some(Err(e))) => Ready(Err(io::Error::new(
                    io::ErrorKind::Other,
                    Error::with_msg(format!("Received by HttpBodyAsAsyncRead: {:?}", e)),
                ))),
                Ready(None) => Ready(Ok(())),
                Pending => Pending,
            }
        }
    }
}

type T001<T> = Pin<Box<dyn Stream<Item = Result<StreamItem<T>, Error>> + Send>>;
type T002<T> = Pin<Box<dyn Future<Output = Result<T001<T>, Error>> + Send>>;

pub struct MergedFromRemotes<SK>
where
    SK: StreamKind,
{
    tcp_establish_futs: Vec<T002<RangeCompletableItem<<SK as StreamKind>::XBinnedEvents>>>,
    nodein: Vec<Option<T001<RangeCompletableItem<<SK as StreamKind>::XBinnedEvents>>>>,
    merged: Option<T001<RangeCompletableItem<<SK as StreamKind>::XBinnedEvents>>>,
    completed: bool,
    errored: bool,
}

impl<SK> MergedFromRemotes<SK>
where
    SK: StreamKind,
{
    pub fn new(evq: EventsQuery, perf_opts: PerfOpts, cluster: Cluster, stream_kind: SK) -> Self {
        let mut tcp_establish_futs = vec![];
        for node in &cluster.nodes {
            let f = super::raw::x_processed_stream_from_node(
                evq.clone(),
                perf_opts.clone(),
                node.clone(),
                stream_kind.clone(),
            );
            let f: T002<RangeCompletableItem<<SK as StreamKind>::XBinnedEvents>> = Box::pin(f);
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

impl<SK> Stream for MergedFromRemotes<SK>
where
    SK: StreamKind,
{
    type Item = Result<StreamItem<RangeCompletableItem<<SK as StreamKind>::XBinnedEvents>>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("MergedFromRemotes  poll_next on completed");
            } else if self.errored {
                self.completed = true;
                return Ready(None);
            } else if let Some(fut) = &mut self.merged {
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
                        let s1 = MergedStream::<_, SK>::new(inps);
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
    hash.update(&patch_coord.patch_t_len().to_le_bytes());
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
        h.update(format!("{}", self.agg_kind).as_bytes());
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
            .join(format!("{}", self.agg_kind))
            .join(format!(
                "{:010}-{:010}",
                self.patch.spec().bin_t_len() / SEC,
                self.patch.spec().patch_t_len() / SEC
            ))
            .join(format!("{}-{:012}", &hash[0..6], self.patch.ix()))
    }
}

pub struct WrittenPbCache {
    bytes: u64,
}

pub async fn write_pb_cache_min_max_avg_scalar<T>(
    values: T,
    patch: PreBinnedPatchCoord,
    agg_kind: AggKind,
    channel: Channel,
    node_config: NodeConfigCached,
) -> Result<WrittenPbCache, Error>
where
    T: Serialize,
{
    let cfd = CacheFileDesc {
        channel: channel.clone(),
        patch: patch.clone(),
        agg_kind: agg_kind.clone(),
    };
    let path = cfd.path(&node_config);
    let enc = serde_cbor::to_vec(&values)?;
    tokio::fs::create_dir_all(path.parent().unwrap()).await?;
    let now = Utc::now();
    let mut h = crc32fast::Hasher::new();
    h.update(&now.timestamp_nanos().to_le_bytes());
    let r = h.finalize();
    let tmp_path =
        path.parent()
            .unwrap()
            .join(format!("{}.tmp.{:08x}", path.file_name().unwrap().to_str().unwrap(), r));
    let res = tokio::task::spawn_blocking({
        let tmp_path = tmp_path.clone();
        move || {
            use fs2::FileExt;
            use io::Write;
            info!("try to write tmp at {:?}", tmp_path);
            let mut f = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp_path)?;
            if false {
                f.lock_exclusive()?;
            }
            f.write_all(&enc)?;
            if false {
                f.unlock()?;
            }
            f.flush()?;
            Ok::<_, Error>(enc.len())
        }
    })
    .await??;
    tokio::fs::rename(&tmp_path, &path).await?;
    let ret = WrittenPbCache { bytes: res as u64 };
    Ok(ret)
}

#[derive(Serialize)]
pub struct ClearCacheAllResult {
    pub log: Vec<String>,
}

pub async fn clear_cache_all(node_config: &NodeConfigCached, dry: bool) -> Result<ClearCacheAllResult, Error> {
    let mut log = vec![];
    log.push(format!("begin at {:?}", chrono::Utc::now()));
    if dry {
        log.push(format!("dry run"));
    }
    let mut dirs = VecDeque::new();
    let mut stack = VecDeque::new();
    stack.push_front(node_config.node.data_base_path.join("cache"));
    loop {
        match stack.pop_front() {
            Some(path) => {
                let mut rd = tokio::fs::read_dir(path).await?;
                while let Some(entry) = rd.next_entry().await? {
                    let path = entry.path();
                    match path.to_str() {
                        Some(_pathstr) => {
                            let meta = path.symlink_metadata()?;
                            //log.push(format!("len {:7}  pathstr {}", meta.len(), pathstr,));
                            let filename_str = path.file_name().unwrap().to_str().unwrap();
                            if filename_str.ends_with("..") || filename_str.ends_with(".") {
                                log.push(format!("ERROR encountered . or .."));
                            } else {
                                if meta.is_dir() {
                                    stack.push_front(path.clone());
                                    dirs.push_front((meta.len(), path));
                                } else if meta.is_file() {
                                    log.push(format!("remove file  len {:7}  {}", meta.len(), path.to_string_lossy()));
                                    if !dry {
                                        match tokio::fs::remove_file(&path).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                log.push(format!(
                                                    "can not remove file  {}  {:?}",
                                                    path.to_string_lossy(),
                                                    e
                                                ));
                                            }
                                        }
                                    }
                                } else {
                                    log.push(format!("not file, note dir"));
                                }
                            }
                        }
                        None => {
                            log.push(format!("Invalid utf-8 path encountered"));
                        }
                    }
                }
            }
            None => break,
        }
    }
    log.push(format!(
        "start to remove {} dirs at {:?}",
        dirs.len(),
        chrono::Utc::now()
    ));
    for (len, path) in dirs {
        log.push(format!("remove dir  len {}  {}", len, path.to_string_lossy()));
        if !dry {
            match tokio::fs::remove_dir(&path).await {
                Ok(_) => {}
                Err(e) => {
                    log.push(format!("can not remove dir  {}  {:?}", path.to_string_lossy(), e));
                }
            }
        }
    }
    log.push(format!("done at {:?}", chrono::Utc::now()));
    let ret = ClearCacheAllResult { log };
    Ok(ret)
}
