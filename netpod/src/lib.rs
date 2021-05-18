use chrono::{DateTime, TimeZone, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use timeunits::*;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

pub mod status;
pub mod streamext;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggQuerySingleChannel {
    pub channel_config: ChannelConfig,
    pub timebin: u32,
    pub tb_file_count: u32,
    pub buffer_size: u32,
}

pub struct BodyStream {
    //pub receiver: async_channel::Receiver<Result<bytes::Bytes, Error>>,
    pub inner: Box<dyn futures_core::Stream<Item = Result<bytes::Bytes, Error>> + Send + Unpin>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ScalarType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
}

impl ScalarType {
    pub fn from_dtype_index(ix: u8) -> Result<Self, Error> {
        use ScalarType::*;
        let g = match ix {
            0 => return Err(Error::with_msg(format!("BOOL not supported"))),
            1 => return Err(Error::with_msg(format!("BOOL8 not supported"))),
            3 => U8,
            5 => U16,
            8 => U32,
            10 => U64,
            2 => I8,
            4 => I16,
            7 => I32,
            9 => I64,
            11 => F32,
            12 => F64,
            6 => return Err(Error::with_msg(format!("CHARACTER not supported"))),
            13 => return Err(Error::with_msg(format!("STRING not supported"))),
            _ => return Err(Error::with_msg(format!("unknown dtype code: {}", ix))),
        };
        Ok(g)
    }

    pub fn bytes(&self) -> u8 {
        use ScalarType::*;
        match self {
            U8 => 1,
            U16 => 2,
            U32 => 4,
            U64 => 8,
            I8 => 1,
            I16 => 2,
            I32 => 4,
            I64 => 8,
            F32 => 4,
            F64 => 8,
        }
    }

    pub fn index(&self) -> u8 {
        use ScalarType::*;
        match self {
            U8 => 3,
            U16 => 5,
            U32 => 8,
            U64 => 10,
            I8 => 2,
            I16 => 4,
            I32 => 7,
            I64 => 9,
            F32 => 11,
            F64 => 12,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub host: String,
    pub listen: String,
    pub port: u16,
    pub port_raw: u16,
    pub split: u32,
    pub data_base_path: PathBuf,
    pub ksprefix: String,
    pub backend: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    pub host: String,
    pub user: String,
    pub pass: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cluster {
    pub nodes: Vec<Node>,
    pub database: Database,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    pub name: String,
    pub cluster: Cluster,
}

impl NodeConfig {
    pub fn get_node(&self) -> Option<(&Node, usize)> {
        if self.name.contains(":") {
            let mut i1 = 0;
            for n in &self.cluster.nodes {
                if self.name == format!("{}:{}", n.host, n.port) {
                    return Some((n, i1));
                }
                i1 += 1;
            }
        } else {
            let mut i1 = 0;
            for n in &self.cluster.nodes {
                if self.name == format!("{}", n.host) {
                    return Some((n, i1));
                }
                i1 += 1;
            }
        }
        None
    }
}

#[derive(Clone, Debug)]
pub struct NodeConfigCached {
    pub node_config: NodeConfig,
    pub node: Node,
    pub ix: usize,
}

impl From<NodeConfig> for Result<NodeConfigCached, Error> {
    fn from(k: NodeConfig) -> Self {
        match k.get_node() {
            Some((node, ix)) => {
                let ret = NodeConfigCached {
                    node: node.clone(),
                    node_config: k,
                    ix,
                };
                Ok(ret)
            }
            None => Err(Error::with_msg(format!("can not find node {:?}", k))),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Channel {
    pub backend: String,
    pub name: String,
}

impl Channel {
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TimeRange {
    Time { beg: DateTime<Utc>, end: DateTime<Utc> },
    Pulse { beg: u64, end: u64 },
    Nano { beg: u64, end: u64 },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Nanos {
    pub ns: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NanoRange {
    pub beg: u64,
    pub end: u64,
}

impl std::fmt::Debug for NanoRange {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "NanoRange {{ beg: {} s, end: {} s }}",
            self.beg / SEC,
            self.end / SEC
        )
    }
}

impl NanoRange {
    pub fn delta(&self) -> u64 {
        self.end - self.beg
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelConfig {
    pub channel: Channel,
    pub keyspace: u8,
    pub time_bin_size: Nanos,
    pub scalar_type: ScalarType,
    pub compression: bool,
    pub shape: Shape,
    pub array: bool,
    pub big_endian: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Shape {
    Scalar,
    Wave(u32),
}

pub mod timeunits {
    pub const MU: u64 = 1000;
    pub const MS: u64 = MU * 1000;
    pub const SEC: u64 = MS * 1000;
    pub const MIN: u64 = SEC * 60;
    pub const HOUR: u64 = MIN * 60;
    pub const DAY: u64 = HOUR * 24;
}

const BIN_T_LEN_OPTIONS: [u64; 4] = [SEC, MIN * 10, HOUR * 2, DAY];

const PATCH_T_LEN_OPTIONS_SCALAR: [u64; 4] = [MIN * 60, HOUR * 2, DAY * 4, DAY * 32];

const PATCH_T_LEN_OPTIONS_WAVE: [u64; 4] = [MIN * 20, HOUR * 2, DAY * 4, DAY * 32];

const BIN_THRESHOLDS: [u64; 31] = [
    2,
    10,
    100,
    1000,
    10_000,
    100_000,
    MU,
    MU * 10,
    MU * 100,
    MS,
    MS * 10,
    MS * 100,
    SEC,
    SEC * 5,
    SEC * 10,
    SEC * 20,
    MIN,
    MIN * 5,
    MIN * 10,
    MIN * 20,
    HOUR,
    HOUR * 2,
    HOUR * 4,
    HOUR * 12,
    DAY,
    DAY * 2,
    DAY * 4,
    DAY * 8,
    DAY * 16,
    DAY * 32,
    DAY * 64,
];

#[derive(Clone, Serialize, Deserialize)]
pub struct PreBinnedPatchGridSpec {
    bin_t_len: u64,
    patch_t_len: u64,
}

impl PreBinnedPatchGridSpec {
    pub fn new(bin_t_len: u64, patch_t_len: u64) -> Self {
        if !Self::is_valid_bin_t_len(bin_t_len) {
            panic!("PreBinnedPatchGridSpec  invalid bin_t_len  {}", bin_t_len);
        }
        Self { bin_t_len, patch_t_len }
    }

    pub fn bin_t_len(&self) -> u64 {
        self.bin_t_len
    }

    pub fn is_valid_bin_t_len(bin_t_len: u64) -> bool {
        for &j in BIN_T_LEN_OPTIONS.iter() {
            if bin_t_len == j {
                return true;
            }
        }
        return false;
    }

    pub fn patch_t_len(&self) -> u64 {
        self.patch_t_len
    }
}

impl std::fmt::Debug for PreBinnedPatchGridSpec {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "PreBinnedPatchGridSpec {{ bin_t_len: {:?}, patch_t_len(): {:?} }}",
            self.bin_t_len / SEC,
            self.patch_t_len() / SEC,
        )
    }
}

#[derive(Clone, Debug)]
pub struct PreBinnedPatchRange {
    pub grid_spec: PreBinnedPatchGridSpec,
    pub offset: u64,
    pub count: u64,
}

fn get_patch_t_len(bin_t_len: u64) -> u64 {
    // TODO mechanism to select different patch lengths for different channels.
    let shape = Shape::Scalar;
    match shape {
        Shape::Scalar => {
            for (i1, &j) in BIN_T_LEN_OPTIONS.iter().enumerate() {
                if bin_t_len == j {
                    return PATCH_T_LEN_OPTIONS_SCALAR[i1];
                }
            }
        }
        Shape::Wave(_) => {
            for (i1, &j) in BIN_T_LEN_OPTIONS.iter().enumerate() {
                if bin_t_len == j {
                    return PATCH_T_LEN_OPTIONS_WAVE[i1];
                }
            }
        }
    }
    panic!()
}

impl PreBinnedPatchRange {
    /// Cover at least the given range with at least as many as the requested number of bins.
    pub fn covering_range(range: NanoRange, min_bin_count: u64) -> Result<Option<Self>, Error> {
        if min_bin_count < 1 {
            Err(Error::with_msg("min_bin_count < 1"))?;
        }
        if min_bin_count > 4000 {
            Err(Error::with_msg("min_bin_count > 4000"))?;
        }
        let dt = range.delta();
        if dt > DAY * 14 {
            Err(Error::with_msg("dt > DAY * 14"))?;
        }
        let bs = dt / min_bin_count;
        let mut i1 = BIN_T_LEN_OPTIONS.len();
        loop {
            if i1 <= 0 {
                break Ok(None);
            } else {
                i1 -= 1;
                let t = BIN_T_LEN_OPTIONS[i1];
                if t <= bs {
                    let bin_t_len = t;
                    let patch_t_len = get_patch_t_len(bin_t_len);
                    let grid_spec = PreBinnedPatchGridSpec { bin_t_len, patch_t_len };
                    let pl = patch_t_len;
                    let ts1 = range.beg / pl * pl;
                    let ts2 = (range.end + pl - 1) / pl * pl;
                    let count = (ts2 - ts1) / pl;
                    let offset = ts1 / pl;
                    let ret = Self {
                        grid_spec,
                        count,
                        offset,
                    };
                    break Ok(Some(ret));
                }
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreBinnedPatchCoord {
    spec: PreBinnedPatchGridSpec,
    ix: u64,
}

impl PreBinnedPatchCoord {
    pub fn bin_t_len(&self) -> u64 {
        self.spec.bin_t_len
    }

    pub fn patch_t_len(&self) -> u64 {
        self.spec.patch_t_len()
    }

    pub fn patch_beg(&self) -> u64 {
        self.spec.patch_t_len() * self.ix
    }

    pub fn patch_end(&self) -> u64 {
        self.spec.patch_t_len() * (self.ix + 1)
    }

    pub fn patch_range(&self) -> NanoRange {
        NanoRange {
            beg: self.patch_beg(),
            end: self.patch_end(),
        }
    }

    pub fn bin_count(&self) -> u64 {
        self.spec.patch_t_len() / self.spec.bin_t_len
    }

    pub fn spec(&self) -> &PreBinnedPatchGridSpec {
        &self.spec
    }

    pub fn ix(&self) -> u64 {
        self.ix
    }

    pub fn to_url_params_strings(&self) -> String {
        format!(
            "patch_t_len={}&bin_t_len={}&patch_ix={}",
            self.spec.patch_t_len(),
            self.spec.bin_t_len(),
            self.ix()
        )
    }

    pub fn new(bin_t_len: u64, patch_t_len: u64, patch_ix: u64) -> Self {
        Self {
            spec: PreBinnedPatchGridSpec::new(bin_t_len, patch_t_len),
            ix: patch_ix,
        }
    }
}

pub struct PreBinnedPatchIterator {
    range: PreBinnedPatchRange,
    #[allow(dead_code)]
    agg_kind: AggKind,
    ix: u64,
}

impl PreBinnedPatchIterator {
    pub fn from_range(range: PreBinnedPatchRange) -> Self {
        Self {
            range,
            agg_kind: AggKind::DimXBins1,
            ix: 0,
        }
    }
}

impl Iterator for PreBinnedPatchIterator {
    type Item = PreBinnedPatchCoord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ix >= self.range.count {
            None
        } else {
            let ret = Self::Item {
                spec: self.range.grid_spec.clone(),
                ix: self.range.offset + self.ix,
            };
            self.ix += 1;
            Some(ret)
        }
    }
}

#[derive(Clone)]
pub struct BinnedGridSpec {
    bin_t_len: u64,
}

impl BinnedGridSpec {
    pub fn new(bin_t_len: u64) -> Self {
        if !Self::is_valid_bin_t_len(bin_t_len) {
            panic!("BinnedGridSpec::new  invalid bin_t_len {}", bin_t_len);
        }
        Self { bin_t_len }
    }

    pub fn bin_t_len(&self) -> u64 {
        self.bin_t_len
    }

    pub fn is_valid_bin_t_len(bin_t_len: u64) -> bool {
        for &j in BIN_T_LEN_OPTIONS.iter() {
            if bin_t_len == j {
                return true;
            }
        }
        return false;
    }
}

impl std::fmt::Debug for BinnedGridSpec {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.bin_t_len < SEC * 90 {
            write!(fmt, "BinnedGridSpec {{ bin_t_len: {:?} ms }}", self.bin_t_len / MS,)
        } else {
            write!(fmt, "BinnedGridSpec {{ bin_t_len: {:?} s }}", self.bin_t_len / SEC,)
        }
    }
}

#[derive(Clone, Debug)]
pub struct BinnedRange {
    pub grid_spec: BinnedGridSpec,
    pub offset: u64,
    pub count: u64,
}

impl BinnedRange {
    pub fn covering_range(range: NanoRange, min_bin_count: u64) -> Result<Option<Self>, Error> {
        if min_bin_count < 1 {
            Err(Error::with_msg("min_bin_count < 1"))?;
        }
        if min_bin_count > 4000 {
            Err(Error::with_msg("min_bin_count > 4000"))?;
        }
        let dt = range.delta();
        if dt > DAY * 14 {
            Err(Error::with_msg("dt > DAY * 14"))?;
        }
        let bs = dt / min_bin_count;
        let mut i1 = BIN_THRESHOLDS.len();
        loop {
            if i1 <= 0 {
                break Ok(None);
            } else {
                i1 -= 1;
                let t = BIN_THRESHOLDS[i1];
                if t <= bs || i1 == 0 {
                    let grid_spec = BinnedGridSpec { bin_t_len: t };
                    let bl = grid_spec.bin_t_len();
                    let ts1 = range.beg / bl * bl;
                    let ts2 = (range.end + bl - 1) / bl * bl;
                    let count = (ts2 - ts1) / bl;
                    let offset = ts1 / bl;
                    let ret = Self {
                        grid_spec,
                        count,
                        offset,
                    };
                    break Ok(Some(ret));
                }
            }
        }
    }
    pub fn get_range(&self, ix: u32) -> NanoRange {
        NanoRange {
            beg: (self.offset + ix as u64) * self.grid_spec.bin_t_len,
            end: (self.offset + ix as u64 + 1) * self.grid_spec.bin_t_len,
        }
    }
    pub fn full_range(&self) -> NanoRange {
        NanoRange {
            beg: (self.offset + 0) * self.grid_spec.bin_t_len,
            end: (self.offset + self.count) * self.grid_spec.bin_t_len,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AggKind {
    DimXBins1,
}

pub fn query_params(q: Option<&str>) -> std::collections::BTreeMap<String, String> {
    let mut map = std::collections::BTreeMap::new();
    match q {
        Some(k) => {
            for par in k.split("&") {
                let mut u = par.split("=");
                if let Some(t1) = u.next() {
                    if let Some(t2) = u.next() {
                        map.insert(t1.into(), t2.into());
                    }
                }
            }
        }
        None => {}
    }
    map
}

pub trait ToNanos {
    fn to_nanos(&self) -> u64;
}

impl<Tz: TimeZone> ToNanos for DateTime<Tz> {
    fn to_nanos(&self) -> u64 {
        self.timestamp() as u64 * timeunits::SEC + self.timestamp_subsec_nanos() as u64
    }
}

pub trait RetStreamExt: Stream {
    fn only_first_error(self) -> OnlyFirstError<Self>
    where
        Self: Sized;
}

pub struct OnlyFirstError<T> {
    inp: T,
    errored: bool,
    completed: bool,
}

impl<T, I, E> Stream for OnlyFirstError<T>
where
    T: Stream<Item = Result<I, E>> + Unpin,
{
    type Item = <T as Stream>::Item;

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
    }
}

impl<T> RetStreamExt for T
where
    T: Stream,
{
    fn only_first_error(self) -> OnlyFirstError<Self> {
        OnlyFirstError {
            inp: self,
            errored: false,
            completed: false,
        }
    }
}

pub mod log {
    #[allow(unused_imports)]
    pub use tracing::{debug, error, event, info, span, trace, warn, Level};
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventDataReadStats {
    pub parsed_bytes: u64,
}

impl EventDataReadStats {
    pub fn new() -> Self {
        Self { parsed_bytes: 0 }
    }
    pub fn trans(&mut self, k: &mut Self) {
        self.parsed_bytes += k.parsed_bytes;
        k.parsed_bytes = 0;
    }
}

#[derive(Clone, Debug)]
pub struct PerfOpts {
    pub inmem_bufcap: usize,
}

#[derive(Clone, Debug)]
pub struct ByteSize(u32);

impl ByteSize {
    pub fn b(b: u32) -> Self {
        Self(b)
    }
    pub fn kb(kb: u32) -> Self {
        Self(1024 * kb)
    }
    pub fn mb(mb: u32) -> Self {
        Self(1024 * 1024 * mb)
    }
    pub fn bytes(&self) -> u32 {
        self.0
    }
}
