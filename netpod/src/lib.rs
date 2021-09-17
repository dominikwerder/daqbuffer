pub mod api1;
pub mod histo;
pub mod query;
pub mod status;
pub mod streamext;

use std::collections::BTreeMap;
use std::fmt;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use futures_core::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};
use url::Url;

use err::Error;
use timeunits::*;

pub const APP_JSON: &'static str = "application/json";
pub const APP_JSON_LINES: &'static str = "application/jsonlines";
pub const APP_OCTET: &'static str = "application/octet-stream";

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
    BOOL,
}

pub trait HasScalarType {
    fn scalar_type(&self) -> ScalarType;
}

impl ScalarType {
    pub fn from_dtype_index(ix: u8) -> Result<Self, Error> {
        use ScalarType::*;
        let g = match ix {
            0 => BOOL,
            1 => BOOL,
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
            BOOL => 1,
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
            BOOL => 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArchiverAppliance {
    pub data_base_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub host: String,
    pub listen: String,
    pub port: u16,
    pub port_raw: u16,
    pub data_base_path: PathBuf,
    pub cache_base_path: PathBuf,
    pub ksprefix: String,
    pub backend: String,
    pub archiver_appliance: Option<ArchiverAppliance>,
}

impl Node {
    pub fn dummy() -> Self {
        Self {
            host: "dummy".into(),
            listen: "dummy".into(),
            port: 4444,
            port_raw: 4444,
            data_base_path: PathBuf::new(),
            cache_base_path: PathBuf::new(),
            ksprefix: "daqlocal".into(),
            backend: "dummybackend".into(),
            archiver_appliance: None,
        }
    }
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
    #[serde(rename = "runMapPulse", default)]
    pub run_map_pulse_task: bool,
    #[serde(rename = "isCentralStorage", default)]
    pub is_central_storage: bool,
    #[serde(rename = "fileIoBufferSize", default)]
    pub file_io_buffer_size: FileIoBufferSize,
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

pub struct HostPort {
    pub host: String,
    pub port: u16,
}

impl HostPort {
    pub fn new<S: Into<String>>(host: S, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    pub fn from_node(node: &Node) -> Self {
        Self {
            host: node.host.clone(),
            port: node.port,
        }
    }
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

impl fmt::Debug for NanoRange {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "NanoRange {{ beg: {}.{:03} s, end: {}.{:03} s }}",
            self.beg / SEC,
            (self.beg % SEC) / MS,
            self.end / SEC,
            (self.end % SEC) / MS,
        )
    }
}

impl NanoRange {
    pub fn from_date_time(beg: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            beg: beg.timestamp_nanos() as u64,
            end: end.timestamp_nanos() as u64,
        }
    }

    pub fn delta(&self) -> u64 {
        self.end - self.beg
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ByteOrder {
    LE,
    BE,
}

impl ByteOrder {
    pub fn little_endian() -> Self {
        Self::LE
    }

    pub fn big_endian() -> Self {
        Self::BE
    }

    pub fn from_dtype_flags(flags: u8) -> Self {
        if flags & 0x20 == 0 {
            Self::LE
        } else {
            Self::BE
        }
    }

    pub fn is_le(&self) -> bool {
        if let Self::LE = self {
            true
        } else {
            false
        }
    }

    pub fn is_be(&self) -> bool {
        if let Self::BE = self {
            true
        } else {
            false
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GenVar {
    Default,
    TimeWeight,
    ConstRegular,
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
    pub byte_order: ByteOrder,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Shape {
    Scalar,
    Wave(u32),
    Image(u32, u32),
}

pub trait HasShape {
    fn shape(&self) -> Shape;
}

pub mod timeunits {
    pub const MU: u64 = 1000;
    pub const MS: u64 = MU * 1000;
    pub const SEC: u64 = MS * 1000;
    pub const MIN: u64 = SEC * 60;
    pub const HOUR: u64 = MIN * 60;
    pub const DAY: u64 = HOUR * 24;
}

const BIN_T_LEN_OPTIONS_0: [u64; 4] = [SEC, MIN * 10, HOUR * 2, DAY];

const PATCH_T_LEN_KEY: [u64; 4] = [SEC, MIN * 10, HOUR * 2, DAY];

const PATCH_T_LEN_OPTIONS_SCALAR: [u64; 4] = [MIN * 60, HOUR * 4, DAY * 4, DAY * 32];
// Maybe alternative for GLS:
//const PATCH_T_LEN_OPTIONS_SCALAR: [u64; 4] = [HOUR * 4, DAY * 4, DAY * 16, DAY * 32];

const PATCH_T_LEN_OPTIONS_WAVE: [u64; 4] = [MIN * 10, HOUR * 2, DAY * 4, DAY * 32];

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
        for &j in PATCH_T_LEN_KEY.iter() {
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
            for (i1, &j) in PATCH_T_LEN_KEY.iter().enumerate() {
                if bin_t_len == j {
                    return PATCH_T_LEN_OPTIONS_SCALAR[i1];
                }
            }
        }
        Shape::Wave(..) => {
            for (i1, &j) in PATCH_T_LEN_KEY.iter().enumerate() {
                if bin_t_len == j {
                    return PATCH_T_LEN_OPTIONS_WAVE[i1];
                }
            }
        }
        Shape::Image(..) => {
            for (i1, &j) in PATCH_T_LEN_KEY.iter().enumerate() {
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
    pub fn covering_range(range: NanoRange, min_bin_count: u32) -> Result<Option<Self>, Error> {
        let bin_t_len_options = &BIN_T_LEN_OPTIONS_0;
        if min_bin_count < 1 {
            Err(Error::with_msg("min_bin_count < 1"))?;
        }
        if min_bin_count > 20000 {
            Err(Error::with_msg(format!("min_bin_count > 20000: {}", min_bin_count)))?;
        }
        let dt = range.delta();
        if dt > DAY * 200 {
            Err(Error::with_msg("dt > DAY * 200"))?;
        }
        let bs = dt / min_bin_count as u64;
        let mut i1 = bin_t_len_options.len();
        loop {
            if i1 <= 0 {
                break Ok(None);
            } else {
                i1 -= 1;
                let t = bin_t_len_options[i1];
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

    pub fn bin_count(&self) -> u32 {
        (self.spec.patch_t_len() / self.spec.bin_t_len) as u32
    }

    pub fn spec(&self) -> &PreBinnedPatchGridSpec {
        &self.spec
    }

    pub fn ix(&self) -> u64 {
        self.ix
    }

    pub fn new(bin_t_len: u64, patch_t_len: u64, patch_ix: u64) -> Self {
        Self {
            spec: PreBinnedPatchGridSpec::new(bin_t_len, patch_t_len),
            ix: patch_ix,
        }
    }
}

impl AppendToUrl for PreBinnedPatchCoord {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("patchTlen", &format!("{}", self.spec.patch_t_len()));
        g.append_pair("binTlen", &format!("{}", self.spec.bin_t_len()));
        g.append_pair("patchIx", &format!("{}", self.ix()));
    }
}

pub struct PreBinnedPatchIterator {
    range: PreBinnedPatchRange,
    ix: u64,
}

impl PreBinnedPatchIterator {
    pub fn from_range(range: PreBinnedPatchRange) -> Self {
        Self { range, ix: 0 }
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
        for &j in PATCH_T_LEN_KEY.iter() {
            if bin_t_len == j {
                return true;
            }
        }
        return false;
    }
}

impl fmt::Debug for BinnedGridSpec {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
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
    pub fn covering_range(range: NanoRange, min_bin_count: u32) -> Result<Option<Self>, Error> {
        let thresholds = &BIN_THRESHOLDS;
        if min_bin_count < 1 {
            Err(Error::with_msg("min_bin_count < 1"))?;
        }
        if min_bin_count > 20000 {
            Err(Error::with_msg(format!("min_bin_count > 20000: {}", min_bin_count)))?;
        }
        let dt = range.delta();
        if dt > DAY * 200 {
            Err(Error::with_msg("dt > DAY * 200"))?;
        }
        let bs = dt / min_bin_count as u64;
        let mut i1 = thresholds.len();
        loop {
            if i1 <= 0 {
                break Ok(None);
            } else {
                i1 -= 1;
                let t = thresholds[i1];
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

#[derive(Clone, Serialize, Deserialize)]
pub enum AggKind {
    EventBlobs,
    DimXBins1,
    DimXBinsN(u32),
    Plain,
    TimeWeightedScalar,
}

impl AggKind {
    pub fn do_time_weighted(&self) -> bool {
        match self {
            Self::EventBlobs => false,
            Self::TimeWeightedScalar => true,
            Self::DimXBins1 => false,
            Self::DimXBinsN(_) => false,
            Self::Plain => false,
        }
    }

    pub fn need_expand(&self) -> bool {
        match self {
            Self::EventBlobs => false,
            Self::TimeWeightedScalar => true,
            Self::DimXBins1 => false,
            Self::DimXBinsN(_) => false,
            Self::Plain => false,
        }
    }
}

pub fn x_bin_count(shape: &Shape, agg_kind: &AggKind) -> usize {
    match agg_kind {
        AggKind::EventBlobs => 0,
        AggKind::TimeWeightedScalar => 0,
        AggKind::DimXBins1 => 0,
        AggKind::DimXBinsN(n) => {
            if *n == 0 {
                match shape {
                    Shape::Scalar => 0,
                    Shape::Wave(n) => *n as usize,
                    Shape::Image(j, k) => *j as usize * *k as usize,
                }
            } else {
                *n as usize
            }
        }
        AggKind::Plain => match shape {
            Shape::Scalar => 0,
            Shape::Wave(n) => *n as usize,
            Shape::Image(j, k) => *j as usize * *k as usize,
        },
    }
}

impl fmt::Display for AggKind {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::EventBlobs => {
                write!(fmt, "EventBlobs")
            }
            Self::DimXBins1 => {
                write!(fmt, "DimXBins1")
            }
            Self::DimXBinsN(n) => {
                write!(fmt, "DimXBinsN{}", n)
            }
            Self::Plain => {
                write!(fmt, "Plain")
            }
            Self::TimeWeightedScalar => {
                write!(fmt, "TimeWeightedScalar")
            }
        }
    }
}

impl fmt::Debug for AggKind {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> std::fmt::Result {
        fmt::Display::fmt(self, fmt)
    }
}

impl FromStr for AggKind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let nmark = "DimXBinsN";
        if s == "EventBlobs" {
            Ok(AggKind::EventBlobs)
        } else if s == "DimXBins1" {
            Ok(AggKind::DimXBins1)
        } else if s == "TimeWeightedScalar" {
            Ok(AggKind::TimeWeightedScalar)
        } else if s.starts_with(nmark) {
            let nbins: u32 = s[nmark.len()..].parse()?;
            Ok(AggKind::DimXBinsN(nbins))
        } else {
            Err(Error::with_msg(format!("can not parse {} as AggKind", s)))
        }
    }
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
pub struct ByteSize(pub u32);

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileIoBufferSize(pub usize);

impl FileIoBufferSize {
    pub fn new(k: usize) -> Self {
        Self(k)
    }
    pub fn bytes(&self) -> usize {
        self.0
    }
}

impl Default for FileIoBufferSize {
    fn default() -> Self {
        Self(1024 * 4)
    }
}

pub fn channel_from_pairs(pairs: &BTreeMap<String, String>) -> Result<Channel, Error> {
    let ret = Channel {
        backend: pairs
            .get("channelBackend")
            .ok_or(Error::with_msg("missing channelBackend"))?
            .into(),
        name: pairs
            .get("channelName")
            .ok_or(Error::with_msg("missing channelName"))?
            .into(),
    };
    Ok(ret)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelSearchQuery {
    pub name_regex: String,
    pub source_regex: String,
    pub description_regex: String,
}

impl ChannelSearchQuery {
    pub fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        let ret = Self {
            name_regex: pairs.get("nameRegex").map_or(String::new(), |k| k.clone()),
            source_regex: pairs.get("sourceRegex").map_or(String::new(), |k| k.clone()),
            description_regex: pairs.get("descriptionRegex").map_or(String::new(), |k| k.clone()),
        };
        Ok(ret)
    }

    pub fn append_to_url(&self, url: &mut Url) {
        url.query_pairs_mut().append_pair("nameRegex", &self.name_regex);
        url.query_pairs_mut().append_pair("sourceRegex", &self.source_regex);
        url.query_pairs_mut()
            .append_pair("descriptionRegex", &self.description_regex);
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn parse_url_1() {
        let mut url = url::Url::parse("http://host/123").unwrap();
        url.query_pairs_mut().append_pair("text", "jo jo • yo");
        assert_eq!(url.to_string(), "http://host/123?text=jo+jo+%E2%80%A2+yo");
    }

    #[test]
    fn parse_url_2() {
        let url = url::Url::parse("dummy:?123").unwrap();
        assert_eq!(url.query().unwrap(), "123")
    }
}

#[derive(Serialize, Deserialize)]
pub struct ChannelSearchSingleResult {
    pub backend: String,
    pub name: String,
    pub source: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub shape: Vec<u32>,
    pub unit: String,
    pub description: String,
    #[serde(rename = "isApi0", skip_serializing_if = "Option::is_none")]
    pub is_api_0: Option<bool>,
}

#[derive(Serialize, Deserialize)]
pub struct ChannelSearchResult {
    pub channels: Vec<ChannelSearchSingleResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProxyBackend {
    pub name: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProxyConfig {
    pub listen: String,
    pub port: u16,
    pub search_hosts: Vec<String>,
    pub backends: Vec<ProxyBackend>,
    pub api_0_search_hosts: Option<Vec<String>>,
    pub api_0_search_backends: Option<Vec<String>>,
}

pub trait HasBackend {
    fn backend(&self) -> &str;
}

pub trait HasTimeout {
    fn timeout(&self) -> Duration;
}

pub trait FromUrl: Sized {
    fn from_url(url: &Url) -> Result<Self, Error>;
}

pub trait AppendToUrl {
    fn append_to_url(&self, url: &mut Url);
}

pub fn get_url_query_pairs(url: &Url) -> BTreeMap<String, String> {
    BTreeMap::from_iter(url.query_pairs().map(|(j, k)| (j.to_string(), k.to_string())))
}

/**
Request type of the channel/config api. \
At least on some backends the channel configuration may change depending on the queried range.
Therefore, the query includes the range.
The presence of a configuration in some range does not imply that there is any data available.
*/
#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelConfigQuery {
    pub channel: Channel,
    pub range: NanoRange,
}

impl HasBackend for ChannelConfigQuery {
    fn backend(&self) -> &str {
        &self.channel.backend
    }
}

impl HasTimeout for ChannelConfigQuery {
    fn timeout(&self) -> Duration {
        Duration::from_millis(2000)
    }
}

impl FromUrl for ChannelConfigQuery {
    fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        let beg_date = pairs.get("begDate").ok_or(Error::with_msg("missing begDate"))?;
        let end_date = pairs.get("endDate").ok_or(Error::with_msg("missing endDate"))?;
        let ret = Self {
            channel: channel_from_pairs(&pairs)?,
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
        };
        Ok(ret)
    }
}

impl AppendToUrl for ChannelConfigQuery {
    fn append_to_url(&self, url: &mut Url) {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
        let mut g = url.query_pairs_mut();
        g.append_pair("channelBackend", &self.channel.backend);
        g.append_pair("channelName", &self.channel.name);
        g.append_pair(
            "begDate",
            &Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt).to_string(),
        );
        g.append_pair(
            "endDate",
            &Utc.timestamp_nanos(self.range.end as i64).format(date_fmt).to_string(),
        );
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelConfigResponse {
    pub channel: Channel,
    #[serde(rename = "scalarType")]
    pub scalar_type: ScalarType,
    pub byte_order: Option<ByteOrder>,
    pub shape: Shape,
}

#[derive(Serialize, Deserialize)]
pub struct EventQueryJsonStringFrame(pub String);

/**
Provide basic information about a channel, especially it's shape.
*/
#[derive(Serialize, Deserialize)]
pub struct ChannelInfo {
    pub scalar_type: ScalarType,
    pub byte_order: Option<ByteOrder>,
    pub shape: Shape,
    pub msg: serde_json::Value,
}
