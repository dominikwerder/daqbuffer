use chrono::{DateTime, TimeZone, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use timeunits::*;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

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
    pub fn from_dtype_index(ix: u8) -> Self {
        use ScalarType::*;
        match ix {
            0 => panic!("BOOL not supported"),
            1 => panic!("BOOL8 not supported"),
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
            6 => panic!("CHARACTER not supported"),
            13 => panic!("STRING not supported"),
            _ => panic!("unknown"),
        }
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

#[derive(Debug)]
pub struct Node {
    pub id: u32,
    pub host: String,
    pub listen: String,
    pub port: u16,
    pub port_raw: u16,
    pub split: u32,
    pub data_base_path: PathBuf,
    pub ksprefix: String,
}

impl Node {
    pub fn name(&self) -> String {
        format!("{}-{}", self.host, self.port)
    }
}

#[derive(Debug)]
pub struct Cluster {
    pub nodes: Vec<Arc<Node>>,
}

#[derive(Debug)]
pub struct NodeConfig {
    pub node: Arc<Node>,
    pub cluster: Arc<Cluster>,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NanoRange {
    pub beg: u64,
    pub end: u64,
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
    pub time_bin_size: u64,
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
    pub const WEEK: u64 = DAY * 7;
}

pub struct BinSpecDimT {
    pub count: u64,
    pub ts1: u64,
    pub ts2: u64,
    pub bs: u64,
}

impl BinSpecDimT {
    pub fn over_range(count: u64, ts1: u64, ts2: u64) -> Self {
        use timeunits::*;
        assert!(count >= 1);
        assert!(count <= 2000);
        assert!(ts2 > ts1);
        let dt = ts2 - ts1;
        assert!(dt <= DAY * 14);
        let bs = dt / count;
        let mut i1 = 0;
        let bs = loop {
            if i1 >= BIN_THRESHOLDS.len() {
                break *BIN_THRESHOLDS.last().unwrap();
            }
            let t = BIN_THRESHOLDS[i1];
            if bs <= t {
                break t;
            }
            i1 += 1;
        };
        //info!("INPUT TS  {}  {}", ts1, ts2);
        //info!("chosen binsize: {}  {}", i1, bs);
        let ts1 = ts1 / bs * bs;
        let ts2 = (ts2 + bs - 1) / bs * bs;
        //info!("ADJUSTED TS  {}  {}", ts1, ts2);
        BinSpecDimT {
            count: (ts2 - ts1) / bs,
            ts1,
            ts2,
            bs,
        }
    }

    pub fn get_range(&self, ix: u32) -> NanoRange {
        NanoRange {
            beg: self.ts1 + ix as u64 * self.bs,
            end: self.ts1 + (ix as u64 + 1) * self.bs,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PreBinnedPatchGridSpec {
    bin_t_len: u64,
}

impl PreBinnedPatchGridSpec {
    pub fn new(bin_t_len: u64) -> Self {
        let mut ok = false;
        for &j in PATCH_T_LEN_OPTIONS.iter() {
            if bin_t_len == j {
                ok = true;
                break;
            }
        }
        if !ok {
            panic!("invalid bin_t_len for PreBinnedPatchGridSpec  {}", bin_t_len);
        }
        Self { bin_t_len }
    }

    pub fn from_query_params(params: &BTreeMap<String, String>) -> Self {
        let bin_t_len = params.get("bin_t_len").unwrap().parse().unwrap();
        if !Self::is_valid_bin_t_len(bin_t_len) {
            panic!("invalid bin_t_len {}", bin_t_len);
        }
        Self { bin_t_len: bin_t_len }
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
        for (i1, &j) in BIN_T_LEN_OPTIONS.iter().enumerate() {
            if self.bin_t_len == j {
                return PATCH_T_LEN_OPTIONS[i1];
            }
        }
        panic!()
    }
}

const BIN_T_LEN_OPTIONS: [u64; 6] = [SEC * 10, MIN * 10, HOUR, HOUR * 4, DAY, DAY * 4];

const PATCH_T_LEN_OPTIONS: [u64; 6] = [MIN * 10, HOUR, HOUR * 4, DAY, DAY * 4, DAY * 12];

const BIN_THRESHOLDS: [u64; 33] = [
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
    WEEK,
    WEEK * 2,
    WEEK * 10,
    WEEK * 60,
];

#[derive(Clone, Debug)]
pub struct PreBinnedPatchRange {
    pub grid_spec: PreBinnedPatchGridSpec,
    pub offset: u64,
    pub count: u64,
}

impl PreBinnedPatchRange {
    pub fn covering_range(range: NanoRange, min_bin_count: u64) -> Option<Self> {
        assert!(min_bin_count >= 1);
        assert!(min_bin_count <= 2000);
        let dt = range.delta();
        assert!(dt <= DAY * 14);
        let bs = dt / min_bin_count;
        //info!("BASIC bs {}", bs);
        let mut i1 = BIN_T_LEN_OPTIONS.len();
        loop {
            if i1 <= 0 {
                break None;
            } else {
                i1 -= 1;
                let t = BIN_T_LEN_OPTIONS[i1];
                //info!("look at threshold {}  bs {}", t, bs);
                if t <= bs {
                    let bs = t;
                    let ts1 = range.beg / bs * bs;
                    let _ts2 = (range.end + bs - 1) / bs * bs;
                    let count = range.delta() / bs;
                    let offset = ts1 / bs;
                    break Some(Self {
                        grid_spec: PreBinnedPatchGridSpec { bin_t_len: bs },
                        count,
                        offset,
                    });
                } else {
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
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

    pub fn bin_count(&self) -> u64 {
        self.patch_t_len() / self.spec.bin_t_len
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

    pub fn from_query_params(params: &BTreeMap<String, String>) -> Self {
        let patch_ix = params.get("patch_ix").unwrap().parse().unwrap();
        Self {
            spec: PreBinnedPatchGridSpec::from_query_params(params),
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
