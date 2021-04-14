#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use serde::{Serialize, Deserialize};
use err::Error;
use std::path::PathBuf;
use chrono::{DateTime, Utc, TimeZone};
use std::sync::Arc;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggQuerySingleChannel {
    pub channel_config: ChannelConfig,
    pub timebin: u32,
    pub tb_file_count: u32,
    pub buffer_size: u32,
}

pub struct BodyStream {
    //pub receiver: async_channel::Receiver<Result<bytes::Bytes, Error>>,
    pub inner: Box<dyn futures_core::Stream<Item=Result<bytes::Bytes, Error>> + Send + Unpin>,
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
    pub port: u16,
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
    Time {
        beg: DateTime<Utc>,
        end: DateTime<Utc>,
    },
    Pulse {
        beg: u64,
        end: u64,
    },
    Nano {
        beg: u64,
        end: u64,
    },
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
    pub shape: Shape,
    pub big_endian: bool,
    pub compression: bool,
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
        let thresholds = [
            2, 10, 100,
            1000, 10_000, 100_000,
            MU, MU * 10, MU * 100,
            MS, MS * 10, MS * 100,
            SEC, SEC * 5, SEC * 10, SEC * 20,
            MIN, MIN * 5, MIN * 10, MIN * 20,
            HOUR, HOUR * 2, HOUR * 4, HOUR * 12,
            DAY, DAY * 2, DAY * 4, DAY * 8, DAY * 16,
            WEEK, WEEK * 2, WEEK * 10, WEEK * 60,
        ];
        let mut i1 = 0;
        let bs = loop {
            if i1 >= thresholds.len() {
                break *thresholds.last().unwrap();
            }
            let t = thresholds[i1];
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
    pub bin_t_len: u64,
    pub patch_t_len: u64,
}

impl PreBinnedPatchGridSpec {
}


#[derive(Clone, Debug)]
pub struct PreBinnedPatchRange {
    pub grid_spec: PreBinnedPatchGridSpec,
    pub offset: u64,
    pub count: u64,
}

impl PreBinnedPatchRange {

    pub fn covering_range(range: NanoRange, min_bin_count: u64) -> Option<Self> {
        use timeunits::*;
        assert!(min_bin_count >= 1);
        assert!(min_bin_count <= 2000);
        let dt = range.delta();
        assert!(dt <= DAY * 14);
        let bs = dt / min_bin_count;
        //info!("BASIC bs {}", bs);
        let thresholds = [
            SEC * 10,
            MIN * 10,
            HOUR,
            HOUR * 4,
            DAY,
            DAY * 4,
        ];
        let mut i1 = thresholds.len();
        loop {
            if i1 <= 0 {
                break None;
            }
            else {
                i1 -= 1;
                let t = thresholds[i1];
                //info!("look at threshold {}  bs {}", t, bs);
                if t <= bs {
                    let bs = t;
                    let ts1 = range.beg / bs * bs;
                    let ts2 = (range.end + bs - 1) / bs * bs;
                    let count = range.delta() / bs;
                    let patch_t_len = if i1 >= thresholds.len() - 1 {
                        bs * 8
                    }
                    else {
                        thresholds[i1 + 1] * 8
                    };
                    let offset = ts1 / bs;
                    break Some(Self {
                        grid_spec: PreBinnedPatchGridSpec {
                            bin_t_len: bs,
                            patch_t_len,
                        },
                        count,
                        offset,
                    });
                }
            }
        }
    }

}


#[derive(Clone, Debug)]
pub struct PreBinnedPatchCoord {
    pub range: NanoRange,
}

impl PreBinnedPatchCoord {

    pub fn bs(&self) -> u64 {
        self.range.end - self.range.beg
    }

}

pub struct PreBinnedPatchIterator {
    range: PreBinnedPatchRange,
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
        }
        else {
            let ret = Self::Item {
                range: NanoRange {
                    beg: (self.range.offset + self.ix) * self.range.grid_spec.patch_t_len,
                    end: (self.range.offset + self.ix + 1) * self.range.grid_spec.patch_t_len,
                },
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
        None => {
        }
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
