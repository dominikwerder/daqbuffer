pub mod api1;
pub mod histo;
pub mod query;
pub mod status;
pub mod streamext;

use chrono::{DateTime, TimeZone, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsVal;
use std::collections::BTreeMap;
use std::fmt;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use std::time::Duration;
use timeunits::*;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};
use url::Url;

pub const APP_JSON: &'static str = "application/json";
pub const APP_JSON_LINES: &'static str = "application/jsonlines";
pub const APP_OCTET: &'static str = "application/octet-stream";
pub const ACCEPT_ALL: &'static str = "*/*";

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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
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
    STRING,
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
            13 => STRING,
            //13 => return Err(Error::with_msg(format!("STRING not supported"))),
            6 => return Err(Error::with_msg(format!("CHARACTER not supported"))),
            _ => return Err(Error::with_msg(format!("unknown dtype code: {:?}", ix))),
        };
        Ok(g)
    }

    pub fn to_bsread_str(&self) -> &'static str {
        use ScalarType::*;
        match self {
            U8 => "uint8",
            U16 => "uint16",
            U32 => "uint32",
            U64 => "uint64",
            I8 => "int8",
            I16 => "int16",
            I32 => "int32",
            I64 => "int64",
            F32 => "float32",
            F64 => "float64",
            BOOL => "bool",
            STRING => "string",
        }
    }

    pub fn from_bsread_str(s: &str) -> Result<Self, Error> {
        use ScalarType::*;
        let ret = match s {
            "uint8" => U8,
            "uint16" => U16,
            "uint32" => U32,
            "uint64" => U64,
            "int8" => I8,
            "int16" => I16,
            "int32" => I32,
            "int64" => I64,
            "float" => F32,
            "double" => F64,
            "float32" => F32,
            "float64" => F64,
            "string" => STRING,
            "bool" => BOOL,
            _ => {
                return Err(Error::with_msg_no_trace(format!(
                    "from_bsread_str can not understand bsread {:?}",
                    s
                )))
            }
        };
        Ok(ret)
    }

    pub fn from_ca_id(k: u16) -> Result<Self, Error> {
        use ScalarType::*;
        let ret = match k {
            0 => STRING,
            1 => I16,
            2 => F32,
            3 => I16,
            4 => I8,
            5 => I32,
            6 => F64,
            _ => {
                return Err(Error::with_msg_no_trace(format!(
                    "from_ca_id can not understand {:?}",
                    k
                )))
            }
        };
        Ok(ret)
    }

    pub fn from_archeng_db_str(s: &str) -> Result<Self, Error> {
        use ScalarType::*;
        let ret = match s {
            "I8" => I8,
            "I16" => I16,
            "I32" => I32,
            "I64" => I64,
            "F32" => F32,
            "F64" => F64,
            _ => {
                return Err(Error::with_msg_no_trace(format!(
                    "from_archeng_db_str can not understand {:?}",
                    s
                )))
            }
        };
        Ok(ret)
    }

    pub fn from_scylla_i32(k: i32) -> Result<Self, Error> {
        if k < 0 || k > u8::MAX as i32 {
            return Err(Error::with_public_msg_no_trace(format!("bad scalar type index {k}")));
        }
        Self::from_dtype_index(k as u8)
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
            STRING => 0,
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
            STRING => 13,
        }
    }

    pub fn to_api3proto(&self) -> &'static str {
        match self {
            ScalarType::U8 => "uint8",
            ScalarType::U16 => "uint16",
            ScalarType::U32 => "uint32",
            ScalarType::U64 => "uint64",
            ScalarType::I8 => "int8",
            ScalarType::I16 => "int16",
            ScalarType::I32 => "int32",
            ScalarType::I64 => "int64",
            ScalarType::F32 => "float32",
            ScalarType::F64 => "float64",
            ScalarType::BOOL => "bool",
            ScalarType::STRING => "string",
        }
    }

    pub fn to_scylla_i32(&self) -> i32 {
        self.index() as i32
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SfDatabuffer {
    pub data_base_path: PathBuf,
    pub ksprefix: String,
    pub splits: Option<Vec<u64>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArchiverAppliance {
    pub data_base_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelArchiver {
    pub data_base_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub host: String,
    // TODO for `listen` and the ports, would be great to allow a default on Cluster level.
    pub listen: String,
    #[serde(deserialize_with = "port_from_any")]
    pub port: u16,
    #[serde(deserialize_with = "port_from_any")]
    pub port_raw: u16,
    pub cache_base_path: PathBuf,
    pub sf_databuffer: Option<SfDatabuffer>,
    pub archiver_appliance: Option<ArchiverAppliance>,
    pub channel_archiver: Option<ChannelArchiver>,
    #[serde(default)]
    pub access_scylla: bool,
}

struct Visit1 {}

impl<'de> serde::de::Visitor<'de> for Visit1 {
    type Value = u16;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "a tcp port number, in numeric or string form.")
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v > u16::MAX as u64 {
            Err(serde::de::Error::invalid_type(
                serde::de::Unexpected::Unsigned(v),
                &self,
            ))
        } else {
            self.visit_i64(v as i64)
        }
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v < 1 || v > u16::MAX as i64 {
            Err(serde::de::Error::invalid_type(serde::de::Unexpected::Signed(v), &self))
        } else {
            Ok(v as u16)
        }
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match v.parse::<u16>() {
            Err(_) => Err(serde::de::Error::invalid_type(serde::de::Unexpected::Str(v), &self)),
            Ok(v) => Ok(v),
        }
    }
}

fn port_from_any<'de, D>(de: D) -> Result<u16, D::Error>
where
    D: serde::Deserializer<'de>,
{
    de.deserialize_any(Visit1 {})
}

impl Node {
    // TODO needed? Could `sf_databuffer` be None?
    pub fn dummy() -> Self {
        Self {
            host: "dummy".into(),
            listen: "dummy".into(),
            port: 4444,
            port_raw: 4444,
            cache_base_path: PathBuf::new(),
            sf_databuffer: Some(SfDatabuffer {
                data_base_path: PathBuf::new(),
                ksprefix: "daqlocal".into(),
                splits: None,
            }),
            archiver_appliance: None,
            channel_archiver: None,
            access_scylla: false,
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
pub struct ScyllaConfig {
    pub hosts: Vec<String>,
    pub keyspace: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cluster {
    pub backend: String,
    pub nodes: Vec<Node>,
    pub database: Database,
    #[serde(rename = "runMapPulse", default)]
    pub run_map_pulse_task: bool,
    #[serde(rename = "isCentralStorage", default)]
    pub is_central_storage: bool,
    #[serde(rename = "fileIoBufferSize", default)]
    pub file_io_buffer_size: FileIoBufferSize,
    pub scylla: Option<ScyllaConfig>,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStatusArchiverAppliance {
    pub readable: Vec<(PathBuf, bool)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    //pub node: NodeConfig,
    pub is_sf_databuffer: bool,
    pub is_archiver_engine: bool,
    pub is_archiver_appliance: bool,
    pub database_size: Result<u64, String>,
    pub archiver_appliance_status: Option<NodeStatusArchiverAppliance>,
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct FilePos {
    pub pos: u64,
}

impl From<FilePos> for u64 {
    fn from(k: FilePos) -> Self {
        k.pos
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TimeRange {
    Time { beg: DateTime<Utc>, end: DateTime<Utc> },
    Pulse { beg: u64, end: u64 },
    Nano { beg: u64, end: u64 },
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Nanos {
    pub ns: u64,
}

impl Nanos {
    pub fn from_ns(ns: u64) -> Self {
        Self { ns }
    }
}

impl fmt::Debug for Nanos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ts = chrono::Utc.timestamp((self.ns / SEC) as i64, (self.ns % SEC) as u32);
        f.debug_struct("Nanos").field("ns", &ts).finish()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NanoRange {
    pub beg: u64,
    pub end: u64,
}

impl fmt::Debug for NanoRange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let beg = chrono::Utc.timestamp((self.beg / SEC) as i64, (self.beg % SEC) as u32);
        let end = chrono::Utc.timestamp((self.end / SEC) as i64, (self.end % SEC) as u32);
        f.debug_struct("NanoRange")
            .field("beg", &beg)
            .field("end", &end)
            .finish()
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

    pub fn from_bsread_str(s: &str) -> Result<ByteOrder, Error> {
        match s {
            "little" => Ok(ByteOrder::LE),
            "big" => Ok(ByteOrder::BE),
            _ => Err(Error::with_msg_no_trace(format!(
                "ByteOrder::from_bsread_str can not understand {}",
                s
            ))),
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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub enum Shape {
    Scalar,
    Wave(u32),
    Image(u32, u32),
}

impl Shape {
    pub fn from_bsread_jsval(v: &JsVal) -> Result<Shape, Error> {
        match v {
            JsVal::Array(v) => match v.len() {
                0 => Ok(Shape::Scalar),
                1 => match &v[0] {
                    JsVal::Number(v) => match v.as_u64() {
                        Some(0) | Some(1) => Ok(Shape::Scalar),
                        Some(v) => Ok(Shape::Wave(v as u32)),
                        None => Err(Error::with_msg_no_trace(format!(
                            "Shape from_bsread_jsval can not understand {:?}",
                            v
                        ))),
                    },
                    _ => Err(Error::with_msg_no_trace(format!(
                        "Shape from_bsread_jsval can not understand {:?}",
                        v
                    ))),
                },
                _ => Err(Error::with_msg_no_trace(format!(
                    "Shape from_bsread_jsval can not understand {:?}",
                    v
                ))),
            },
            _ => Err(Error::with_msg_no_trace(format!(
                "Shape from_bsread_jsval can not understand {:?}",
                v
            ))),
        }
    }

    // TODO use simply a list to represent all shapes: empty, or with 1 or 2 entries.
    pub fn from_db_jsval(v: &JsVal) -> Result<Shape, Error> {
        match v {
            JsVal::String(s) => {
                if s == "Scalar" {
                    Ok(Shape::Scalar)
                } else {
                    Err(Error::with_msg_no_trace(format!(
                        "Shape from_db_jsval can not understand {:?}",
                        v
                    )))
                }
            }
            JsVal::Object(j) => match j.get("Wave") {
                Some(JsVal::Number(j)) => Ok(Shape::Wave(j.as_u64().ok_or_else(|| {
                    Error::with_msg_no_trace(format!("Shape from_db_jsval can not understand {:?}", v))
                })? as u32)),
                _ => Err(Error::with_msg_no_trace(format!(
                    "Shape from_db_jsval can not understand {:?}",
                    v
                ))),
            },
            _ => Err(Error::with_msg_no_trace(format!(
                "Shape from_db_jsval can not understand {:?}",
                v
            ))),
        }
    }

    pub fn from_dims_str(s: &str) -> Result<Self, Error> {
        let a: Vec<u32> = serde_json::from_str(s)?;
        if a.len() == 0 {
            Ok(Shape::Scalar)
        } else if a.len() == 1 {
            Ok(Shape::Wave(a[0]))
        } else if a.len() == 2 {
            Ok(Shape::Image(a[0], a[1]))
        } else {
            Err(Error::with_public_msg_no_trace("only scalar, 1d and 2d supported"))
        }
    }

    pub fn from_scylla_shape_dims(v: &[i32]) -> Result<Self, Error> {
        let res = if v.len() == 0 {
            Shape::Scalar
        } else if v.len() == 1 {
            Shape::Wave(v[0] as u32)
        } else if v.len() == 2 {
            Shape::Image(v[0] as u32, v[1] as u32)
        } else {
            return Err(Error::with_public_msg_no_trace(format!("bad shape_dims {v:?}")));
        };
        Ok(res)
    }

    pub fn from_ca_count(k: u16) -> Result<Self, Error> {
        if k == 0 {
            Err(Error::with_public_msg_no_trace(format!(
                "zero sized ca data count {k:?}"
            )))
        } else if k == 1 {
            Ok(Shape::Scalar)
        } else if k <= 2048 {
            Ok(Shape::Wave(k as u32))
        } else {
            Err(Error::with_public_msg_no_trace(format!(
                "too large ca data count {k:?}"
            )))
        }
    }

    pub fn to_scylla_vec(&self) -> Vec<i32> {
        use Shape::*;
        match self {
            Scalar => vec![],
            Wave(n) => vec![*n as i32],
            Image(n, m) => vec![*n as i32, *m as i32],
        }
    }
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

//const BIN_T_LEN_OPTIONS_0: [u64; 4] = [SEC, MIN * 10, HOUR * 2, DAY];

//const PATCH_T_LEN_KEY: [u64; 4] = [SEC, MIN * 10, HOUR * 2, DAY];

//const PATCH_T_LEN_OPTIONS_SCALAR: [u64; 4] = [MIN * 60, HOUR * 4, DAY * 4, DAY * 32];
// Maybe alternative for GLS:
//const PATCH_T_LEN_OPTIONS_SCALAR: [u64; 4] = [HOUR * 4, DAY * 4, DAY * 16, DAY * 32];

//const PATCH_T_LEN_OPTIONS_WAVE: [u64; 4] = [MIN * 10, HOUR * 2, DAY * 4, DAY * 32];

const BIN_T_LEN_OPTIONS_0: [u64; 2] = [
    //
    //SEC,
    //MIN * 10,
    HOUR * 2,
    DAY,
];

const PATCH_T_LEN_KEY: [u64; 2] = [
    //
    //SEC,
    //MIN * 10,
    HOUR * 2,
    DAY,
];
const PATCH_T_LEN_OPTIONS_SCALAR: [u64; 2] = [
    //
    //MIN * 60,
    //HOUR * 4,
    DAY * 8,
    DAY * 32,
];
const PATCH_T_LEN_OPTIONS_WAVE: [u64; 2] = [
    //
    //MIN * 10,
    //HOUR * 2,
    DAY * 8,
    DAY * 32,
];

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
                    if !PreBinnedPatchGridSpec::is_valid_bin_t_len(bin_t_len) {
                        return Err(Error::with_msg_no_trace(format!("not a valid bin_t_len {}", bin_t_len)));
                    }
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
    Stats1,
}

impl AggKind {
    pub fn do_time_weighted(&self) -> bool {
        match self {
            Self::EventBlobs => false,
            Self::TimeWeightedScalar => true,
            Self::DimXBins1 => false,
            Self::DimXBinsN(_) => false,
            Self::Plain => false,
            Self::Stats1 => false,
        }
    }

    pub fn need_expand(&self) -> bool {
        match self {
            Self::EventBlobs => false,
            Self::TimeWeightedScalar => true,
            Self::DimXBins1 => false,
            Self::DimXBinsN(_) => false,
            Self::Plain => false,
            Self::Stats1 => false,
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
        AggKind::Stats1 => 0,
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
            Self::Stats1 => {
                write!(fmt, "Stats1")
            }
        }
    }
}

impl fmt::Debug for AggKind {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
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
        } else if s == "Stats1" {
            Ok(AggKind::Stats1)
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RangeFilterStats {
    pub events_pre: u64,
    pub events_post: u64,
    pub events_unordered: u64,
}

impl RangeFilterStats {
    pub fn new() -> Self {
        Self {
            events_pre: 0,
            events_post: 0,
            events_unordered: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DiskStats {
    OpenStats(OpenStats),
    SeekStats(SeekStats),
    ReadStats(ReadStats),
    ReadExactStats(ReadExactStats),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenStats {
    pub duration: Duration,
}

impl OpenStats {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeekStats {
    pub duration: Duration,
}

impl SeekStats {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadStats {
    pub duration: Duration,
}

impl ReadStats {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadExactStats {
    pub duration: Duration,
}

impl ReadExactStats {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReadSys {
    TokioAsyncRead,
    Read2,
    Read3,
    Read4,
}

impl ReadSys {
    pub fn default() -> Self {
        Self::TokioAsyncRead
    }
}

impl From<&str> for ReadSys {
    fn from(k: &str) -> Self {
        if k == "TokioAsyncRead" {
            Self::TokioAsyncRead
        } else if k == "Read2" {
            Self::Read2
        } else if k == "Read3" {
            Self::Read3
        } else if k == "Read4" {
            Self::Read4
        } else {
            Self::default()
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiskIoTune {
    pub read_sys: ReadSys,
    pub read_buffer_len: usize,
    pub read_queue_len: usize,
}

impl DiskIoTune {
    pub fn default_for_testing() -> Self {
        Self {
            read_sys: ReadSys::default(),
            read_buffer_len: 1024 * 4,
            read_queue_len: 4,
        }
    }
    pub fn default() -> Self {
        Self {
            read_sys: ReadSys::default(),
            read_buffer_len: 1024 * 4,
            read_queue_len: 4,
        }
    }
}

pub fn channel_from_pairs(pairs: &BTreeMap<String, String>) -> Result<Channel, Error> {
    let ret = Channel {
        backend: pairs
            .get("channelBackend")
            .ok_or(Error::with_public_msg("missing channelBackend"))?
            .into(),
        name: pairs
            .get("channelName")
            .ok_or(Error::with_public_msg("missing channelName"))?
            .into(),
    };
    Ok(ret)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelSearchQuery {
    pub backend: Option<String>,
    pub name_regex: String,
    pub source_regex: String,
    pub description_regex: String,
}

impl ChannelSearchQuery {
    pub fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        let ret = Self {
            backend: pairs.get("backend").map(Into::into),
            name_regex: pairs.get("nameRegex").map_or(String::new(), |k| k.clone()),
            source_regex: pairs.get("sourceRegex").map_or(String::new(), |k| k.clone()),
            description_regex: pairs.get("descriptionRegex").map_or(String::new(), |k| k.clone()),
        };
        Ok(ret)
    }

    pub fn append_to_url(&self, url: &mut Url) {
        let mut qp = url.query_pairs_mut();
        if let Some(v) = &self.backend {
            qp.append_pair("backend", v);
        }
        qp.append_pair("nameRegex", &self.name_regex);
        qp.append_pair("sourceRegex", &self.source_regex);
        qp.append_pair("descriptionRegex", &self.description_regex);
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
    pub name: String,
    pub listen: String,
    pub port: u16,
    #[serde(default)]
    pub backends_status: Vec<ProxyBackend>,
    #[serde(default)]
    pub backends: Vec<ProxyBackend>,
    #[serde(default)]
    pub backends_pulse_map: Vec<ProxyBackend>,
    #[serde(default)]
    pub backends_search: Vec<ProxyBackend>,
    #[serde(default)]
    pub backends_event_download: Vec<ProxyBackend>,
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
    pub expand: bool,
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
        let beg_date = pairs.get("begDate").ok_or(Error::with_public_msg("missing begDate"))?;
        let end_date = pairs.get("endDate").ok_or(Error::with_public_msg("missing endDate"))?;
        let expand = pairs.get("expand").map(|s| s == "true").unwrap_or(false);
        let ret = Self {
            channel: channel_from_pairs(&pairs)?,
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            expand,
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
        if self.expand {
            g.append_pair("expand", "true");
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelConfigResponse {
    pub channel: Channel,
    #[serde(rename = "scalarType")]
    pub scalar_type: ScalarType,
    #[serde(rename = "byteOrder")]
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

pub fn f32_close(a: f32, b: f32) -> bool {
    if (a - b).abs() < 1e-5 {
        true
    } else if a / b > 0.9999 && a / b < 1.0001 {
        true
    } else {
        false
    }
}

pub fn f64_close(a: f64, b: f64) -> bool {
    if (a - b).abs() < 1e-5 {
        true
    } else if a / b > 0.9999 && a / b < 1.0001 {
        true
    } else {
        false
    }
}

pub fn test_cluster() -> Cluster {
    let nodes = (0..3)
        .into_iter()
        .map(|id| Node {
            host: "localhost".into(),
            listen: "0.0.0.0".into(),
            port: 6170 + id as u16,
            port_raw: 6170 + id as u16 + 100,
            cache_base_path: test_data_base_path_databuffer().join(format!("node{:02}", id)),
            sf_databuffer: Some(SfDatabuffer {
                data_base_path: test_data_base_path_databuffer().join(format!("node{:02}", id)),
                ksprefix: "ks".into(),
                splits: None,
            }),
            archiver_appliance: None,
            channel_archiver: None,
            access_scylla: false,
        })
        .collect();
    Cluster {
        backend: "testbackend".into(),
        nodes,
        database: Database {
            host: "127.0.0.1".into(),
            name: "testingdaq".into(),
            user: "testingdaq".into(),
            pass: "testingdaq".into(),
        },
        scylla: None,
        run_map_pulse_task: false,
        is_central_storage: false,
        file_io_buffer_size: Default::default(),
    }
}

pub fn sls_test_cluster() -> Cluster {
    let nodes = (0..1)
        .into_iter()
        .map(|id| Node {
            host: "localhost".into(),
            listen: "0.0.0.0".into(),
            port: 6190 + id as u16,
            port_raw: 6190 + id as u16 + 100,
            cache_base_path: test_data_base_path_databuffer().join(format!("node{:02}", id)),
            sf_databuffer: None,
            archiver_appliance: None,
            channel_archiver: Some(ChannelArchiver {
                data_base_paths: vec![test_data_base_path_channel_archiver_sls()],
            }),
            access_scylla: false,
        })
        .collect();
    Cluster {
        backend: "sls-archive".into(),
        nodes,
        database: Database {
            host: "127.0.0.1".into(),
            name: "testingdaq".into(),
            user: "testingdaq".into(),
            pass: "testingdaq".into(),
        },
        scylla: None,
        run_map_pulse_task: false,
        is_central_storage: false,
        file_io_buffer_size: Default::default(),
    }
}

pub fn archapp_test_cluster() -> Cluster {
    let nodes = (0..1)
        .into_iter()
        .map(|id| Node {
            host: "localhost".into(),
            listen: "0.0.0.0".into(),
            port: 6200 + id as u16,
            port_raw: 6200 + id as u16 + 100,
            cache_base_path: test_data_base_path_databuffer().join(format!("node{:02}", id)),
            sf_databuffer: None,
            channel_archiver: None,
            archiver_appliance: Some(ArchiverAppliance {
                data_base_paths: vec![test_data_base_path_archiver_appliance()],
            }),
            access_scylla: false,
        })
        .collect();
    Cluster {
        backend: "sf-archive".into(),
        nodes,
        database: Database {
            host: "127.0.0.1".into(),
            name: "testingdaq".into(),
            user: "testingdaq".into(),
            pass: "testingdaq".into(),
        },
        scylla: None,
        run_map_pulse_task: false,
        is_central_storage: false,
        file_io_buffer_size: Default::default(),
    }
}

pub fn test_data_base_path_databuffer() -> PathBuf {
    let homedir = std::env::var("HOME").unwrap();
    let data_base_path = PathBuf::from(homedir).join("daqbuffer-testdata").join("databuffer");
    data_base_path
}

pub fn test_data_base_path_channel_archiver_sls() -> PathBuf {
    let homedir = std::env::var("HOME").unwrap();
    let data_base_path = PathBuf::from(homedir)
        .join("daqbuffer-testdata")
        .join("sls")
        .join("gfa03");
    data_base_path
}

pub fn test_data_base_path_archiver_appliance() -> PathBuf {
    let homedir = std::env::var("HOME").unwrap();
    let data_base_path = PathBuf::from(homedir)
        .join("daqbuffer-testdata")
        .join("archappdata")
        .join("lts")
        .join("ArchiverStore");
    data_base_path
}
