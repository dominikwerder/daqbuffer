pub mod histo;
pub mod query;
pub mod range;
pub mod status;
pub mod streamext;

pub mod log {
    pub use tracing::{self, debug, error, event, info, span, trace, warn, Level};
}

use crate::log::*;
use bytes::Bytes;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use err::anyhow;
use err::Error;
use err::Res2;
use futures_util::Stream;
use futures_util::StreamExt;
use range::evrange::NanoRange;
use range::evrange::PulseRange;
use range::evrange::SeriesRange;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsVal;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use timeunits::*;
use url::Url;

pub const APP_JSON: &'static str = "application/json";
pub const APP_JSON_LINES: &'static str = "application/jsonlines";
pub const APP_OCTET: &'static str = "application/octet-stream";
pub const ACCEPT_ALL: &'static str = "*/*";

pub const CONNECTION_STATUS_DIV: u64 = timeunits::DAY;
pub const TS_MSP_GRID_UNIT: u64 = timeunits::SEC * 10;
pub const TS_MSP_GRID_SPACING: u64 = 6 * 2;

const TEST_BACKEND: &str = "testbackend-00";

pub fn is_false<T>(x: T) -> bool
where
    T: std::borrow::Borrow<bool>,
{
    *x.borrow() == false
}

pub trait CmpZero {
    fn is_zero(&self) -> bool;
}

impl CmpZero for u32 {
    fn is_zero(&self) -> bool {
        *self == 0
    }
}

pub struct BodyStream {
    //pub receiver: async_channel::Receiver<Result<Bytes, Error>>,
    pub inner: Box<dyn Stream<Item = Result<Bytes, Error>> + Send + Unpin>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
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

impl fmt::Debug for ScalarType {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.to_variant_str())
    }
}

impl fmt::Display for ScalarType {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.to_variant_str())
    }
}

impl Serialize for ScalarType {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S::Error: serde::ser::Error,
    {
        use ScalarType::*;
        match self {
            U8 => ser.serialize_str("u8"),
            U16 => ser.serialize_str("u16"),
            U32 => ser.serialize_str("u32"),
            U64 => ser.serialize_str("u64"),
            I8 => ser.serialize_str("i8"),
            I16 => ser.serialize_str("i16"),
            I32 => ser.serialize_str("i32"),
            I64 => ser.serialize_str("i64"),
            F32 => ser.serialize_str("f32"),
            F64 => ser.serialize_str("f64"),
            BOOL => ser.serialize_str("bool"),
            STRING => ser.serialize_str("string"),
        }
    }
}

struct ScalarTypeVis;

impl<'de> serde::de::Visitor<'de> for ScalarTypeVis {
    type Value = ScalarType;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("a string describing the ScalarType variant")
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        let s = value.to_lowercase();
        let ret = match s.as_str() {
            "u8" => ScalarType::U8,
            "u16" => ScalarType::U16,
            "u32" => ScalarType::U32,
            "u64" => ScalarType::U64,
            "i8" => ScalarType::I8,
            "i16" => ScalarType::I16,
            "i32" => ScalarType::I32,
            "i64" => ScalarType::I64,
            "f32" => ScalarType::F32,
            "f64" => ScalarType::F64,
            "bool" => ScalarType::BOOL,
            "string" => ScalarType::STRING,
            k => return Err(E::custom(format!("can not understand variant {k:?}"))),
        };
        Ok(ret)
    }
}

impl<'de> Deserialize<'de> for ScalarType {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        de.deserialize_str(ScalarTypeVis)
    }
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

    pub fn to_variant_str(&self) -> &'static str {
        use ScalarType::*;
        match self {
            U8 => "u8",
            U16 => "u16",
            U32 => "u32",
            U64 => "u64",
            I8 => "i8",
            I16 => "i16",
            I32 => "i32",
            I64 => "i64",
            F32 => "f32",
            F64 => "f64",
            BOOL => "bool",
            STRING => "string",
        }
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

    pub fn to_scylla_i32(&self) -> i32 {
        self.index() as i32
    }

    pub fn from_url_str(s: &str) -> Result<Self, Error> {
        let ret = serde_json::from_str(&format!("\"{s}\""))?;
        Ok(ret)
    }
}

impl AppendToUrl for ScalarType {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("scalarType", self.to_variant_str());
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
    #[serde(deserialize_with = "serde_port::port_from_any")]
    pub port: u16,
    #[serde(deserialize_with = "serde_port::port_from_any")]
    pub port_raw: u16,
    pub cache_base_path: PathBuf,
    pub sf_databuffer: Option<SfDatabuffer>,
    pub archiver_appliance: Option<ArchiverAppliance>,
    pub channel_archiver: Option<ChannelArchiver>,
    pub prometheus_api_bind: Option<SocketAddr>,
}

mod serde_port {
    use super::*;

    struct Vis;

    impl<'de> serde::de::Visitor<'de> for Vis {
        type Value = u16;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "a tcp port number, in numeric or string form.")
        }

        fn visit_u64<E>(self, val: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if val > u16::MAX as u64 {
                Err(serde::de::Error::invalid_type(
                    serde::de::Unexpected::Unsigned(val),
                    &self,
                ))
            } else {
                self.visit_i64(val as i64)
            }
        }

        fn visit_i64<E>(self, val: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if val < 1 || val > u16::MAX as i64 {
                Err(serde::de::Error::invalid_type(
                    serde::de::Unexpected::Signed(val),
                    &self,
                ))
            } else {
                Ok(val as u16)
            }
        }

        fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            match val.parse::<u16>() {
                Err(_) => Err(serde::de::Error::invalid_type(serde::de::Unexpected::Str(val), &self)),
                Ok(v) => Ok(v),
            }
        }
    }

    pub fn port_from_any<'de, D>(de: D) -> Result<u16, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        de.deserialize_any(Vis)
    }

    #[test]
    fn test_port_from_any() {
        #[derive(Deserialize)]
        struct Conf {
            #[serde(deserialize_with = "port_from_any")]
            port: u16,
        }
        let conf: Conf = serde_json::from_str(r#"{"port":"9192"}"#).unwrap();
        assert_eq!(conf.port, 9192);
        let conf: Conf = serde_json::from_str(r#"{"port":9194}"#).unwrap();
        assert_eq!(conf.port, 9194);
    }
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
            prometheus_api_bind: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    pub host: String,
    pub port: u16,
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
    pub cache_scylla: Option<ScyllaConfig>,
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
pub struct TableSizes {
    pub sizes: Vec<(String, String)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStatusSub {
    pub url: String,
    pub status: Result<NodeStatus, Error>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_sf_databuffer: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_archiver_engine: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_archiver_appliance: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_size: Option<Result<u64, String>>,
    //#[serde(default, skip_serializing_if = "Option::is_none")]
    //pub table_sizes: Option<Result<TableSizes, Error>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archiver_appliance_status: Option<NodeStatusArchiverAppliance>,
    #[serde(default, skip_serializing_if = "VecDeque::is_empty")]
    pub subs: VecDeque<NodeStatusSub>,
}

// Describes a swissfel-databuffer style "channel" which is a time-series with a unique name within a "backend".
// Also the concept of "backend" could be split into "facility" and some optional other identifier
// for cases like e.g. post-mortem, or to differentiate between channel-access and bsread for cases where
// the same channel-name is delivered via different methods.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SfDbChannel {
    series: Option<u64>,
    // "backend" is currently used in the existing systems for multiple purposes:
    // it can indicate the facility (eg. sf-databuffer, hipa, ...) but also
    // some special subsystem (eg. sf-rf-databuffer).
    pub backend: String,
    pub name: String,
}

impl SfDbChannel {
    pub fn from_full<T: Into<String>, U: Into<String>>(backend: T, series: Option<u64>, name: U) -> Self {
        Self {
            backend: backend.into(),
            series,
            name: name.into(),
        }
    }

    pub fn from_name<T: Into<String>, U: Into<String>>(backend: T, name: U) -> Self {
        Self {
            backend: backend.into(),
            series: None,
            name: name.into(),
        }
    }

    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn series(&self) -> Option<u64> {
        self.series
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn set_series(&mut self, series: u64) {
        self.series = Some(series);
    }
}

impl FromUrl for SfDbChannel {
    fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let ret = SfDbChannel {
            backend: pairs
                .get("backend")
                .ok_or(Error::with_public_msg("missing backend"))?
                .into(),
            name: pairs
                .get("channelName")
                //.ok_or(Error::with_public_msg("missing channelName"))?
                .map(String::from)
                .unwrap_or(String::new())
                .into(),
            series: pairs
                .get("seriesId")
                .and_then(|x| x.parse::<u64>().map_or(None, |x| Some(x))),
        };
        if ret.name.is_empty() && ret.series.is_none() {
            return Err(Error::with_public_msg(format!(
                "Missing one of channelName or seriesId parameters."
            )));
        }
        Ok(ret)
    }
}

impl AppendToUrl for SfDbChannel {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("backend", &self.backend);
        if self.name().len() > 0 {
            g.append_pair("channelName", &self.name);
        }
        if let Some(series) = self.series {
            g.append_pair("seriesId", &series.to_string());
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelTyped {
    pub channel: SfDbChannel,
    pub scalar_type: ScalarType,
    pub shape: Shape,
}

impl ChannelTyped {
    pub fn channel(&self) -> &SfDbChannel {
        &self.channel
    }
}

// Describes a Scylla-based "daqbuffer" style time series.
// The tuple `(backend, series)` is supposed to be unique.
// Contains also the name because it is so useful.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaqbufSeries {
    pub series: u64,
    // "backend" is currently used in the existing systems for multiple purposes:
    // it can indicate the facility (eg. sf-databuffer, hipa, ...) but also
    // some special subsystem (eg. sf-rf-databuffer).
    pub backend: String,
    // This name is only for better user-facing messages. The (backend, series-id) is the identifier.
    pub name: String,
}

impl DaqbufSeries {
    pub fn series(&self) -> u64 {
        self.series
    }

    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl FromUrl for DaqbufSeries {
    fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let ret = DaqbufSeries {
            series: pairs
                .get("seriesId")
                .ok_or_else(|| Error::with_public_msg("missing seriesId"))
                .map(|x| x.parse::<u64>())??,
            backend: pairs
                .get("backend")
                .ok_or_else(|| Error::with_public_msg("missing backend"))?
                .into(),
            name: pairs
                .get("channelName")
                .map(String::from)
                .unwrap_or(String::new())
                .into(),
        };
        Ok(ret)
    }
}

impl AppendToUrl for DaqbufSeries {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("backend", &self.backend);
        if self.name().len() > 0 {
            g.append_pair("channelName", &self.name);
        }
        if let series = self.series {
            g.append_pair("seriesId", &series.to_string());
        }
    }
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
pub enum ByteOrder {
    Little,
    Big,
}

impl ByteOrder {
    pub fn from_dtype_flags(flags: u8) -> Self {
        if flags & 0x20 == 0 {
            Self::Little
        } else {
            Self::Big
        }
    }

    pub fn from_bsread_str(s: &str) -> Result<ByteOrder, Error> {
        match s {
            "little" => Ok(ByteOrder::Little),
            "big" => Ok(ByteOrder::Big),
            _ => Err(Error::with_msg_no_trace(format!(
                "ByteOrder::from_bsread_str can not understand {}",
                s
            ))),
        }
    }

    pub fn is_le(&self) -> bool {
        if let Self::Little = self {
            true
        } else {
            false
        }
    }

    pub fn is_be(&self) -> bool {
        if let Self::Big = self {
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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub enum ShapeOld {
    Scalar,
    Wave(u32),
    Image(u32, u32),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Shape {
    Scalar,
    Wave(u32),
    Image(u32, u32),
}

mod serde_shape {
    use super::*;

    impl Serialize for Shape {
        fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error>
        where
            S::Error: serde::ser::Error,
        {
            use Shape::*;
            match self {
                Scalar => ser.collect_seq([0u32; 0].iter()),
                Wave(a) => ser.collect_seq([*a].iter()),
                Image(a, b) => ser.collect_seq([*a, *b].iter()),
            }
        }
    }

    struct ShapeVis;

    impl<'de> serde::de::Visitor<'de> for ShapeVis {
        type Value = Shape;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            fmt.write_str("a vector describing the shape")
        }

        // TODO unused, do not support deser from any for Shape
        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if v == "Scalar" {
                Ok(Shape::Scalar)
            } else {
                Err(E::custom(format!("unexpected value: {v:?}")))
            }
        }

        // TODO unused, do not support deser from any for Shape
        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            use serde::de::Error;
            if let Some(key) = map.next_key::<String>()? {
                if key == "Wave" {
                    let n: u32 = map.next_value()?;
                    Ok(Shape::Wave(n))
                } else if key == "Image" {
                    let a = map.next_value::<[u32; 2]>()?;
                    Ok(Shape::Image(a[0], a[1]))
                } else {
                    Err(A::Error::custom(format!("unexpected key {key:?}")))
                }
            } else {
                Err(A::Error::custom(format!("invalid shape format")))
            }
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut a = vec![];
            while let Some(item) = seq.next_element()? {
                let n: u32 = item;
                a.push(n);
            }
            if a.len() == 0 {
                Ok(Shape::Scalar)
            } else if a.len() == 1 {
                Ok(Shape::Wave(a[0]))
            } else if a.len() == 2 {
                Ok(Shape::Image(a[0], a[1]))
            } else {
                use serde::de::Error;
                Err(A::Error::custom(format!("bad shape")))
            }
        }
    }

    impl<'de> Deserialize<'de> for Shape {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let res = de.deserialize_seq(ShapeVis);
            res
        }
    }
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
        } else if k <= 1024 * 32 {
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
            Scalar => Vec::new(),
            Wave(n) => vec![*n as i32],
            Image(n, m) => vec![*n as i32, *m as i32],
        }
    }

    pub fn from_url_str(s: &str) -> Result<Self, Error> {
        let ret = serde_json::from_str(s)?;
        Ok(ret)
    }
}

impl AppendToUrl for Shape {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("shape", &format!("{:?}", self.to_scylla_vec()));
    }
}

#[test]
fn test_shape_serde() {
    let s = serde_json::to_string(&Shape::Image(42, 43)).unwrap();
    assert_eq!(s, r#"[42,43]"#);
    let s = serde_json::to_string(&ShapeOld::Scalar).unwrap();
    assert_eq!(s, r#""Scalar""#);
    let s = serde_json::to_string(&ShapeOld::Wave(8)).unwrap();
    assert_eq!(s, r#"{"Wave":8}"#);
    let s = serde_json::to_string(&ShapeOld::Image(42, 43)).unwrap();
    assert_eq!(s, r#"{"Image":[42,43]}"#);
    let s: ShapeOld = serde_json::from_str(r#""Scalar""#).unwrap();
    assert_eq!(s, ShapeOld::Scalar);
    let s: ShapeOld = serde_json::from_str(r#"{"Wave": 123}"#).unwrap();
    assert_eq!(s, ShapeOld::Wave(123));
    let s: ShapeOld = serde_json::from_str(r#"{"Image":[77, 78]}"#).unwrap();
    assert_eq!(s, ShapeOld::Image(77, 78));
    let s: Shape = serde_json::from_str(r#"[]"#).unwrap();
    assert_eq!(s, Shape::Scalar);
    let s: Shape = serde_json::from_str(r#"[12]"#).unwrap();
    assert_eq!(s, Shape::Wave(12));
    let s: Shape = serde_json::from_str(r#"[12, 13]"#).unwrap();
    assert_eq!(s, Shape::Image(12, 13));
}

pub mod timeunits {
    pub const MU: u64 = 1000;
    pub const MS: u64 = MU * 1000;
    pub const SEC: u64 = MS * 1000;
    pub const MIN: u64 = SEC * 60;
    pub const HOUR: u64 = MIN * 60;
    pub const DAY: u64 = HOUR * 24;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Dim0Kind {
    Time,
    Pulse,
}

pub trait Dim0Index: Clone + fmt::Debug + PartialOrd {
    fn add(&self, v: &Self) -> Self;
    fn sub(&self, v: &Self) -> Self;
    fn sub_n(&self, v: u64) -> Self;
    fn times(&self, x: u64) -> Self;
    fn div_n(&self, n: u64) -> Self;
    fn div_v(&self, v: &Self) -> u64;
    fn as_u64(&self) -> u64;
    fn series_range(a: Self, b: Self) -> SeriesRange;
    fn prebin_bin_len_opts() -> Vec<Self>;
    fn prebin_patch_len_for(i: usize) -> Self;
    fn to_pre_binned_patch_range_enum(
        &self,
        bin_count: u64,
        patch_offset: u64,
        patch_count: u64,
    ) -> PreBinnedPatchRangeEnum;
    fn binned_bin_len_opts() -> Vec<Self>;
    fn to_binned_range_enum(&self, bin_off: u64, bin_cnt: u64) -> BinnedRangeEnum;
}

pub trait Dim0Range: Clone + fmt::Debug + PartialOrd {}

pub struct Dim0RangeValue<T>
where
    T: Dim0Index,
{
    pub ix: [T; 2],
}

#[derive(Clone, Deserialize, PartialEq, PartialOrd)]
pub struct TsNano(pub u64);

mod ts_nano_ser {
    use super::TsNano;
    use crate::timeunits::SEC;
    use chrono::TimeZone;
    use chrono::Utc;
    use serde::Serialize;

    impl Serialize for TsNano {
        fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let ts = Utc.timestamp_opt((self.0 / SEC) as i64, (self.0 % SEC) as u32);
            let value = format!("{}", ts.earliest().unwrap());
            ser.serialize_newtype_struct("TsNano", &value)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct PulseId(u64);

impl TsNano {
    pub fn from_ns(ns: u64) -> Self {
        Self(ns)
    }

    pub fn ns(&self) -> u64 {
        self.0
    }
}

impl fmt::Debug for TsNano {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ts = Utc.timestamp_opt((self.0 / SEC) as i64, (self.0 % SEC) as u32);
        f.debug_struct("TsNano")
            .field("ts", &ts.earliest().unwrap_or(Default::default()))
            .finish()
    }
}

impl PulseId {
    pub fn from_id(id: u64) -> Self {
        Self(id)
    }
}

impl Dim0Index for TsNano {
    fn add(&self, v: &Self) -> Self {
        Self(self.0 + v.0)
    }

    fn sub(&self, v: &Self) -> Self {
        Self(self.0 - v.0)
    }

    fn sub_n(&self, v: u64) -> Self {
        Self(self.0 - v)
    }

    fn times(&self, x: u64) -> Self {
        Self(self.0 * x)
    }

    fn div_n(&self, n: u64) -> Self {
        Self(self.0 / n)
    }

    fn div_v(&self, v: &Self) -> u64 {
        self.0 / v.0
    }

    fn as_u64(&self) -> u64 {
        self.0
    }

    fn series_range(a: Self, b: Self) -> SeriesRange {
        SeriesRange::TimeRange(NanoRange { beg: a.0, end: b.0 })
    }

    fn prebin_bin_len_opts() -> Vec<Self> {
        PREBIN_TIME_BIN_LEN_VAR0.iter().map(|&x| Self(x)).collect()
    }

    fn prebin_patch_len_for(i: usize) -> Self {
        todo!()
    }

    fn to_pre_binned_patch_range_enum(
        &self,
        bin_count: u64,
        patch_offset: u64,
        patch_count: u64,
    ) -> PreBinnedPatchRangeEnum {
        PreBinnedPatchRangeEnum::Time(PreBinnedPatchRange {
            first: PreBinnedPatchCoord {
                bin_len: self.clone(),
                bin_count,
                patch_offset,
            },
            patch_count,
        })
    }

    fn binned_bin_len_opts() -> Vec<Self> {
        TIME_BIN_THRESHOLDS.iter().map(|&x| Self(x)).collect()
    }

    fn to_binned_range_enum(&self, bin_off: u64, bin_cnt: u64) -> BinnedRangeEnum {
        BinnedRangeEnum::Time(BinnedRange {
            bin_len: self.clone(),
            bin_off,
            bin_cnt,
        })
    }
}

impl Dim0Index for PulseId {
    fn add(&self, v: &Self) -> Self {
        Self(self.0 + v.0)
    }

    fn sub(&self, v: &Self) -> Self {
        Self(self.0 - v.0)
    }

    fn sub_n(&self, v: u64) -> Self {
        Self(self.0 - v)
    }

    fn times(&self, x: u64) -> Self {
        Self(self.0 * x)
    }

    fn div_n(&self, n: u64) -> Self {
        Self(self.0 / n)
    }

    fn div_v(&self, v: &Self) -> u64 {
        self.0 / v.0
    }

    fn as_u64(&self) -> u64 {
        self.0
    }

    fn series_range(a: Self, b: Self) -> SeriesRange {
        SeriesRange::PulseRange(PulseRange { beg: a.0, end: b.0 })
    }

    fn prebin_bin_len_opts() -> Vec<Self> {
        PREBIN_PULSE_BIN_LEN_VAR0.iter().map(|&x| Self(x)).collect()
    }

    fn prebin_patch_len_for(i: usize) -> Self {
        todo!()
    }

    fn to_pre_binned_patch_range_enum(
        &self,
        bin_count: u64,
        patch_offset: u64,
        patch_count: u64,
    ) -> PreBinnedPatchRangeEnum {
        PreBinnedPatchRangeEnum::Pulse(PreBinnedPatchRange {
            first: PreBinnedPatchCoord {
                bin_len: self.clone(),
                bin_count,
                patch_offset,
            },
            patch_count,
        })
    }

    fn binned_bin_len_opts() -> Vec<Self> {
        PULSE_BIN_THRESHOLDS.iter().map(|&x| Self(x)).collect()
    }

    fn to_binned_range_enum(&self, bin_off: u64, bin_cnt: u64) -> BinnedRangeEnum {
        BinnedRangeEnum::Pulse(BinnedRange {
            bin_len: self.clone(),
            bin_off,
            bin_cnt,
        })
    }
}

const PREBIN_TIME_BIN_LEN_VAR0: [u64; 3] = [MIN * 1, HOUR * 1, DAY];

const PREBIN_PULSE_BIN_LEN_VAR0: [u64; 4] = [100, 10000, 1000000, 100000000];

const PATCH_T_LEN_OPTIONS_SCALAR: [u64; 3] = [
    //
    //MIN * 60,
    HOUR * 6,
    DAY * 16,
    DAY * 64,
];

const PATCH_T_LEN_OPTIONS_WAVE: [u64; 3] = [
    //
    //MIN * 10,
    HOUR * 6,
    DAY * 8,
    DAY * 32,
];

const TIME_BIN_THRESHOLDS: [u64; 39] = [
    MU,
    MU * 2,
    MU * 5,
    MU * 10,
    MU * 20,
    MU * 50,
    MU * 100,
    MU * 200,
    MU * 500,
    MS,
    MS * 2,
    MS * 5,
    MS * 10,
    MS * 20,
    MS * 50,
    MS * 100,
    MS * 200,
    MS * 500,
    SEC,
    SEC * 2,
    SEC * 5,
    SEC * 10,
    SEC * 20,
    MIN,
    MIN * 2,
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

const PULSE_BIN_THRESHOLDS: [u64; 25] = [
    10, 20, 40, 80, 100, 200, 400, 800, 1000, 2000, 4000, 8000, 10000, 20000, 40000, 80000, 100000, 200000, 400000,
    800000, 1000000, 2000000, 4000000, 8000000, 10000000,
];

const fn time_bin_threshold_at(i: usize) -> TsNano {
    TsNano(TIME_BIN_THRESHOLDS[i])
}

const fn pulse_bin_threshold_at(i: usize) -> PulseId {
    PulseId(PULSE_BIN_THRESHOLDS[i])
}

/// Identifies one patch on the binning grid at a certain resolution.
/// A patch consists of `bin_count` consecutive bins.
/// In total, a given `PreBinnedPatchCoord` spans a time range from `patch_beg` to `patch_end`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreBinnedPatchCoord<T>
where
    T: Dim0Index,
{
    bin_len: T,
    bin_count: u64,
    patch_offset: u64,
}

impl<T> PreBinnedPatchCoord<T>
where
    T: Dim0Index,
{
    pub fn new(bin_len: T, bin_count: u64, patch_offset: u64) -> Self {
        Self {
            bin_len,
            bin_count,
            patch_offset,
        }
    }
    pub fn bin_len(&self) -> T {
        self.bin_len.clone()
    }

    pub fn patch_len(&self) -> T {
        self.bin_len().times(self.bin_count)
    }

    pub fn patch_beg(&self) -> T {
        self.bin_len().times(self.bin_count).times(self.patch_offset)
    }

    pub fn patch_end(&self) -> T {
        self.bin_len().times(self.bin_count).times(1 + self.patch_offset)
    }

    pub fn series_range(&self) -> SeriesRange {
        T::series_range(self.patch_beg(), self.patch_end())
    }

    pub fn bin_count(&self) -> u64 {
        self.bin_count
    }

    pub fn patch_offset(&self) -> u64 {
        self.patch_offset
    }

    pub fn edges(&self) -> Vec<T> {
        let mut ret = Vec::new();
        let mut t = self.patch_beg();
        ret.push(t.clone());
        for _ in 0..self.bin_count() {
            t = t.add(&self.bin_len);
            ret.push(t.clone());
        }
        ret
    }

    pub fn next(&self) -> Self {
        Self::new(self.bin_len.clone(), self.bin_count, 1 + self.patch_offset)
    }
}

impl<T> AppendToUrl for PreBinnedPatchCoord<T>
where
    T: Dim0Index,
{
    fn append_to_url(&self, url: &mut Url) {
        error!("TODO AppendToUrl for PreBinnedPatchCoord");
        err::todo();
        // TODO must also emit the type of the series index
        let mut g = url.query_pairs_mut();
        g.append_pair("patchTlen", &format!("{}", 4242));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PreBinnedPatchCoordEnum {
    Time(PreBinnedPatchCoord<TsNano>),
    Pulse(PreBinnedPatchCoord<PulseId>),
}

impl PreBinnedPatchCoordEnum {
    pub fn bin_count(&self) -> u64 {
        todo!()
    }

    pub fn span_desc(&self) -> String {
        match self {
            PreBinnedPatchCoordEnum::Time(k) => {
                format!("pre-W-{}-B-{}", k.bin_len.0 * k.bin_count / SEC, k.patch_offset / SEC)
            }
            PreBinnedPatchCoordEnum::Pulse(k) => {
                format!("pre-W-{}-B-{}", k.bin_len.0 * k.bin_count / SEC, k.patch_offset / SEC)
            }
        }
    }
}

impl FromUrl for PreBinnedPatchCoordEnum {
    fn from_url(url: &Url) -> Result<Self, Error> {
        todo!()
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        todo!()
    }
}

impl AppendToUrl for PreBinnedPatchCoordEnum {
    fn append_to_url(&self, url: &mut Url) {
        todo!()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreBinnedPatchRange<T>
where
    T: Dim0Index,
{
    first: PreBinnedPatchCoord<T>,
    patch_count: u64,
}

impl<T> PreBinnedPatchRange<T>
where
    T: Dim0Index,
{
    pub fn edges(&self) -> Vec<u64> {
        let mut ret = Vec::new();
        err::todo();
        ret
    }

    pub fn series_range(&self) -> SeriesRange {
        T::series_range(err::todoval(), err::todoval())
    }

    pub fn patch_count(&self) -> u64 {
        self.patch_count
    }

    pub fn bin_count(&self) -> u64 {
        err::todoval()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PreBinnedPatchRangeEnum {
    Time(PreBinnedPatchRange<TsNano>),
    Pulse(PreBinnedPatchRange<PulseId>),
}

impl PreBinnedPatchRangeEnum {
    fn covering_range_ty<T>(a: T, b: T, min_bin_count: u32) -> Result<Self, Error>
    where
        T: Dim0Index + 'static,
    {
        let opts = T::prebin_bin_len_opts();
        if min_bin_count < 1 {
            Err(Error::with_msg("min_bin_count < 1"))?;
        }
        if min_bin_count > 20000 {
            Err(Error::with_msg(format!("min_bin_count > 20000: {}", min_bin_count)))?;
        }
        let du = b.sub(&a);
        let max_bin_len = du.div_n(min_bin_count as u64);
        for (i1, bl) in opts.iter().enumerate().rev() {
            if bl <= &max_bin_len {
                let patch_len = <T as Dim0Index>::prebin_patch_len_for(i1);
                let bin_count = patch_len.div_v(bl);
                let patch_off_1 = a.div_v(&patch_len);
                let patch_off_2 = (b.add(&patch_len).sub_n(1)).div_v(&patch_len);
                let patch_count = patch_off_2 - patch_off_1;
                let ret = T::to_pre_binned_patch_range_enum(&bl, bin_count, patch_off_1, patch_count);
                return Ok(ret);
            }
        }
        Err(Error::with_msg_no_trace("can not find matching pre-binned grid"))
    }

    /// Cover at least the given range with at least as many as the requested number of bins.
    pub fn covering_range(range: SeriesRange, min_bin_count: u32) -> Result<Self, Error> {
        match range {
            SeriesRange::TimeRange(k) => Self::covering_range_ty(TsNano(k.beg), TsNano(k.end), min_bin_count),
            SeriesRange::PulseRange(k) => Self::covering_range_ty(PulseId(k.beg), PulseId(k.end), min_bin_count),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BinnedRange<T>
where
    T: Dim0Index,
{
    // TODO remove pub, which is currently used in tests
    pub bin_len: T,
    pub bin_off: u64,
    pub bin_cnt: u64,
}

impl<T> fmt::Debug for BinnedRange<T>
where
    T: Dim0Index,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("BinnedRange")
            .field("bin_len", &self.bin_len)
            .field("bin_off", &self.bin_off)
            .field("bin_cnt", &self.bin_cnt)
            .finish()
    }
}

impl BinnedRange<TsNano> {
    pub fn to_nano_range(&self) -> NanoRange {
        self.full_range()
    }
}

impl<T> BinnedRange<T>
where
    T: Dim0Index,
{
    pub fn bin_count(&self) -> u64 {
        self.bin_cnt
    }

    pub fn get_range(&self, ix: u32) -> NanoRange {
        /*NanoRange {
            beg: (self.offset + ix as u64) * self.grid_spec.bin_t_len,
            end: (self.offset + ix as u64 + 1) * self.grid_spec.bin_t_len,
        }*/
        err::todoval()
    }

    pub fn full_range(&self) -> NanoRange {
        /*NanoRange {
            beg: self.offset * self.grid_spec.bin_t_len,
            end: (self.offset + self.bin_count) * self.grid_spec.bin_t_len,
        }*/
        let beg = self.bin_len.times(self.bin_off).as_u64();
        let end = self.bin_len.times(self.bin_off + self.bin_cnt).as_u64();
        warn!("TODO make generic for pulse");
        NanoRange { beg, end }
    }

    pub fn edges_u64(&self) -> Vec<u64> {
        let mut ret = Vec::new();
        let mut t = self.bin_len.times(self.bin_off);
        let end = self.bin_len.times(self.bin_off + self.bin_cnt);
        while t <= end {
            ret.push(t.as_u64());
            t = t.add(&self.bin_len);
        }
        ret
    }

    pub fn edges(&self) -> Vec<T> {
        let mut ret = Vec::new();
        let mut t = self.bin_len.times(self.bin_off);
        let end = self.bin_len.times(self.bin_off + self.bin_cnt);
        while t <= end {
            ret.push(t.clone());
            t = t.add(&self.bin_len);
        }
        ret
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinnedRangeEnum {
    Time(BinnedRange<TsNano>),
    Pulse(BinnedRange<PulseId>),
}

impl BinnedRangeEnum {
    fn covering_range_ty<T>(a: T, b: T, min_bin_count: u32) -> Result<Self, Error>
    where
        T: Dim0Index + 'static,
    {
        let opts = T::binned_bin_len_opts();
        if min_bin_count < 1 {
            Err(Error::with_msg("min_bin_count < 1"))?;
        }
        if min_bin_count > 20000 {
            Err(Error::with_msg(format!("min_bin_count > 20000: {}", min_bin_count)))?;
        }
        let du = b.sub(&a);
        let max_bin_len = du.div_n(min_bin_count as u64);
        for (_, bl) in opts.iter().enumerate().rev() {
            if bl <= &max_bin_len {
                let off_1 = a.div_v(&bl);
                let off_2 = (b.add(&bl).sub_n(1)).div_v(&bl);
                eprintln!("off_1 {off_1:?}  off_2 {off_2:?}");
                let bin_cnt = off_2 - off_1;
                let ret = T::to_binned_range_enum(bl, off_1, bin_cnt);
                return Ok(ret);
            }
        }
        Err(Error::with_msg_no_trace("can not find matching pre-binned grid"))
    }

    /// Cover at least the given range with at least as many as the requested number of bins.
    pub fn covering_range(range: SeriesRange, min_bin_count: u32) -> Result<Self, Error> {
        match range {
            SeriesRange::TimeRange(k) => Self::covering_range_ty(TsNano(k.beg), TsNano(k.end), min_bin_count),
            SeriesRange::PulseRange(k) => Self::covering_range_ty(PulseId(k.beg), PulseId(k.end), min_bin_count),
        }
    }

    pub fn bin_count(&self) -> u64 {
        match self {
            BinnedRangeEnum::Time(k) => k.bin_count(),
            BinnedRangeEnum::Pulse(k) => k.bin_count(),
        }
    }

    pub fn range_at(&self, i: usize) -> Option<SeriesRange> {
        match self {
            BinnedRangeEnum::Time(k) => {
                if (i as u64) < k.bin_cnt {
                    let beg = k.bin_len.0 * (k.bin_off + i as u64);
                    let x = SeriesRange::TimeRange(NanoRange {
                        beg,
                        end: beg + k.bin_len.0,
                    });
                    Some(x)
                } else {
                    None
                }
            }
            BinnedRangeEnum::Pulse(k) => {
                if (i as u64) < k.bin_cnt {
                    let beg = k.bin_len.0 * (k.bin_off + i as u64);
                    let x = SeriesRange::PulseRange(PulseRange {
                        beg,
                        end: beg + k.bin_len.0,
                    });
                    Some(x)
                } else {
                    None
                }
            }
        }
    }

    pub fn dim0kind(&self) -> Dim0Kind {
        match self {
            BinnedRangeEnum::Time(_) => Dim0Kind::Time,
            BinnedRangeEnum::Pulse(_) => Dim0Kind::Pulse,
        }
    }

    pub fn binned_range_time(&self) -> BinnedRange<TsNano> {
        match self {
            BinnedRangeEnum::Time(x) => x.clone(),
            BinnedRangeEnum::Pulse(_) => panic!(),
        }
    }

    // Only a helper for unit tests.
    pub fn from_custom(len: TsNano, off: u64, cnt: u64) -> BinnedRangeEnum {
        let rng = BinnedRange {
            bin_len: len,
            bin_off: off,
            bin_cnt: cnt,
        };
        BinnedRangeEnum::Time(rng)
    }
}

#[cfg(test)]
mod test_binned_range {
    use super::*;

    #[test]
    fn binned_range_00() {
        let range = NanoRange {
            beg: HOUR * 72,
            end: HOUR * 73,
        };
        let range = BinnedRangeEnum::covering_range(range.into(), 10).unwrap();
        assert_eq!(range.bin_count(), 12);
        match range {
            BinnedRangeEnum::Time(range) => {
                assert_eq!(range.edges_u64()[0], HOUR * 72);
                assert_eq!(range.edges_u64()[2], HOUR * 72 + MIN * 5 * 2);
            }
            BinnedRangeEnum::Pulse(_) => panic!(),
        }
    }

    #[test]
    fn binned_range_01() {
        let range = NanoRange {
            beg: MIN * 20 + SEC * 10,
            end: HOUR * 10 + MIN * 20 + SEC * 30,
        };
        let range = BinnedRangeEnum::covering_range(range.into(), 10).unwrap();
        assert_eq!(range.bin_count(), 11);
        match range {
            BinnedRangeEnum::Time(range) => {
                assert_eq!(range.edges_u64()[0], HOUR * 0);
                assert_eq!(range.edges_u64()[1], HOUR * 1);
                assert_eq!(range.edges_u64()[11], HOUR * 11);
            }
            BinnedRangeEnum::Pulse(_) => panic!(),
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
    PulseIdDiff,
}

impl AggKind {
    pub fn do_time_weighted(&self) -> bool {
        match self {
            Self::EventBlobs => false,
            Self::TimeWeightedScalar => true,
            Self::DimXBins1 => false,
            Self::DimXBinsN(_) => false,
            Self::Plain => false,
            Self::PulseIdDiff => false,
        }
    }

    pub fn need_expand(&self) -> bool {
        match self {
            Self::EventBlobs => false,
            Self::TimeWeightedScalar => true,
            Self::DimXBins1 => false,
            Self::DimXBinsN(_) => false,
            Self::Plain => false,
            Self::PulseIdDiff => false,
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
        AggKind::PulseIdDiff => 0,
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
            Self::PulseIdDiff => {
                write!(fmt, "PulseIdDiff")
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
        } else if s.starts_with(nmark) {
            let nbins: u32 = s[nmark.len()..].parse()?;
            Ok(AggKind::DimXBinsN(nbins))
        } else if s == "PulseIdDiff" {
            Ok(AggKind::PulseIdDiff)
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
    complete: bool,
}

impl<T> OnlyFirstError<T> {
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }
}

impl<T, I, E> Stream for OnlyFirstError<T>
where
    T: Stream<Item = Result<I, E>> + Unpin,
{
    type Item = <T as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.complete {
            panic!("{} poll_next on complete", Self::type_name())
        }
        if self.errored {
            self.complete = true;
            return Ready(None);
        }
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => Ready(Some(Ok(k))),
            Ready(Some(Err(e))) => {
                self.errored = true;
                Ready(Some(Err(e)))
            }
            Ready(None) => {
                self.complete = true;
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
            complete: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RangeFilterStats {
    pub items_no_prune_high: u64,
    pub items_all_prune_high: u64,
    pub items_part_prune_high: u64,
}

impl RangeFilterStats {
    pub fn new() -> Self {
        Self {
            items_no_prune_high: 0,
            items_all_prune_high: 0,
            items_part_prune_high: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DiskStats {
    OpenStats(OpenStats),
    SeekStats(SeekStats),
    ReadStats(ReadStats),
    ReadExactStats(ReadExactStats),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OpenStats {
    pub duration: Duration,
}

impl OpenStats {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SeekStats {
    pub duration: Duration,
}

impl SeekStats {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadStats {
    pub duration: Duration,
}

impl ReadStats {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

impl PerfOpts {
    pub fn default() -> Self {
        Self {
            inmem_bufcap: 1024 * 512,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    Read5,
}

impl ReadSys {
    pub fn default() -> Self {
        Self::Read5
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
        } else if k == "Read5" {
            Self::Read5
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

impl Default for DiskIoTune {
    fn default() -> Self {
        Self::default()
    }
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
    #[serde(rename = "seriesId")]
    pub series: u64,
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
pub struct StatusSub {
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProxyConfig {
    pub name: String,
    pub listen: String,
    pub port: u16,
    pub backends: Vec<ProxyBackend>,
    pub status_subs: Vec<StatusSub>,
}

pub trait HasBackend {
    fn backend(&self) -> &str;
}

pub trait HasTimeout {
    fn timeout(&self) -> Duration;
}

pub trait FromUrl: Sized {
    fn from_url(url: &Url) -> Result<Self, Error>;
    // TODO put this in separate trait, because some implementors need url path segments to construct.
    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error>;
}

pub trait AppendToUrl {
    fn append_to_url(&self, url: &mut Url);
}

pub fn get_url_query_pairs(url: &Url) -> BTreeMap<String, String> {
    BTreeMap::from_iter(url.query_pairs().map(|(j, k)| (j.to_string(), k.to_string())))
}

// Request type of the channel/config api.
// At least on some backends the channel configuration may change depending on the queried range.
// Therefore, the query includes the range.
// The presence of a configuration in some range does not imply that there is any data available.
#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelConfigQuery {
    pub channel: SfDbChannel,
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
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let beg_date = pairs
            .get("begDate")
            .map(String::from)
            .unwrap_or_else(|| String::from("1970-01-01T00:00:00Z"));
        let end_date = pairs
            .get("endDate")
            .map(String::from)
            .unwrap_or_else(|| String::from("3000-01-01T00:00:00Z"));
        let expand = pairs.get("expand").map(|s| s == "true").unwrap_or(false);
        let ret = Self {
            channel: SfDbChannel::from_pairs(&pairs)?,
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
        self.channel.append_to_url(url);
        let mut g = url.query_pairs_mut();
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
    #[serde(rename = "channel")]
    pub channel: SfDbChannel,
    #[serde(rename = "scalarType")]
    pub scalar_type: ScalarType,
    #[serde(rename = "byteOrder", default, skip_serializing_if = "Option::is_none")]
    pub byte_order: Option<ByteOrder>,
    #[serde(rename = "shape")]
    pub shape: Shape,
}

/**
Provide basic information about a channel, especially it's shape.
Also, byte-order is important for clients that process the raw databuffer event data (python data_api3).
*/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelInfo {
    pub scalar_type: ScalarType,
    pub byte_order: Option<ByteOrder>,
    pub shape: Shape,
    pub msg: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChConf {
    pub backend: String,
    pub series: Option<u64>,
    pub name: String,
    pub scalar_type: ScalarType,
    pub shape: Shape,
}

impl ChConf {
    pub fn try_series(&self) -> Res2<u64> {
        self.series
            .ok_or_else(|| anyhow::anyhow!("ChConf without SeriesId {self:?}"))
    }
}

pub fn f32_close(a: f32, b: f32) -> bool {
    if (a - b).abs() < 1e-4 || (a / b > 0.999 && a / b < 1.001) {
        true
    } else {
        false
    }
}

pub fn f64_close(a: f64, b: f64) -> bool {
    if (a - b).abs() < 1e-5 || (a / b > 0.9999 && a / b < 1.0001) {
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
            prometheus_api_bind: None,
        })
        .collect();
    Cluster {
        backend: TEST_BACKEND.into(),
        nodes,
        database: Database {
            host: "127.0.0.1".into(),
            port: 5432,
            name: "testingdaq".into(),
            user: "testingdaq".into(),
            pass: "testingdaq".into(),
        },
        scylla: None,
        cache_scylla: None,
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
            prometheus_api_bind: None,
        })
        .collect();
    Cluster {
        backend: "sls-archive".into(),
        nodes,
        database: Database {
            host: "127.0.0.1".into(),
            port: 5432,
            name: "testingdaq".into(),
            user: "testingdaq".into(),
            pass: "testingdaq".into(),
        },
        scylla: None,
        cache_scylla: None,
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
            prometheus_api_bind: None,
        })
        .collect();
    Cluster {
        backend: "sf-archive".into(),
        nodes,
        database: Database {
            host: "127.0.0.1".into(),
            port: 5432,
            name: "testingdaq".into(),
            user: "testingdaq".into(),
            pass: "testingdaq".into(),
        },
        scylla: None,
        cache_scylla: None,
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

#[cfg(test)]
mod test_parse {
    use super::*;

    #[test]
    fn parse_scalar_type_shape() {
        let mut url: Url = "http://test/path".parse().unwrap();
        {
            let mut g = url.query_pairs_mut();
            g.append_pair("scalarType", &format!("{:?}", ScalarType::F32));
            g.append_pair("shape", &format!("{:?}", Shape::Image(3, 4)));
        }
        let url = url;
        let urls = format!("{}", url);
        let url: Url = urls.parse().unwrap();
        let mut a = BTreeMap::new();
        for (k, v) in url.query_pairs() {
            let k = k.to_string();
            let v = v.to_string();
            info!("k {k:?}  v {v:?}");
            a.insert(k, v);
        }
        assert_eq!(a.get("scalarType").unwrap(), "f32");
        assert_eq!(a.get("shape").unwrap(), "Image(3, 4)");
    }
}
