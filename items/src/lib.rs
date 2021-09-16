use crate::frame::make_frame_2;
use crate::numops::BoolNum;
use bytes::BytesMut;
use chrono::{TimeZone, Utc};
use err::Error;
use netpod::timeunits::{MS, SEC};
use netpod::{log::Level, AggKind, EventDataReadStats, EventQueryJsonStringFrame, NanoRange, Shape};
use serde::de::{self, DeserializeOwned, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{AsyncRead, ReadBuf};

pub mod eventvalues;
pub mod frame;
pub mod inmem;
pub mod minmaxavgbins;
pub mod minmaxavgdim1bins;
pub mod minmaxavgwavebins;
pub mod numops;
pub mod streams;
pub mod waveevents;
pub mod xbinnedscalarevents;
pub mod xbinnedwaveevents;

pub const EVENT_QUERY_JSON_STRING_FRAME: u32 = 0x100;
pub const EVENT_VALUES_FRAME_TYPE_ID: u32 = 0x500;
pub const MIN_MAX_AVG_BINS: u32 = 0x700;
pub const WAVE_EVENTS_FRAME_TYPE_ID: u32 = 0x800;
pub const X_BINNED_SCALAR_EVENTS_FRAME_TYPE_ID: u32 = 0x8800;
pub const X_BINNED_WAVE_EVENTS_FRAME_TYPE_ID: u32 = 0x900;
pub const MIN_MAX_AVG_WAVE_BINS: u32 = 0xa00;
pub const MIN_MAX_AVG_DIM_1_BINS_FRAME_TYPE_ID: u32 = 0xb00;
pub const EVENT_FULL_FRAME_TYPE_ID: u32 = 0x2200;

pub fn bool_is_false(j: &bool) -> bool {
    *j == false
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RangeCompletableItem<T> {
    RangeComplete,
    Data(T),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StatsItem {
    EventDataReadStats(EventDataReadStats),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StreamItem<T> {
    DataItem(T),
    Log(LogItem),
    Stats(StatsItem),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogItem {
    pub node_ix: u32,
    #[serde(with = "levelserde")]
    pub level: Level,
    pub msg: String,
}

impl LogItem {
    pub fn quick(level: Level, msg: String) -> Self {
        Self {
            level,
            msg,
            node_ix: 42,
        }
    }
}

pub type Sitemty<T> = Result<StreamItem<RangeCompletableItem<T>>, Error>;

struct VisitLevel;

impl<'de> Visitor<'de> for VisitLevel {
    type Value = u32;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "expect u32 Level code")
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
    }
}

mod levelserde {
    use super::Level;
    use super::VisitLevel;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(t: &Level, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let g = match *t {
            Level::ERROR => 1,
            Level::WARN => 2,
            Level::INFO => 3,
            Level::DEBUG => 4,
            Level::TRACE => 5,
        };
        s.serialize_u32(g)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        match d.deserialize_u32(VisitLevel) {
            Ok(level) => {
                let g = if level == 1 {
                    Level::ERROR
                } else if level == 2 {
                    Level::WARN
                } else if level == 3 {
                    Level::INFO
                } else if level == 4 {
                    Level::DEBUG
                } else if level == 5 {
                    Level::TRACE
                } else {
                    Level::TRACE
                };
                Ok(g)
            }
            Err(e) => Err(e),
        }
    }
}

pub const INMEM_FRAME_ENCID: u32 = 0x12121212;
pub const INMEM_FRAME_HEAD: usize = 20;
pub const INMEM_FRAME_FOOT: usize = 4;
pub const INMEM_FRAME_MAGIC: u32 = 0xc6c3b73d;

pub trait SubFrId {
    const SUB: u32;
}

impl SubFrId for u8 {
    const SUB: u32 = 3;
}

impl SubFrId for u16 {
    const SUB: u32 = 5;
}

impl SubFrId for u32 {
    const SUB: u32 = 8;
}

impl SubFrId for u64 {
    const SUB: u32 = 10;
}

impl SubFrId for i8 {
    const SUB: u32 = 2;
}

impl SubFrId for i16 {
    const SUB: u32 = 4;
}

impl SubFrId for i32 {
    const SUB: u32 = 7;
}

impl SubFrId for i64 {
    const SUB: u32 = 9;
}

impl SubFrId for f32 {
    const SUB: u32 = 11;
}

impl SubFrId for f64 {
    const SUB: u32 = 12;
}

impl SubFrId for BoolNum {
    const SUB: u32 = 13;
}

pub trait SitemtyFrameType {
    const FRAME_TYPE_ID: u32;
}

pub trait FrameType {
    const FRAME_TYPE_ID: u32;
}

impl FrameType for EventQueryJsonStringFrame {
    const FRAME_TYPE_ID: u32 = EVENT_QUERY_JSON_STRING_FRAME;
}

impl<T> FrameType for Sitemty<T>
where
    T: SitemtyFrameType,
{
    const FRAME_TYPE_ID: u32 = T::FRAME_TYPE_ID;
}

pub trait ProvidesFrameType {
    fn frame_type_id(&self) -> u32;
}

pub trait Framable: Send {
    fn typeid(&self) -> u32;
    fn make_frame(&self) -> Result<BytesMut, Error>;
}

// TODO need also Framable for those types defined in other crates.
impl<T> Framable for Sitemty<T>
where
    T: SitemtyFrameType + Serialize + Send,
{
    fn typeid(&self) -> u32 {
        T::FRAME_TYPE_ID
    }

    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame_2(self, T::FRAME_TYPE_ID)
    }
}

pub trait EventsNodeProcessor: Send + Unpin {
    type Input;
    type Output: Send + Unpin + DeserializeOwned + WithTimestamps + TimeBinnableType + ByteEstimate;
    fn create(shape: Shape, agg_kind: AggKind) -> Self;
    fn process(&self, inp: Self::Input) -> Self::Output;
}

#[derive(Clone, Debug, Deserialize)]
pub struct IsoDateTime(chrono::DateTime<Utc>);

impl Serialize for IsoDateTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string())
    }
}

pub fn make_iso_ts(tss: &[u64]) -> Vec<IsoDateTime> {
    tss.iter()
        .map(|&k| IsoDateTime(Utc.timestamp_nanos(k as i64)))
        .collect()
}

pub enum Fits {
    Empty,
    Lower,
    Greater,
    Inside,
    PartlyLower,
    PartlyGreater,
    PartlyLowerAndGreater,
}

pub trait WithLen {
    fn len(&self) -> usize;
}

pub trait WithTimestamps {
    fn ts(&self, ix: usize) -> u64;
}

pub trait ByteEstimate {
    fn byte_estimate(&self) -> u64;
}

pub trait RangeOverlapInfo {
    fn ends_before(&self, range: NanoRange) -> bool;
    fn ends_after(&self, range: NanoRange) -> bool;
    fn starts_after(&self, range: NanoRange) -> bool;
}

pub trait FitsInside {
    fn fits_inside(&self, range: NanoRange) -> Fits;
}

pub trait FilterFittingInside: Sized {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self>;
}

pub trait PushableIndex {
    // TODO check whether it makes sense to allow a move out of src. Or use a deque for src type and pop?
    fn push_index(&mut self, src: &Self, ix: usize);
}

pub trait Appendable: WithLen {
    fn empty() -> Self;
    fn append(&mut self, src: &Self);
}

pub trait EventAppendable {
    type Value;
    fn append_event(&mut self, ts: u64, value: Self::Value);
}

pub trait TimeBins: Send + Unpin + WithLen + Appendable + FilterFittingInside {
    fn ts1s(&self) -> &Vec<u64>;
    fn ts2s(&self) -> &Vec<u64>;
}

pub trait TimeBinnableType:
    Send + Unpin + RangeOverlapInfo + FilterFittingInside + Appendable + Serialize + ReadableFromFile
{
    type Output: TimeBinnableType;
    type Aggregator: TimeBinnableTypeAggregator<Input = Self, Output = Self::Output> + Send + Unpin;
    fn aggregator(range: NanoRange, bin_count: usize, do_time_weight: bool) -> Self::Aggregator;
}

// TODO should get I/O and tokio dependence out of this crate
pub trait ReadableFromFile: Sized {
    fn read_from_file(file: File) -> Result<ReadPbv<Self>, Error>;
    // TODO should not need this:
    fn from_buf(buf: &[u8]) -> Result<Self, Error>;
}

// TODO should get I/O and tokio dependence out of this crate
pub struct ReadPbv<T>
where
    T: ReadableFromFile,
{
    buf: Vec<u8>,
    all: Vec<u8>,
    file: Option<File>,
    _m1: PhantomData<T>,
}

impl<T> ReadPbv<T>
where
    T: ReadableFromFile,
{
    fn new(file: File) -> Self {
        Self {
            // TODO make buffer size a parameter:
            buf: vec![0; 1024 * 32],
            all: vec![],
            file: Some(file),
            _m1: PhantomData,
        }
    }
}

impl<T> Future for ReadPbv<T>
where
    T: ReadableFromFile + Unpin,
{
    type Output = Result<StreamItem<RangeCompletableItem<T>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        let mut buf = std::mem::replace(&mut self.buf, Vec::new());
        let ret = 'outer: loop {
            let mut dst = ReadBuf::new(&mut buf);
            if dst.remaining() == 0 || dst.capacity() == 0 {
                break Ready(Err(Error::with_msg("bad read buffer")));
            }
            let fp = self.file.as_mut().unwrap();
            let f = Pin::new(fp);
            break match File::poll_read(f, cx, &mut dst) {
                Ready(res) => match res {
                    Ok(_) => {
                        if dst.filled().len() > 0 {
                            self.all.extend_from_slice(dst.filled());
                            continue 'outer;
                        } else {
                            match T::from_buf(&mut self.all) {
                                Ok(item) => Ready(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))),
                                Err(e) => Ready(Err(e)),
                            }
                        }
                    }
                    Err(e) => Ready(Err(e.into())),
                },
                Pending => Pending,
            };
        };
        self.buf = buf;
        ret
    }
}

pub fn ts_offs_from_abs(tss: &[u64]) -> (u64, Vec<u64>, Vec<u64>) {
    let ts_anchor_sec = tss.first().map_or(0, |&k| k) / SEC;
    let ts_anchor_ns = ts_anchor_sec * SEC;
    let ts_off_ms: Vec<_> = tss.iter().map(|&k| (k - ts_anchor_ns) / MS).collect();
    let ts_off_ns = tss
        .iter()
        .zip(ts_off_ms.iter().map(|&k| k * MS))
        .map(|(&j, k)| (j - ts_anchor_ns - k))
        .collect();
    (ts_anchor_sec, ts_off_ms, ts_off_ns)
}

pub trait TimeBinnableTypeAggregator: Send {
    type Input: TimeBinnableType;
    type Output: TimeBinnableType;
    fn range(&self) -> &NanoRange;
    fn ingest(&mut self, item: &Self::Input);
    fn result_reset(&mut self, range: NanoRange, expand: bool) -> Self::Output;
}
