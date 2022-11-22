pub mod binnedevents;
pub mod binsdim0;
pub mod binsdim1;
pub mod eventfull;
pub mod eventsitem;
pub mod frame;
pub mod inmem;
pub mod numops;
pub mod plainevents;
pub mod scalarevents;
pub mod statsevents;
pub mod streams;
pub mod waveevents;
pub mod xbinnedscalarevents;
pub mod xbinnedwaveevents;

use crate::frame::make_frame_2;
use crate::numops::BoolNum;
use bytes::BytesMut;
use chrono::{TimeZone, Utc};
use err::Error;
use frame::{make_error_frame, make_log_frame, make_range_complete_frame, make_stats_frame};
#[allow(unused)]
use netpod::log::*;
use netpod::timeunits::{MS, SEC};
use netpod::{log::Level, AggKind, EventDataReadStats, NanoRange, Shape};
use netpod::{DiskStats, RangeFilterStats, ScalarType};
use numops::StringNum;
use serde::de::{self, DeserializeOwned, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use std::any::Any;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{AsyncRead, ReadBuf};

pub const TERM_FRAME_TYPE_ID: u32 = 0xaa01;
pub const ERROR_FRAME_TYPE_ID: u32 = 0xaa02;
pub const SITEMTY_NONSPEC_FRAME_TYPE_ID: u32 = 0xaa04;
pub const EVENT_QUERY_JSON_STRING_FRAME: u32 = 0x100;
pub const EVENTS_0D_FRAME_TYPE_ID: u32 = 0x500;
pub const MIN_MAX_AVG_DIM_0_BINS_FRAME_TYPE_ID: u32 = 0x700;
pub const MIN_MAX_AVG_DIM_1_BINS_FRAME_TYPE_ID: u32 = 0x800;
pub const MIN_MAX_AVG_WAVE_BINS: u32 = 0xa00;
pub const WAVE_EVENTS_FRAME_TYPE_ID: u32 = 0xb00;
pub const LOG_FRAME_TYPE_ID: u32 = 0xc00;
pub const STATS_FRAME_TYPE_ID: u32 = 0xd00;
pub const RANGE_COMPLETE_FRAME_TYPE_ID: u32 = 0xe00;
pub const EVENT_FULL_FRAME_TYPE_ID: u32 = 0x2200;
pub const EVENTS_ITEM_FRAME_TYPE_ID: u32 = 0x2300;
pub const STATS_EVENTS_FRAME_TYPE_ID: u32 = 0x2400;
pub const ITEMS_2_CHANNEL_EVENTS_FRAME_TYPE_ID: u32 = 0x2500;
pub const X_BINNED_SCALAR_EVENTS_FRAME_TYPE_ID: u32 = 0x8800;
pub const X_BINNED_WAVE_EVENTS_FRAME_TYPE_ID: u32 = 0x8900;

pub fn bool_is_false(j: &bool) -> bool {
    *j == false
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RangeCompletableItem<T> {
    RangeComplete,
    Data(T),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StatsItem {
    EventDataReadStats(EventDataReadStats),
    RangeFilterStats(RangeFilterStats),
    DiskStats(DiskStats),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StreamItem<T> {
    DataItem(T),
    Log(LogItem),
    Stats(StatsItem),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

impl<T> FrameType for Sitemty<T>
where
    T: FrameType,
{
    fn frame_type_id(&self) -> u32 {
        match self {
            Ok(item) => match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => SITEMTY_NONSPEC_FRAME_TYPE_ID,
                    RangeCompletableItem::Data(item) => item.frame_type_id(),
                },
                StreamItem::Log(_) => SITEMTY_NONSPEC_FRAME_TYPE_ID,
                StreamItem::Stats(_) => SITEMTY_NONSPEC_FRAME_TYPE_ID,
            },
            Err(_) => ERROR_FRAME_TYPE_ID,
        }
    }
}

pub fn sitem_data<X>(x: X) -> Sitemty<X> {
    Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))
}

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
    const SUB: u32 = 0x03;
}

impl SubFrId for u16 {
    const SUB: u32 = 0x05;
}

impl SubFrId for u32 {
    const SUB: u32 = 0x08;
}

impl SubFrId for u64 {
    const SUB: u32 = 0x0a;
}

impl SubFrId for i8 {
    const SUB: u32 = 0x02;
}

impl SubFrId for i16 {
    const SUB: u32 = 0x04;
}

impl SubFrId for i32 {
    const SUB: u32 = 0x07;
}

impl SubFrId for i64 {
    const SUB: u32 = 0x09;
}

impl SubFrId for f32 {
    const SUB: u32 = 0x0b;
}

impl SubFrId for f64 {
    const SUB: u32 = 0x0c;
}

impl SubFrId for StringNum {
    const SUB: u32 = 0x0d;
}

impl SubFrId for BoolNum {
    const SUB: u32 = 0x0e;
}

// Required for any inner type of Sitemty.
pub trait FrameTypeInnerStatic {
    const FRAME_TYPE_ID: u32;
}

// To be implemented by the T of Sitemty<T>, e.g. ScalarEvents.
pub trait FrameTypeInnerDyn {
    // TODO check actual usage of this
    fn frame_type_id(&self) -> u32;
}

impl<T> FrameTypeInnerDyn for T
where
    T: FrameTypeInnerStatic,
{
    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeInnerStatic>::FRAME_TYPE_ID
    }
}

pub trait FrameTypeStatic {
    const FRAME_TYPE_ID: u32;
}

impl<T> FrameTypeStatic for Sitemty<T>
where
    T: FrameTypeInnerStatic,
{
    const FRAME_TYPE_ID: u32 = <T as FrameTypeInnerStatic>::FRAME_TYPE_ID;
}

// Framable trait objects need some inspection to handle the supposed-to-be common Err ser format:
// Meant to be implemented by Sitemty.
pub trait FrameType {
    fn frame_type_id(&self) -> u32;
}

impl<T> FrameType for Box<T>
where
    T: FrameType,
{
    fn frame_type_id(&self) -> u32 {
        self.as_ref().frame_type_id()
    }
}

impl FrameTypeInnerDyn for Box<dyn TimeBinned> {
    fn frame_type_id(&self) -> u32 {
        FrameTypeInnerDyn::frame_type_id(self.as_time_binnable_dyn())
    }
}

impl FrameTypeInnerDyn for Box<dyn EventsDyn> {
    fn frame_type_id(&self) -> u32 {
        FrameTypeInnerDyn::frame_type_id(self.as_time_binnable_dyn())
    }
}

pub trait ContainsError {
    fn is_err(&self) -> bool;
    fn err(&self) -> Option<&::err::Error>;
}

impl<T> ContainsError for Box<T>
where
    T: ContainsError,
{
    fn is_err(&self) -> bool {
        self.as_ref().is_err()
    }

    fn err(&self) -> Option<&::err::Error> {
        self.as_ref().err()
    }
}

impl<T> ContainsError for Sitemty<T> {
    fn is_err(&self) -> bool {
        match self {
            Ok(_) => false,
            Err(_) => true,
        }
    }

    fn err(&self) -> Option<&::err::Error> {
        match self {
            Ok(_) => None,
            Err(e) => Some(e),
        }
    }
}

pub trait Framable {
    fn make_frame(&self) -> Result<BytesMut, Error>;
}

pub trait FramableInner: erased_serde::Serialize + FrameTypeInnerDyn + Send {
    fn _dummy(&self);
}

impl<T: erased_serde::Serialize + FrameTypeInnerDyn + Send> FramableInner for T {
    fn _dummy(&self) {}
}

erased_serde::serialize_trait_object!(EventsDyn);
erased_serde::serialize_trait_object!(TimeBinnableDyn);
erased_serde::serialize_trait_object!(TimeBinned);

impl<T> Framable for Sitemty<T>
where
    T: Sized + serde::Serialize + FrameType,
{
    fn make_frame(&self) -> Result<BytesMut, Error> {
        match self {
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k))) => {
                let frame_type_id = k.frame_type_id();
                make_frame_2(self, frame_type_id)
            }
            Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)) => make_range_complete_frame(),
            Ok(StreamItem::Log(item)) => make_log_frame(item),
            Ok(StreamItem::Stats(item)) => make_stats_frame(item),
            Err(e) => make_error_frame(e),
        }
    }
}

impl<T> Framable for Box<T>
where
    T: Framable + ?Sized,
{
    fn make_frame(&self) -> Result<BytesMut, Error> {
        self.as_ref().make_frame()
    }
}

pub trait FrameDecodable: FrameTypeStatic + DeserializeOwned {
    fn from_error(e: ::err::Error) -> Self;
    fn from_log(item: LogItem) -> Self;
    fn from_stats(item: StatsItem) -> Self;
    fn from_range_complete() -> Self;
}

impl<T> FrameDecodable for Sitemty<T>
where
    T: FrameTypeInnerStatic + DeserializeOwned,
{
    fn from_error(e: err::Error) -> Self {
        Err(e)
    }

    fn from_log(item: LogItem) -> Self {
        Ok(StreamItem::Log(item))
    }

    fn from_stats(item: StatsItem) -> Self {
        Ok(StreamItem::Stats(item))
    }

    fn from_range_complete() -> Self {
        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
    }
}

#[derive(Serialize, Deserialize)]
pub struct EventQueryJsonStringFrame(pub String);

impl FrameTypeInnerStatic for EventQueryJsonStringFrame {
    const FRAME_TYPE_ID: u32 = EVENT_QUERY_JSON_STRING_FRAME;
}

impl FrameType for EventQueryJsonStringFrame {
    fn frame_type_id(&self) -> u32 {
        EventQueryJsonStringFrame::FRAME_TYPE_ID
    }
}

pub trait EventsNodeProcessor: Send + Unpin {
    type Input;
    type Output: Send + Unpin + DeserializeOwned + WithTimestamps + TimeBinnableType + ByteEstimate;
    fn create(shape: Shape, agg_kind: AggKind) -> Self;
    fn process(&self, inp: Self::Input) -> Self::Output;
}

pub trait EventsTypeAliases {
    type TimeBinOutput;
}

impl<ENP> EventsTypeAliases for ENP
where
    ENP: EventsNodeProcessor,
    <ENP as EventsNodeProcessor>::Output: TimeBinnableType,
{
    type TimeBinOutput = <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output;
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
    // TODO do not take by value.
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

pub trait NewEmpty {
    fn empty(shape: Shape) -> Self;
}

pub trait Appendable: WithLen {
    fn empty_like_self(&self) -> Self;
    fn append(&mut self, src: &Self);

    // TODO the `ts2` makes no sense for non-bin-implementors
    fn append_zero(&mut self, ts1: u64, ts2: u64);
}

pub trait Clearable {
    fn clear(&mut self);
}

pub trait EventAppendable
where
    Self: Sized,
{
    type Value;
    fn append_event(ret: Option<Self>, ts: u64, pulse: u64, value: Self::Value) -> Self;
}

pub trait TimeBins: Send + Unpin + WithLen + Appendable + FilterFittingInside {
    fn ts1s(&self) -> &Vec<u64>;
    fn ts2s(&self) -> &Vec<u64>;
}

pub trait TimeBinnableType:
    Send
    + Unpin
    + RangeOverlapInfo
    + FilterFittingInside
    + NewEmpty
    + Appendable
    + Serialize
    + DeserializeOwned
    + ReadableFromFile
    + FrameTypeInnerStatic
{
    type Output: TimeBinnableType;
    type Aggregator: TimeBinnableTypeAggregator<Input = Self, Output = Self::Output> + Send + Unpin;
    fn aggregator(range: NanoRange, bin_count: usize, do_time_weight: bool) -> Self::Aggregator;
}

/// Provides a time-binned representation of the implementing type.
/// In contrast to `TimeBinnableType` this is meant for trait objects.

// TODO should not require Sync!
// TODO SitemtyFrameType is already supertrait of FramableInner.
pub trait TimeBinnableDyn:
    std::fmt::Debug
    + FramableInner
    + FrameType
    + FrameTypeInnerDyn
    + WithLen
    + RangeOverlapInfo
    + Any
    + Sync
    + Send
    + 'static
{
    fn time_binner_new(&self, edges: Vec<u64>, do_time_weight: bool) -> Box<dyn TimeBinnerDyn>;
    fn as_any(&self) -> &dyn Any;
}

pub trait TimeBinnableDynStub:
    std::fmt::Debug
    + FramableInner
    + FrameType
    + FrameTypeInnerDyn
    + WithLen
    + RangeOverlapInfo
    + Any
    + Sync
    + Send
    + 'static
{
}

// impl for the stubs TODO: remove
impl<T> TimeBinnableDyn for T
where
    T: TimeBinnableDynStub,
{
    fn time_binner_new(&self, _edges: Vec<u64>, _do_time_weight: bool) -> Box<dyn TimeBinnerDyn> {
        error!("TODO impl time_binner_new for T {}", std::any::type_name::<T>());
        err::todoval()
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

// TODO maybe this is no longer needed:
pub trait TimeBinnableDynAggregator: Send {
    fn ingest(&mut self, item: &dyn TimeBinnableDyn);
    fn result(&mut self) -> Box<dyn TimeBinned>;
}

/// Container of some form of events, for use as trait object.
pub trait EventsDyn: TimeBinnableDyn {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnableDyn;
    fn verify(&self);
    fn output_info(&self);
}

/// Data in time-binned form.
pub trait TimeBinned: TimeBinnableDyn {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnableDyn;
    fn edges_slice(&self) -> (&[u64], &[u64]);
    fn counts(&self) -> &[u64];
    fn mins(&self) -> Vec<f32>;
    fn maxs(&self) -> Vec<f32>;
    fn avgs(&self) -> Vec<f32>;
    fn validate(&self) -> Result<(), String>;
}

impl FrameType for Box<dyn TimeBinned> {
    fn frame_type_id(&self) -> u32 {
        FrameType::frame_type_id(self.as_ref())
    }
}

impl WithLen for Box<dyn TimeBinned> {
    fn len(&self) -> usize {
        self.as_time_binnable_dyn().len()
    }
}

impl RangeOverlapInfo for Box<dyn TimeBinned> {
    fn ends_before(&self, range: NanoRange) -> bool {
        self.as_time_binnable_dyn().ends_before(range)
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        self.as_time_binnable_dyn().ends_after(range)
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        self.as_time_binnable_dyn().starts_after(range)
    }
}

impl TimeBinnableDyn for Box<dyn TimeBinned> {
    fn time_binner_new(&self, edges: Vec<u64>, do_time_weight: bool) -> Box<dyn TimeBinnerDyn> {
        self.as_time_binnable_dyn().time_binner_new(edges, do_time_weight)
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
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

pub fn pulse_offs_from_abs(pulse: &[u64]) -> (u64, Vec<u64>) {
    let pulse_anchor = pulse.first().map_or(0, |k| *k);
    let pulse_off: Vec<_> = pulse.iter().map(|k| *k - pulse_anchor).collect();
    (pulse_anchor, pulse_off)
}

pub trait TimeBinnableTypeAggregator: Send {
    type Input: TimeBinnableType;
    type Output: TimeBinnableType;
    fn range(&self) -> &NanoRange;
    fn ingest(&mut self, item: &Self::Input);
    // TODO this API is too convoluted for a minimal performance gain: should separate `result` and `reset`
    // or simply require to construct a new which is almost equally expensive.
    fn result_reset(&mut self, range: NanoRange, expand: bool) -> Self::Output;
}

pub trait TimestampInspectable: WithTimestamps + WithLen {}

impl<T> TimestampInspectable for T where T: WithTimestamps + WithLen {}

pub fn inspect_timestamps(events: &dyn TimestampInspectable, range: NanoRange) -> String {
    use fmt::Write;
    let rd = range.delta();
    let mut buf = String::new();
    let n = events.len();
    for i in 0..n {
        if i < 3 || i > (n - 4) {
            let ts = events.ts(i);
            let z = ts - range.beg;
            let z = z as f64 / rd as f64 * 2.0 - 1.0;
            write!(&mut buf, "i  {:3}  tt {:6.3}\n", i, z).unwrap();
        }
    }
    buf
}

pub trait TimeBinnerDyn: Send {
    fn bins_ready_count(&self) -> usize;
    fn bins_ready(&mut self) -> Option<Box<dyn TimeBinned>>;
    fn ingest(&mut self, item: &dyn TimeBinnableDyn);

    /// If there is a bin in progress with non-zero count, push it to the result set.
    /// With push_empty == true, a bin in progress is pushed even if it contains no counts.
    fn push_in_progress(&mut self, push_empty: bool);

    /// Implies `Self::push_in_progress` but in addition, pushes a zero-count bin if the call
    /// to `push_in_progress` did not change the result count, as long as edges are left.
    /// The next call to `Self::bins_ready_count` must return one higher count than before.
    fn cycle(&mut self);
}

pub fn empty_events_dyn(scalar_type: &ScalarType, shape: &Shape, agg_kind: &AggKind) -> Box<dyn TimeBinnableDyn> {
    match shape {
        Shape::Scalar => match agg_kind {
            AggKind::TimeWeightedScalar => {
                use ScalarType::*;
                type K<T> = scalarevents::ScalarEvents<T>;
                match scalar_type {
                    U8 => Box::new(K::<u8>::empty()),
                    U16 => Box::new(K::<u16>::empty()),
                    U32 => Box::new(K::<u32>::empty()),
                    U64 => Box::new(K::<u64>::empty()),
                    I8 => Box::new(K::<i8>::empty()),
                    I16 => Box::new(K::<i16>::empty()),
                    I32 => Box::new(K::<i32>::empty()),
                    I64 => Box::new(K::<i64>::empty()),
                    F32 => Box::new(K::<f32>::empty()),
                    F64 => Box::new(K::<f64>::empty()),
                    _ => err::todoval(),
                }
            }
            _ => err::todoval(),
        },
        Shape::Wave(_n) => match agg_kind {
            AggKind::DimXBins1 => {
                use ScalarType::*;
                type K<T> = waveevents::WaveEvents<T>;
                match scalar_type {
                    U8 => Box::new(K::<u8>::empty()),
                    F32 => Box::new(K::<f32>::empty()),
                    F64 => Box::new(K::<f64>::empty()),
                    _ => err::todoval(),
                }
            }
            _ => err::todoval(),
        },
        Shape::Image(..) => err::todoval(),
    }
}

pub fn empty_binned_dyn(scalar_type: &ScalarType, shape: &Shape, agg_kind: &AggKind) -> Box<dyn TimeBinnableDyn> {
    match shape {
        Shape::Scalar => match agg_kind {
            AggKind::TimeWeightedScalar => {
                use ScalarType::*;
                type K<T> = binsdim0::MinMaxAvgDim0Bins<T>;
                match scalar_type {
                    U8 => Box::new(K::<u8>::empty()),
                    U16 => Box::new(K::<u16>::empty()),
                    U32 => Box::new(K::<u32>::empty()),
                    U64 => Box::new(K::<u64>::empty()),
                    I8 => Box::new(K::<i8>::empty()),
                    I16 => Box::new(K::<i16>::empty()),
                    I32 => Box::new(K::<i32>::empty()),
                    I64 => Box::new(K::<i64>::empty()),
                    F32 => Box::new(K::<f32>::empty()),
                    F64 => Box::new(K::<f64>::empty()),
                    _ => err::todoval(),
                }
            }
            _ => err::todoval(),
        },
        Shape::Wave(_n) => match agg_kind {
            AggKind::DimXBins1 => {
                use ScalarType::*;
                type K<T> = binsdim0::MinMaxAvgDim0Bins<T>;
                match scalar_type {
                    U8 => Box::new(K::<u8>::empty()),
                    F32 => Box::new(K::<f32>::empty()),
                    F64 => Box::new(K::<f64>::empty()),
                    _ => err::todoval(),
                }
            }
            _ => err::todoval(),
        },
        Shape::Image(..) => err::todoval(),
    }
}

#[test]
fn bin_binned_01() {
    use binsdim0::MinMaxAvgDim0Bins;
    let edges = vec![SEC * 1000, SEC * 1010, SEC * 1020, SEC * 1030];
    let inp0 = <MinMaxAvgDim0Bins<u32> as NewEmpty>::empty(Shape::Scalar);
    let mut time_binner = inp0.time_binner_new(edges, true);
    let inp1 = MinMaxAvgDim0Bins::<u32> {
        ts1s: vec![SEC * 1000, SEC * 1010],
        ts2s: vec![SEC * 1010, SEC * 1020],
        counts: vec![1, 1],
        mins: vec![3, 4],
        maxs: vec![10, 9],
        avgs: vec![7., 6.],
    };
    assert_eq!(time_binner.bins_ready_count(), 0);
    time_binner.ingest(&inp1);
    assert_eq!(time_binner.bins_ready_count(), 1);
    time_binner.push_in_progress(false);
    assert_eq!(time_binner.bins_ready_count(), 2);
    // From here on, pushing any more should not change the bin count:
    time_binner.push_in_progress(false);
    assert_eq!(time_binner.bins_ready_count(), 2);
    // On the other hand, cycling should add one more zero-bin:
    time_binner.cycle();
    assert_eq!(time_binner.bins_ready_count(), 3);
    time_binner.cycle();
    assert_eq!(time_binner.bins_ready_count(), 3);
    let bins = time_binner.bins_ready().expect("bins should be ready");
    eprintln!("bins: {:?}", bins);
    assert_eq!(time_binner.bins_ready_count(), 0);
    assert_eq!(bins.counts(), &[1, 1, 0]);
    // TODO use proper float-compare logic:
    assert_eq!(bins.mins(), &[3., 4., 0.]);
    assert_eq!(bins.maxs(), &[10., 9., 0.]);
    assert_eq!(bins.avgs(), &[7., 6., 0.]);
}

#[test]
fn bin_binned_02() {
    use binsdim0::MinMaxAvgDim0Bins;
    let edges = vec![SEC * 1000, SEC * 1020];
    let inp0 = <MinMaxAvgDim0Bins<u32> as NewEmpty>::empty(Shape::Scalar);
    let mut time_binner = inp0.time_binner_new(edges, true);
    let inp1 = MinMaxAvgDim0Bins::<u32> {
        ts1s: vec![SEC * 1000, SEC * 1010],
        ts2s: vec![SEC * 1010, SEC * 1020],
        counts: vec![1, 1],
        mins: vec![3, 4],
        maxs: vec![10, 9],
        avgs: vec![7., 6.],
    };
    assert_eq!(time_binner.bins_ready_count(), 0);
    time_binner.ingest(&inp1);
    assert_eq!(time_binner.bins_ready_count(), 0);
    time_binner.cycle();
    assert_eq!(time_binner.bins_ready_count(), 1);
    time_binner.cycle();
    //assert_eq!(time_binner.bins_ready_count(), 2);
    let bins = time_binner.bins_ready().expect("bins should be ready");
    eprintln!("bins: {:?}", bins);
    assert_eq!(time_binner.bins_ready_count(), 0);
    assert_eq!(bins.counts(), &[2]);
    assert_eq!(bins.mins(), &[3.]);
    assert_eq!(bins.maxs(), &[10.]);
    assert_eq!(bins.avgs(), &[13. / 2.]);
}
