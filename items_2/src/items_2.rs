pub mod binsdim0;
pub mod eventsdim0;
pub mod streams;

use chrono::{DateTime, TimeZone, Utc};
use futures_util::Stream;
use netpod::log::error;
use netpod::timeunits::*;
use netpod::{AggKind, NanoRange, ScalarType, Shape};
use serde::{Deserialize, Serialize, Serializer};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use streams::Collectable;

pub fn bool_is_false(x: &bool) -> bool {
    *x == false
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

#[allow(unused)]
const fn is_nan_int<T>(_x: &T) -> bool {
    false
}

#[allow(unused)]
fn is_nan_f32(x: f32) -> bool {
    x.is_nan()
}

#[allow(unused)]
fn is_nan_f64(x: f64) -> bool {
    x.is_nan()
}

pub trait AsPrimF32 {
    fn as_prim_f32(&self) -> f32;
}

macro_rules! impl_as_prim_f32 {
    ($ty:ident) => {
        impl AsPrimF32 for $ty {
            fn as_prim_f32(&self) -> f32 {
                *self as f32
            }
        }
    };
}

impl_as_prim_f32!(u8);
impl_as_prim_f32!(u16);
impl_as_prim_f32!(u32);
impl_as_prim_f32!(u64);
impl_as_prim_f32!(i8);
impl_as_prim_f32!(i16);
impl_as_prim_f32!(i32);
impl_as_prim_f32!(i64);
impl_as_prim_f32!(f32);
impl_as_prim_f32!(f64);

pub trait ScalarOps: fmt::Debug + Clone + PartialOrd + AsPrimF32 + Serialize + Unpin + Send + 'static {
    fn zero() -> Self;
}

macro_rules! impl_num_ops {
    ($ty:ident, $zero:expr) => {
        impl ScalarOps for $ty {
            fn zero() -> Self {
                $zero
            }
        }
    };
}

impl_num_ops!(u8, 0);
impl_num_ops!(u16, 0);
impl_num_ops!(u32, 0);
impl_num_ops!(u64, 0);
impl_num_ops!(i8, 0);
impl_num_ops!(i16, 0);
impl_num_ops!(i32, 0);
impl_num_ops!(i64, 0);
impl_num_ops!(f32, 0.);
impl_num_ops!(f64, 0.);

#[allow(unused)]
struct Ts(u64);

struct Error {
    #[allow(unused)]
    kind: ErrorKind,
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { kind }
    }
}

enum ErrorKind {
    #[allow(unused)]
    MismatchedType,
}

pub trait WithLen {
    fn len(&self) -> usize;
}

pub trait TimeSeries {
    fn ts_min(&self) -> Option<u64>;
    fn ts_max(&self) -> Option<u64>;
    fn ts_min_max(&self) -> Option<(u64, u64)>;
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

// TODO can this be removed?
pub trait FitsInside {
    fn fits_inside(&self, range: NanoRange) -> Fits;
}

impl<T> FitsInside for T
where
    T: TimeSeries,
{
    fn fits_inside(&self, range: NanoRange) -> Fits {
        if let Some((min, max)) = self.ts_min_max() {
            if max <= range.beg {
                Fits::Lower
            } else if min >= range.end {
                Fits::Greater
            } else if min < range.beg && max > range.end {
                Fits::PartlyLowerAndGreater
            } else if min < range.beg {
                Fits::PartlyLower
            } else if max > range.end {
                Fits::PartlyGreater
            } else {
                Fits::Inside
            }
        } else {
            Fits::Empty
        }
    }
}

pub trait RangeOverlapInfo {
    fn ends_before(&self, range: NanoRange) -> bool;
    fn ends_after(&self, range: NanoRange) -> bool;
    fn starts_after(&self, range: NanoRange) -> bool;
}

impl<T> RangeOverlapInfo for T
where
    T: TimeSeries,
{
    fn ends_before(&self, range: NanoRange) -> bool {
        if let Some(max) = self.ts_max() {
            max <= range.beg
        } else {
            true
        }
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        if let Some(max) = self.ts_max() {
            max > range.end
        } else {
            true
        }
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        if let Some(min) = self.ts_min() {
            min >= range.end
        } else {
            true
        }
    }
}

pub trait EmptyForScalarTypeShape {
    fn empty(scalar_type: ScalarType, shape: Shape) -> Self;
}

pub trait EmptyForShape {
    fn empty(shape: Shape) -> Self;
}

pub trait Empty {
    fn empty() -> Self;
}

pub trait AppendEmptyBin {
    fn append_empty_bin(&mut self, ts1: u64, ts2: u64);
}

#[derive(Clone, Debug, Deserialize)]
pub struct IsoDateTime(DateTime<Utc>);

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

pub trait TimeBinner: Send {
    fn bins_ready_count(&self) -> usize;
    fn bins_ready(&mut self) -> Option<Box<dyn TimeBinned>>;
    fn ingest(&mut self, item: &dyn TimeBinnable);

    /// If there is a bin in progress with non-zero count, push it to the result set.
    /// With push_empty == true, a bin in progress is pushed even if it contains no counts.
    fn push_in_progress(&mut self, push_empty: bool);

    /// Implies `Self::push_in_progress` but in addition, pushes a zero-count bin if the call
    /// to `push_in_progress` did not change the result count, as long as edges are left.
    /// The next call to `Self::bins_ready_count` must return one higher count than before.
    fn cycle(&mut self);
}

/// Provides a time-binned representation of the implementing type.
/// In contrast to `TimeBinnableType` this is meant for trait objects.
pub trait TimeBinnable: WithLen + RangeOverlapInfo + Any + Send {
    fn time_binner_new(&self, edges: Vec<u64>, do_time_weight: bool) -> Box<dyn TimeBinner>;
    fn as_any(&self) -> &dyn Any;
}

/// Container of some form of events, for use as trait object.
pub trait Events: Collectable + TimeBinnable {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnable;
    fn verify(&self);
    fn output_info(&self);
    fn as_collectable_mut(&mut self) -> &mut dyn Collectable;
}

/// Data in time-binned form.
pub trait TimeBinned: TimeBinnable {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnable;
    fn edges_slice(&self) -> (&[u64], &[u64]);
    fn counts(&self) -> &[u64];
    fn mins(&self) -> Vec<f32>;
    fn maxs(&self) -> Vec<f32>;
    fn avgs(&self) -> Vec<f32>;
    fn validate(&self) -> Result<(), String>;
}

pub trait TimeBinnableType: Send + Unpin + RangeOverlapInfo {
    type Output: TimeBinnableType;
    type Aggregator: TimeBinnableTypeAggregator<Input = Self, Output = Self::Output> + Send + Unpin;
    fn aggregator(range: NanoRange, bin_count: usize, do_time_weight: bool) -> Self::Aggregator;
}

pub trait TimeBinnableTypeAggregator: Send {
    type Input: TimeBinnableType;
    type Output: TimeBinnableType;
    fn range(&self) -> &NanoRange;
    fn ingest(&mut self, item: &Self::Input);
    fn result_reset(&mut self, range: NanoRange, expand: bool) -> Self::Output;
}

pub fn empty_events_dyn(scalar_type: &ScalarType, shape: &Shape, agg_kind: &AggKind) -> Box<dyn TimeBinnable> {
    match shape {
        Shape::Scalar => match agg_kind {
            AggKind::TimeWeightedScalar => {
                use ScalarType::*;
                type K<T> = eventsdim0::EventsDim0<T>;
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
                    _ => {
                        error!("TODO empty_events_dyn");
                        err::todoval()
                    }
                }
            }
            _ => {
                error!("TODO empty_events_dyn");
                err::todoval()
            }
        },
        Shape::Wave(..) => {
            error!("TODO empty_events_dyn");
            err::todoval()
        }
        Shape::Image(..) => {
            error!("TODO empty_events_dyn");
            err::todoval()
        }
    }
}

pub fn empty_binned_dyn(scalar_type: &ScalarType, shape: &Shape, agg_kind: &AggKind) -> Box<dyn TimeBinnable> {
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
                    _ => {
                        error!("TODO empty_binned_dyn");
                        err::todoval()
                    }
                }
            }
            _ => {
                error!("TODO empty_binned_dyn");
                err::todoval()
            }
        },
        Shape::Wave(_n) => match agg_kind {
            AggKind::DimXBins1 => {
                use ScalarType::*;
                type K<T> = binsdim0::MinMaxAvgDim0Bins<T>;
                match scalar_type {
                    U8 => Box::new(K::<u8>::empty()),
                    F32 => Box::new(K::<f32>::empty()),
                    F64 => Box::new(K::<f64>::empty()),
                    _ => {
                        error!("TODO empty_binned_dyn");
                        err::todoval()
                    }
                }
            }
            _ => {
                error!("TODO empty_binned_dyn");
                err::todoval()
            }
        },
        Shape::Image(..) => {
            error!("TODO empty_binned_dyn");
            err::todoval()
        }
    }
}

pub enum ConnStatus {}

pub struct ConnStatusEvent {
    pub ts: u64,
    pub status: ConnStatus,
}

trait MergableEvents: Any {
    fn len(&self) -> usize;
    fn ts_min(&self) -> Option<u64>;
    fn ts_max(&self) -> Option<u64>;
    fn take_from(&mut self, src: &mut dyn MergableEvents, ts_end: u64) -> Result<(), Error>;
}

pub enum ChannelEvents {
    Events(Box<dyn Events>),
    Status(ConnStatusEvent),
    RangeComplete,
}

impl MergableEvents for ChannelEvents {
    fn len(&self) -> usize {
        error!("TODO MergableEvents");
        todo!()
    }

    fn ts_min(&self) -> Option<u64> {
        error!("TODO MergableEvents");
        todo!()
    }

    fn ts_max(&self) -> Option<u64> {
        error!("TODO MergableEvents");
        todo!()
    }

    fn take_from(&mut self, _src: &mut dyn MergableEvents, _ts_end: u64) -> Result<(), Error> {
        error!("TODO MergableEvents");
        todo!()
    }
}

struct ChannelEventsMerger {
    _inp_1: Pin<Box<dyn Stream<Item = Result<Box<dyn MergableEvents>, Error>>>>,
    _inp_2: Pin<Box<dyn Stream<Item = Result<Box<dyn MergableEvents>, Error>>>>,
}

impl Stream for ChannelEventsMerger {
    type Item = Result<ChannelEvents, Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        //use Poll::*;
        error!("TODO ChannelEventsMerger");
        err::todoval()
    }
}

// TODO do this with some blanket impl:
impl Collectable for Box<dyn Collectable> {
    fn new_collector(&self) -> Box<dyn streams::Collector> {
        Collectable::new_collector(self.as_ref())
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        Collectable::as_any_mut(self.as_mut())
    }
}
