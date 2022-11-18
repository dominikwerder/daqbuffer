pub mod binsdim0;
pub mod collect;
pub mod eventsdim0;
pub mod merger;
pub mod merger_cev;
pub mod streams;
#[cfg(test)]
pub mod test;
pub mod testgen;
pub mod timebin;

use crate::streams::Collector;
use chrono::{DateTime, TimeZone, Utc};
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items::FrameTypeInnerStatic;
use items::RangeCompletableItem;
use items::Sitemty;
use items::StreamItem;
use items::SubFrId;
use merger_cev::MergeableCev;
use netpod::log::*;
use netpod::timeunits::*;
use netpod::{AggKind, NanoRange, ScalarType, Shape};
use serde::{Deserialize, Serialize, Serializer};
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::time::Duration;
use std::time::Instant;
use streams::Collectable;
use streams::ToJsonResult;

pub fn bool_is_false(x: &bool) -> bool {
    *x == false
}

// TODO take iterator instead of slice, because a VecDeque can't produce a slice in general.
pub fn ts_offs_from_abs(tss: &[u64]) -> (u64, VecDeque<u64>, VecDeque<u64>) {
    let ts_anchor_sec = tss.first().map_or(0, |&k| k) / SEC;
    let ts_anchor_ns = ts_anchor_sec * SEC;
    let ts_off_ms: VecDeque<_> = tss.iter().map(|&k| (k - ts_anchor_ns) / MS).collect();
    let ts_off_ns = tss
        .iter()
        .zip(ts_off_ms.iter().map(|&k| k * MS))
        .map(|(&j, k)| (j - ts_anchor_ns - k))
        .collect();
    (ts_anchor_sec, ts_off_ms, ts_off_ns)
}

pub fn ts_offs_from_abs_with_anchor(ts_anchor_sec: u64, tss: &[u64]) -> (VecDeque<u64>, VecDeque<u64>) {
    let ts_anchor_ns = ts_anchor_sec * SEC;
    let ts_off_ms: VecDeque<_> = tss.iter().map(|&k| (k - ts_anchor_ns) / MS).collect();
    let ts_off_ns = tss
        .iter()
        .zip(ts_off_ms.iter().map(|&k| k * MS))
        .map(|(&j, k)| (j - ts_anchor_ns - k))
        .collect();
    (ts_off_ms, ts_off_ns)
}

// TODO take iterator instead of slice, because a VecDeque can't produce a slice in general.
pub fn pulse_offs_from_abs(pulse: &[u64]) -> (u64, VecDeque<u64>) {
    let pulse_anchor = pulse.first().map_or(0, |k| *k);
    let pulse_off = pulse.iter().map(|k| *k - pulse_anchor).collect();
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

pub trait ScalarOps:
    fmt::Debug + Clone + PartialOrd + SubFrId + AsPrimF32 + Serialize + Unpin + Send + 'static
{
    fn zero() -> Self;
    fn equal_slack(&self, rhs: &Self) -> bool;
}

macro_rules! impl_num_ops {
    ($ty:ident, $zero:expr, $equal_slack:ident) => {
        impl ScalarOps for $ty {
            fn zero() -> Self {
                $zero
            }

            fn equal_slack(&self, rhs: &Self) -> bool {
                $equal_slack(*self, *rhs)
            }
        }
    };
}

fn equal_int<T: PartialEq>(a: T, b: T) -> bool {
    a == b
}

fn equal_f32(a: f32, b: f32) -> bool {
    (a - b).abs() < 1e-4 || (a / b > 0.999 && a / b < 1.001)
}

fn equal_f64(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-6 || (a / b > 0.99999 && a / b < 1.00001)
}

impl_num_ops!(u8, 0, equal_int);
impl_num_ops!(u16, 0, equal_int);
impl_num_ops!(u32, 0, equal_int);
impl_num_ops!(u64, 0, equal_int);
impl_num_ops!(i8, 0, equal_int);
impl_num_ops!(i16, 0, equal_int);
impl_num_ops!(i32, 0, equal_int);
impl_num_ops!(i64, 0, equal_int);
impl_num_ops!(f32, 0., equal_f32);
impl_num_ops!(f64, 0., equal_f64);

#[allow(unused)]
struct Ts(u64);

#[derive(Debug, PartialEq)]
pub enum ErrorKind {
    General,
    #[allow(unused)]
    MismatchedType,
}

// TODO stack error better
#[derive(Debug, PartialEq)]
pub struct Error {
    #[allow(unused)]
    kind: ErrorKind,
    msg: Option<String>,
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{self:?}")
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { kind, msg: None }
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Self {
            msg: Some(msg),
            kind: ErrorKind::General,
        }
    }
}

// TODO this discards structure
impl From<err::Error> for Error {
    fn from(e: err::Error) -> Self {
        Self {
            msg: Some(format!("{e}")),
            kind: ErrorKind::General,
        }
    }
}

// TODO this discards structure
impl From<Error> for err::Error {
    fn from(e: Error) -> Self {
        err::Error::with_msg_no_trace(format!("{e}"))
    }
}

impl std::error::Error for Error {}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        format!("{msg}").into()
    }
}

pub trait WithLen {
    fn len(&self) -> usize;
}

// TODO can probably be removed.
pub trait TimeBins {
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

pub trait RangeOverlapInfo {
    fn ends_before(&self, range: NanoRange) -> bool;
    fn ends_after(&self, range: NanoRange) -> bool;
    fn starts_after(&self, range: NanoRange) -> bool;
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

#[derive(Clone, Debug, PartialEq, Deserialize)]
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
    fn ingest(&mut self, item: &dyn TimeBinnable);
    fn bins_ready_count(&self) -> usize;
    fn bins_ready(&mut self) -> Option<Box<dyn TimeBinned>>;

    /// If there is a bin in progress with non-zero count, push it to the result set.
    /// With push_empty == true, a bin in progress is pushed even if it contains no counts.
    fn push_in_progress(&mut self, push_empty: bool);

    /// Implies `Self::push_in_progress` but in addition, pushes a zero-count bin if the call
    /// to `push_in_progress` did not change the result count, as long as edges are left.
    /// The next call to `Self::bins_ready_count` must return one higher count than before.
    fn cycle(&mut self);

    fn set_range_complete(&mut self);
}

// TODO remove the Any bound. Factor out into custom AsAny trait.

/// Provides a time-binned representation of the implementing type.
/// In contrast to `TimeBinnableType` this is meant for trait objects.
pub trait TimeBinnable: fmt::Debug + WithLen + RangeOverlapInfo + Any + Send {
    // TODO implementors may fail if edges contain not at least 2 entries.
    fn time_binner_new(&self, edges: Vec<u64>, do_time_weight: bool) -> Box<dyn TimeBinner>;
    fn as_any(&self) -> &dyn Any;

    // TODO just a helper for the empty result.
    fn to_box_to_json_result(&self) -> Box<dyn ToJsonResult>;
}

// TODO can I remove the Any bound?

/// Container of some form of events, for use as trait object.
pub trait Events: fmt::Debug + Any + Collectable + TimeBinnable + Send + erased_serde::Serialize {
    fn as_time_binnable(&self) -> &dyn TimeBinnable;
    fn verify(&self) -> bool;
    fn output_info(&self);
    fn as_collectable_mut(&mut self) -> &mut dyn Collectable;
    fn ts_min(&self) -> Option<u64>;
    fn ts_max(&self) -> Option<u64>;
    fn take_new_events_until_ts(&mut self, ts_end: u64) -> Box<dyn Events>;
    fn move_into_fresh(&mut self, ts_end: u64) -> Box<dyn Events>;
    fn move_into_existing(&mut self, tgt: &mut Box<dyn Events>, ts_end: u64) -> Result<(), ()>;
    fn clone_dyn(&self) -> Box<dyn Events>;
    fn partial_eq_dyn(&self, other: &dyn Events) -> bool;
    fn serde_id(&self) -> &'static str;
    fn nty_id(&self) -> u32;
}

erased_serde::serialize_trait_object!(Events);

impl crate::merger::Mergeable for Box<dyn Events> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn ts_min(&self) -> Option<u64> {
        self.as_ref().ts_min()
    }

    fn ts_max(&self) -> Option<u64> {
        self.as_ref().ts_max()
    }

    fn is_compatible_target(&self, _tgt: &Self) -> bool {
        // TODO currently unused
        todo!()
    }

    fn move_into_fresh(&mut self, ts_end: u64) -> Self {
        self.as_mut().move_into_fresh(ts_end)
    }

    fn move_into_existing(&mut self, tgt: &mut Self, ts_end: u64) -> Result<(), merger::MergeError> {
        self.as_mut()
            .move_into_existing(tgt, ts_end)
            .map_err(|()| merger::MergeError::NotCompatible)
    }
}

impl PartialEq for Box<dyn Events> {
    fn eq(&self, other: &Self) -> bool {
        Events::partial_eq_dyn(self.as_ref(), other.as_ref())
    }
}

// TODO remove
struct EventsCollector2 {}

impl WithLen for EventsCollector2 {
    fn len(&self) -> usize {
        todo!()
    }
}

// TODO remove
impl Collector for EventsCollector2 {
    fn ingest(&mut self, _src: &mut dyn Collectable) {
        todo!()
    }

    fn set_range_complete(&mut self) {
        todo!()
    }

    fn set_timed_out(&mut self) {
        todo!()
    }

    fn result(&mut self) -> Result<Box<dyn ToJsonResult>, err::Error> {
        todo!()
    }
}

// TODO remove
impl Collectable for Box<dyn Events> {
    fn new_collector(&self) -> Box<dyn Collector> {
        Box::new(EventsCollector2 {})
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        todo!()
    }
}

/*impl crate::streams::CollectorType for EventsCollector {
    type Input = dyn Events;
    type Output = Box<dyn Events>;

    fn ingest(&mut self, src: &mut Self::Input) {
        todo!()
    }

    fn set_range_complete(&mut self) {
        todo!()
    }

    fn set_timed_out(&mut self) {
        todo!()
    }

    fn result(&mut self) -> Result<Self::Output, err::Error> {
        todo!()
    }
}*/

/*impl crate::streams::CollectableType for dyn Events {
    type Collector = EventsCollector;

    fn new_collector() -> Self::Collector {
        todo!()
    }
}*/

/// Data in time-binned form.
pub trait TimeBinned: Any + TimeBinnable {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnable;
    fn as_collectable_mut(&mut self) -> &mut dyn Collectable;
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

pub fn empty_events_dyn_2(scalar_type: &ScalarType, shape: &Shape, agg_kind: &AggKind) -> Box<dyn Events> {
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
                        error!("TODO empty_events_dyn  {scalar_type:?}  {shape:?}  {agg_kind:?}");
                        err::todoval()
                    }
                }
            }
            _ => {
                error!("TODO empty_events_dyn  {scalar_type:?}  {shape:?}  {agg_kind:?}");
                err::todoval()
            }
        },
        Shape::Wave(..) => {
            error!("TODO empty_events_dyn  {scalar_type:?}  {shape:?}  {agg_kind:?}");
            err::todoval()
        }
        Shape::Image(..) => {
            error!("TODO empty_events_dyn  {scalar_type:?}  {shape:?}  {agg_kind:?}");
            err::todoval()
        }
    }
}

// TODO needed any longer?
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
                        error!("TODO empty_events_dyn  {scalar_type:?}  {shape:?}  {agg_kind:?}");
                        err::todoval()
                    }
                }
            }
            _ => {
                error!("TODO empty_events_dyn  {scalar_type:?}  {shape:?}  {agg_kind:?}");
                err::todoval()
            }
        },
        Shape::Wave(..) => {
            error!("TODO empty_events_dyn  {scalar_type:?}  {shape:?}  {agg_kind:?}");
            err::todoval()
        }
        Shape::Image(..) => {
            error!("TODO empty_events_dyn  {scalar_type:?}  {shape:?}  {agg_kind:?}");
            err::todoval()
        }
    }
}

pub fn empty_binned_dyn(scalar_type: &ScalarType, shape: &Shape, agg_kind: &AggKind) -> Box<dyn TimeBinnable> {
    match shape {
        Shape::Scalar => match agg_kind {
            AggKind::TimeWeightedScalar => {
                use ScalarType::*;
                type K<T> = binsdim0::BinsDim0<T>;
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
                type K<T> = binsdim0::BinsDim0<T>;
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

// TODO maybe rename to ChannelStatus?
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConnStatus {
    Connect,
    Disconnect,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnStatusEvent {
    pub ts: u64,
    pub status: ConnStatus,
}

impl ConnStatusEvent {
    pub fn new(ts: u64, status: ConnStatus) -> Self {
        Self { ts, status }
    }
}

/// Events on a channel consist not only of e.g. timestamped values, but can be also
/// connection status changes.
#[derive(Debug)]
pub enum ChannelEvents {
    Events(Box<dyn Events>),
    Status(ConnStatusEvent),
}

impl FrameTypeInnerStatic for ChannelEvents {
    const FRAME_TYPE_ID: u32 = items::ITEMS_2_CHANNEL_EVENTS_FRAME_TYPE_ID;
}

impl Clone for ChannelEvents {
    fn clone(&self) -> Self {
        match self {
            Self::Events(arg0) => Self::Events(arg0.clone_dyn()),
            Self::Status(arg0) => Self::Status(arg0.clone()),
        }
    }
}

mod serde_channel_events {
    use super::{ChannelEvents, Events};
    use crate::eventsdim0::EventsDim0;
    use serde::de::{self, EnumAccess, VariantAccess, Visitor};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;

    impl Serialize for ChannelEvents {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let name = "ChannelEvents";
            match self {
                ChannelEvents::Events(obj) => {
                    use serde::ser::SerializeTupleVariant;
                    let mut ser = serializer.serialize_tuple_variant(name, 0, "Events", 3)?;
                    ser.serialize_field(obj.serde_id())?;
                    ser.serialize_field(&obj.nty_id())?;
                    ser.serialize_field(obj)?;
                    ser.end()
                }
                ChannelEvents::Status(val) => serializer.serialize_newtype_variant(name, 1, "Status", val),
            }
        }
    }

    struct EventsBoxVisitor;

    impl<'de> Visitor<'de> for EventsBoxVisitor {
        type Value = Box<dyn Events>;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "Events object")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            use items::SubFrId;
            let e0: &str = seq.next_element()?.ok_or(de::Error::missing_field("ty .0"))?;
            let e1: u32 = seq.next_element()?.ok_or(de::Error::missing_field("nty .1"))?;
            if e0 == EventsDim0::<u8>::serde_id() {
                match e1 {
                    f32::SUB => {
                        let obj: EventsDim0<f32> = seq.next_element()?.ok_or(de::Error::missing_field("obj .2"))?;
                        Ok(Box::new(obj))
                    }
                    _ => Err(de::Error::custom(&format!("unknown nty {e1}"))),
                }
            } else {
                Err(de::Error::custom(&format!("unknown ty {e0}")))
            }
        }
    }

    pub struct ChannelEventsVisitor;

    impl ChannelEventsVisitor {
        fn name() -> &'static str {
            "ChannelEvents"
        }

        fn allowed_variants() -> &'static [&'static str] {
            &["Events", "Status", "RangeComplete"]
        }
    }

    impl<'de> Visitor<'de> for ChannelEventsVisitor {
        type Value = ChannelEvents;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "ChannelEvents")
        }

        fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
        where
            A: EnumAccess<'de>,
        {
            let (id, var) = data.variant()?;
            match id {
                "Events" => {
                    let c = var.tuple_variant(3, EventsBoxVisitor)?;
                    Ok(Self::Value::Events(c))
                }
                _ => return Err(de::Error::unknown_variant(id, Self::allowed_variants())),
            }
        }
    }

    impl<'de> Deserialize<'de> for ChannelEvents {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            de.deserialize_enum(
                ChannelEventsVisitor::name(),
                ChannelEventsVisitor::allowed_variants(),
                ChannelEventsVisitor,
            )
        }
    }
}

#[cfg(test)]
mod test_channel_events_serde {
    use super::ChannelEvents;
    use crate::eventsdim0::EventsDim0;
    use crate::Empty;

    #[test]
    fn channel_events() {
        let mut evs = EventsDim0::empty();
        evs.push(8, 2, 3.0f32);
        evs.push(12, 3, 3.2f32);
        let item = ChannelEvents::Events(Box::new(evs));
        let s = serde_json::to_string_pretty(&item).unwrap();
        eprintln!("{s}");
        let w: ChannelEvents = serde_json::from_str(&s).unwrap();
        eprintln!("{w:?}");
    }
}

impl PartialEq for ChannelEvents {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Events(l0), Self::Events(r0)) => l0 == r0,
            (Self::Status(l0), Self::Status(r0)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl MergeableCev for ChannelEvents {
    fn ts_min(&self) -> Option<u64> {
        use ChannelEvents::*;
        match self {
            Events(k) => k.ts_min(),
            Status(k) => Some(k.ts),
        }
    }

    fn ts_max(&self) -> Option<u64> {
        error!("TODO impl MergableEvents for ChannelEvents");
        err::todoval()
    }
}

impl crate::merger::Mergeable for ChannelEvents {
    fn len(&self) -> usize {
        match self {
            ChannelEvents::Events(k) => k.len(),
            ChannelEvents::Status(_) => 1,
        }
    }

    fn ts_min(&self) -> Option<u64> {
        match self {
            ChannelEvents::Events(k) => k.ts_min(),
            ChannelEvents::Status(k) => Some(k.ts),
        }
    }

    fn ts_max(&self) -> Option<u64> {
        match self {
            ChannelEvents::Events(k) => k.ts_max(),
            ChannelEvents::Status(k) => Some(k.ts),
        }
    }

    fn is_compatible_target(&self, tgt: &Self) -> bool {
        use ChannelEvents::*;
        match self {
            Events(_) => {
                // TODO better to delegate this to inner type?
                if let Events(_) = tgt {
                    true
                } else {
                    false
                }
            }
            Status(_) => {
                // TODO better to delegate this to inner type?
                if let Status(_) = tgt {
                    true
                } else {
                    false
                }
            }
        }
    }

    fn move_into_fresh(&mut self, ts_end: u64) -> Self {
        match self {
            ChannelEvents::Events(k) => ChannelEvents::Events(k.move_into_fresh(ts_end)),
            ChannelEvents::Status(k) => ChannelEvents::Status(k.clone()),
        }
    }

    fn move_into_existing(&mut self, tgt: &mut Self, ts_end: u64) -> Result<(), merger::MergeError> {
        match self {
            ChannelEvents::Events(k) => match tgt {
                ChannelEvents::Events(tgt) => k.move_into_existing(tgt, ts_end),
                ChannelEvents::Status(_) => Err(merger::MergeError::NotCompatible),
            },
            ChannelEvents::Status(_) => match tgt {
                ChannelEvents::Events(_) => Err(merger::MergeError::NotCompatible),
                ChannelEvents::Status(_) => Err(merger::MergeError::Full),
            },
        }
    }
}

impl Collectable for ChannelEvents {
    fn new_collector(&self) -> Box<dyn Collector> {
        match self {
            ChannelEvents::Events(_item) => todo!(),
            ChannelEvents::Status(_) => todo!(),
        }
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct ChannelEventsTimeBinner {
    // TODO `ConnStatus` contains all the changes that can happen to a connection, but
    // here we would rather require a simplified current state for binning purpose.
    edges: Vec<u64>,
    do_time_weight: bool,
    conn_state: ConnStatus,
    binner: Option<Box<dyn crate::TimeBinner>>,
}

impl fmt::Debug for ChannelEventsTimeBinner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelEventsTimeBinner")
            .field("conn_state", &self.conn_state)
            .finish()
    }
}

impl crate::timebin::TimeBinner for ChannelEventsTimeBinner {
    type Input = ChannelEvents;
    type Output = Box<dyn crate::TimeBinned>;

    fn ingest(&mut self, item: &mut Self::Input) {
        match item {
            ChannelEvents::Events(item) => {
                if self.binner.is_none() {
                    let binner = item.time_binner_new(self.edges.clone(), self.do_time_weight);
                    self.binner = Some(binner);
                }
                match self.binner.as_mut() {
                    Some(binner) => binner.ingest(item.as_time_binnable()),
                    None => {
                        error!("ingest without active binner item {item:?}");
                        ()
                    }
                }
            }
            ChannelEvents::Status(item) => {
                warn!("TODO consider channel status in time binning {item:?}");
            }
        }
    }

    fn set_range_complete(&mut self) {
        match self.binner.as_mut() {
            Some(binner) => binner.set_range_complete(),
            None => (),
        }
    }

    fn bins_ready_count(&self) -> usize {
        match &self.binner {
            Some(binner) => binner.bins_ready_count(),
            None => 0,
        }
    }

    fn bins_ready(&mut self) -> Option<Self::Output> {
        match self.binner.as_mut() {
            Some(binner) => binner.bins_ready(),
            None => None,
        }
    }

    fn push_in_progress(&mut self, push_empty: bool) {
        match self.binner.as_mut() {
            Some(binner) => binner.push_in_progress(push_empty),
            None => (),
        }
    }

    fn cycle(&mut self) {
        match self.binner.as_mut() {
            Some(binner) => binner.cycle(),
            None => (),
        }
    }
}

impl crate::timebin::TimeBinnable for ChannelEvents {
    type TimeBinner = ChannelEventsTimeBinner;

    fn time_binner_new(&self, edges: Vec<u64>, do_time_weight: bool) -> Self::TimeBinner {
        let (binner, status) = match self {
            ChannelEvents::Events(_events) => (None, ConnStatus::Connect),
            ChannelEvents::Status(status) => (None, status.status.clone()),
        };
        ChannelEventsTimeBinner {
            edges,
            do_time_weight,
            conn_state: status,
            binner,
        }
    }
}

#[derive(Debug)]
pub struct EventsCollector {
    coll: Option<Box<()>>,
}

impl EventsCollector {
    pub fn new() -> Self {
        Self { coll: Box::new(()) }
    }
}

impl crate::collect::Collector for EventsCollector {
    type Input = Box<dyn Events>;
    type Output = Box<dyn Events>;

    fn len(&self) -> usize {
        todo!()
    }

    fn ingest(&mut self, item: &mut Self::Input) {
        todo!()
    }

    fn set_range_complete(&mut self) {
        todo!()
    }

    fn set_timed_out(&mut self) {
        todo!()
    }

    fn result(&mut self) -> Result<Self::Output, Error> {
        todo!()
    }
}

impl crate::collect::Collectable for Box<dyn Events> {
    type Collector = EventsCollector;

    fn new_collector(&self) -> Self::Collector {
        Collectable::new_collector(self)
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

fn flush_binned(
    binner: &mut Box<dyn TimeBinner>,
    coll: &mut Option<Box<dyn Collector>>,
    force: bool,
) -> Result<(), Error> {
    trace!("flush_binned  bins_ready_count: {}", binner.bins_ready_count());
    if force {
        if binner.bins_ready_count() == 0 {
            debug!("cycle the binner forced");
            binner.cycle();
        } else {
            debug!("bins ready, do not force");
        }
    }
    if binner.bins_ready_count() > 0 {
        let ready = binner.bins_ready();
        match ready {
            Some(mut ready) => {
                trace!("binned_collected ready {ready:?}");
                if coll.is_none() {
                    *coll = Some(ready.as_collectable_mut().new_collector());
                }
                let cl = coll.as_mut().unwrap();
                cl.ingest(ready.as_collectable_mut());
                Ok(())
            }
            None => Err(format!("bins_ready_count but no result").into()),
        }
    } else {
        Ok(())
    }
}

// TODO remove
pub async fn binned_collected(
    scalar_type: ScalarType,
    shape: Shape,
    agg_kind: AggKind,
    edges: Vec<u64>,
    timeout: Duration,
    inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>,
) -> Result<Box<dyn ToJsonResult>, Error> {
    event!(Level::TRACE, "binned_collected");
    if edges.len() < 2 {
        return Err(format!("binned_collected  but edges.len() {}", edges.len()).into());
    }
    let ts_edges_max = *edges.last().unwrap();
    let deadline = Instant::now() + timeout;
    let mut did_timeout = false;
    // TODO use a trait to allow check of unfinished data [hcn2956jxhwsf]
    #[allow(unused)]
    let bin_count_exp = edges.len().max(2) as u32 - 1;
    let do_time_weight = agg_kind.do_time_weighted();
    // TODO maybe TimeBinner should take all ChannelEvents and handle this?
    let mut did_range_complete = false;
    let mut coll = None;
    let mut binner = None;
    let empty_item = empty_events_dyn_2(&scalar_type, &shape, &AggKind::TimeWeightedScalar);
    let tmp_item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(ChannelEvents::Events(
        empty_item,
    ))));
    let empty_stream = futures_util::stream::once(futures_util::future::ready(tmp_item));
    let mut stream = empty_stream.chain(inp);
    loop {
        let item = futures_util::select! {
            k = stream.next().fuse() => {
                if let Some(k) = k {
                    k?
                }else {
                    break;
                }
            },
            _ = tokio::time::sleep_until(deadline.into()).fuse() => {
                did_timeout = true;
                break;
            }
        };
        match item {
            StreamItem::DataItem(k) => match k {
                RangeCompletableItem::RangeComplete => {
                    did_range_complete = true;
                }
                RangeCompletableItem::Data(k) => match k {
                    ChannelEvents::Events(events) => {
                        if events.starts_after(NanoRange {
                            beg: 0,
                            end: ts_edges_max,
                        }) {
                        } else {
                            if binner.is_none() {
                                let bb = events.as_time_binnable().time_binner_new(edges.clone(), do_time_weight);
                                binner = Some(bb);
                            }
                            let binner = binner.as_mut().unwrap();
                            binner.ingest(events.as_time_binnable());
                            flush_binned(binner, &mut coll, false)?;
                        }
                    }
                    ChannelEvents::Status(item) => {
                        trace!("{:?}", item);
                    }
                },
            },
            StreamItem::Log(item) => {
                // TODO collect also errors here?
                trace!("{:?}", item);
            }
            StreamItem::Stats(item) => {
                // TODO do something with the stats
                trace!("{:?}", item);
            }
        }
    }
    if let Some(mut binner) = binner {
        if did_range_complete {
            trace!("did_range_complete");
            binner.set_range_complete();
        } else {
            debug!("range not complete");
        }
        if did_timeout {
            warn!("timeout");
        } else {
            trace!("cycle the binner");
            binner.cycle();
        }
        trace!("flush binned");
        flush_binned(&mut binner, &mut coll, false)?;
        if coll.is_none() {
            debug!("force a bin");
            flush_binned(&mut binner, &mut coll, true)?;
        } else {
            trace!("coll is already some");
        }
    } else {
        error!("no binner, should always have one");
    }
    match coll {
        Some(mut coll) => {
            let res = coll.result().map_err(|e| format!("{e}"))?;
            tokio::time::sleep(Duration::from_millis(2000)).await;
            Ok(res)
        }
        None => {
            error!("binned_collected nothing collected");
            let item = empty_binned_dyn(&scalar_type, &shape, &AggKind::DimXBins1);
            let ret = item.to_box_to_json_result();
            tokio::time::sleep(Duration::from_millis(2000)).await;
            Ok(ret)
        }
    }
}

pub fn runfut<T, F>(fut: F) -> Result<T, err::Error>
where
    F: std::future::Future<Output = Result<T, Error>>,
{
    use futures_util::TryFutureExt;
    let fut = fut.map_err(|e| e.into());
    taskrun::run(fut)
}
