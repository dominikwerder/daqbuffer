pub mod binsdim0;
pub mod channelevents;
pub mod collect;
pub mod eventsdim0;
pub mod merger;
pub mod merger_cev;
pub mod streams;
#[cfg(test)]
pub mod test;
pub mod testgen;
pub mod timebin;

use crate as items_2;
use crate::streams::Collectable;
use crate::streams::Collector;
use crate::streams::ToJsonResult;
use channelevents::ChannelEvents;
use chrono::{DateTime, TimeZone, Utc};
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items::RangeCompletableItem;
use items::Sitemty;
use items::StreamItem;
use items::SubFrId;
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

pub trait AsAnyRef {
    fn as_any_ref(&self) -> &dyn Any;
}

pub trait AsAnyMut {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/*impl AsAnyRef for Box<dyn AsAnyRef> {
    fn as_any_ref(&self) -> &dyn Any {
        self.as_ref().as_any_ref()
    }
}*/

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

// Helper trait to bridge between impls of event containers during refactoring.
// TODO get rid when no longer needed.
pub trait IntoEvents {
    fn into_events(self) -> Box<dyn Events>;
}

impl<NTY> IntoEvents for items::scalarevents::ScalarEvents<NTY>
where
    NTY: ScalarOps,
{
    fn into_events(self) -> Box<dyn Events> {
        let ret = items_2::eventsdim0::EventsDim0::<NTY> {
            tss: self.tss.into(),
            pulses: self.pulses.into(),
            values: self.values.into(),
        };
        Box::new(ret)
    }
}

// TODO can I remove the Any bound?

/// Container of some form of events, for use as trait object.
pub trait Events:
    fmt::Debug
    + Any
    + Collectable
    + items_2::collect::CollectableWithDefault
    + TimeBinnable
    + Send
    + erased_serde::Serialize
{
    fn as_time_binnable(&self) -> &dyn TimeBinnable;
    fn verify(&self) -> bool;
    fn output_info(&self);
    fn as_collectable_mut(&mut self) -> &mut dyn Collectable;
    fn as_collectable_with_default_ref(&self) -> &dyn crate::collect::CollectableWithDefault;
    fn as_collectable_with_default_mut(&mut self) -> &mut dyn crate::collect::CollectableWithDefault;
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

#[derive(Debug)]
pub struct EventsCollector {
    coll: Box<dyn items_2::collect::CollectorDyn>,
}

impl EventsCollector {
    pub fn new(coll: Box<dyn items_2::collect::CollectorDyn>) -> Self {
        Self { coll }
    }
}

impl items_2::collect::Collector for EventsCollector {
    type Input = Box<dyn Events>;

    // TODO this Output trait does not differentiate between e.g. collected events, collected bins, different aggs, etc...
    type Output = Box<dyn items_2::collect::Collected>;

    fn len(&self) -> usize {
        self.coll.len()
    }

    fn ingest(&mut self, item: &mut Self::Input) {
        self.coll.ingest(item.as_collectable_with_default_mut());
    }

    fn set_range_complete(&mut self) {
        self.coll.set_range_complete()
    }

    fn set_timed_out(&mut self) {
        self.coll.set_timed_out()
    }

    fn result(&mut self) -> Result<Self::Output, Error> {
        self.coll.result()
    }
}

impl items_2::collect::Collectable for Box<dyn Events> {
    type Collector = EventsCollector;

    fn new_collector(&self) -> Self::Collector {
        let coll = items_2::collect::CollectableWithDefault::new_collector(self.as_ref());
        EventsCollector::new(coll)
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
