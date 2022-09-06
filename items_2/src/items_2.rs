pub mod binsdim0;
pub mod eventsdim0;
pub mod streams;
#[cfg(test)]
pub mod test;

use chrono::{DateTime, TimeZone, Utc};
use futures_util::Stream;
use netpod::log::*;
use netpod::timeunits::*;
use netpod::{AggKind, NanoRange, ScalarType, Shape};
use serde::{Deserialize, Serialize, Serializer};
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::{Context, Poll};
use streams::Collectable;

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

#[derive(Debug, PartialEq)]
pub enum ErrorKind {
    #[allow(unused)]
    MismatchedType,
}

#[derive(Debug, PartialEq)]
pub struct Error {
    #[allow(unused)]
    kind: ErrorKind,
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { kind }
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
pub trait Events: fmt::Debug + Any + Collectable + TimeBinnable {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnable;
    fn verify(&self);
    fn output_info(&self);
    fn as_collectable_mut(&mut self) -> &mut dyn Collectable;
    fn ts_min(&self) -> Option<u64>;
    fn take_new_events_until_ts(&mut self, ts_end: u64) -> Box<dyn Events>;
    fn partial_eq_dyn(&self, other: &dyn Events) -> bool;
}

impl PartialEq for Box<dyn Events> {
    fn eq(&self, other: &Self) -> bool {
        Events::partial_eq_dyn(self.as_ref(), other.as_ref())
    }
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

#[derive(Clone, Debug, PartialEq)]
pub enum ConnStatus {
    Connect,
    Disconnect,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConnStatusEvent {
    pub ts: u64,
    pub status: ConnStatus,
}

trait MergableEvents: Any {
    fn ts_min(&self) -> Option<u64>;
    fn ts_max(&self) -> Option<u64>;
}

impl<T: MergableEvents> MergableEvents for Box<T> {
    fn ts_min(&self) -> Option<u64> {
        todo!()
    }

    fn ts_max(&self) -> Option<u64> {
        todo!()
    }
}

#[derive(Debug)]
pub enum ChannelEvents {
    Events(Box<dyn Events>),
    Status(ConnStatusEvent),
    RangeComplete,
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

impl ChannelEvents {
    fn forget_after_merge_process(&self) -> bool {
        use ChannelEvents::*;
        match self {
            Events(k) => k.len() == 0,
            Status(_) => true,
            RangeComplete => true,
        }
    }
}

impl MergableEvents for ChannelEvents {
    fn ts_min(&self) -> Option<u64> {
        use ChannelEvents::*;
        match self {
            Events(k) => k.ts_min(),
            Status(k) => Some(k.ts),
            RangeComplete => panic!(),
        }
    }

    fn ts_max(&self) -> Option<u64> {
        error!("TODO MergableEvents");
        todo!()
    }
}

pub struct ChannelEventsMerger {
    inp1: Pin<Box<dyn Stream<Item = Result<ChannelEvents, Error>>>>,
    inp2: Pin<Box<dyn Stream<Item = Result<ChannelEvents, Error>>>>,
    inp1_done: bool,
    inp2_done: bool,
    inp1_item: Option<ChannelEvents>,
    inp2_item: Option<ChannelEvents>,
    out: Option<ChannelEvents>,
    range_complete: bool,
    done: bool,
    complete: bool,
}

impl ChannelEventsMerger {
    pub fn new(
        inp1: Pin<Box<dyn Stream<Item = Result<ChannelEvents, Error>>>>,
        inp2: Pin<Box<dyn Stream<Item = Result<ChannelEvents, Error>>>>,
    ) -> Self {
        Self {
            done: false,
            complete: false,
            inp1,
            inp2,
            inp1_done: false,
            inp2_done: false,
            inp1_item: None,
            inp2_item: None,
            out: None,
            range_complete: false,
        }
    }

    fn handle_no_ts_event(item: &mut Option<ChannelEvents>, range_complete: &mut bool) -> bool {
        match item {
            Some(k) => match k {
                ChannelEvents::Events(_) => false,
                ChannelEvents::Status(_) => false,
                ChannelEvents::RangeComplete => {
                    *range_complete = true;
                    item.take();
                    true
                }
            },
            None => false,
        }
    }

    fn process(mut self: Pin<&mut Self>, _cx: &mut Context) -> ControlFlow<Poll<Option<<Self as Stream>::Item>>> {
        eprintln!("process {self:?}");
        use ControlFlow::*;
        use Poll::*;
        // Some event types have no timestamp.
        let gg: &mut ChannelEventsMerger = &mut self;
        let &mut ChannelEventsMerger {
            inp1_item: ref mut item,
            range_complete: ref mut raco,
            ..
        } = gg;
        if Self::handle_no_ts_event(item, raco) {
            return Continue(());
        }
        let &mut ChannelEventsMerger {
            inp2_item: ref mut item,
            range_complete: ref mut raco,
            ..
        } = gg;
        if Self::handle_no_ts_event(item, raco) {
            return Continue(());
        }

        // Find the two lowest ts.
        let mut tsj = [None, None];
        for (i1, item) in [&self.inp1_item, &self.inp2_item].into_iter().enumerate() {
            if let Some(a) = &item {
                if let Some(tsmin) = a.ts_min() {
                    if let Some((_, k)) = tsj[0] {
                        if tsmin < k {
                            tsj[1] = tsj[0];
                            tsj[0] = Some((i1, tsmin));
                        } else {
                            if let Some((_, k)) = tsj[1] {
                                if tsmin < k {
                                    tsj[1] = Some((i1, tsmin));
                                } else {
                                }
                            } else {
                                tsj[1] = Some((i1, tsmin));
                            }
                        }
                    } else {
                        tsj[0] = Some((i1, tsmin));
                    }
                } else {
                    // TODO design such that this can't occur.
                    warn!("found input item without timestamp");
                    //Break(Ready(Some(Err(Error::))))
                }
            }
        }
        eprintln!("---------- Found lowest: {tsj:?}");

        if tsj[0].is_none() {
            Continue(())
        } else if tsj[1].is_none() {
            let (_ts_min, itemref) = if tsj[0].as_mut().unwrap().0 == 0 {
                (tsj[0].as_ref().unwrap().1, self.inp1_item.as_mut())
            } else {
                (tsj[0].as_ref().unwrap().1, self.inp2_item.as_mut())
            };
            if itemref.is_none() {
                panic!("logic error");
            }
            // Precondition: at least one event is before the requested range.
            use ChannelEvents::*;
            let itemout = match itemref {
                Some(Events(k)) => Events(k.take_new_events_until_ts(u64::MAX)),
                Some(Status(k)) => Status(k.clone()),
                Some(RangeComplete) => RangeComplete,
                None => panic!(),
            };
            {
                // TODO refactor
                if tsj[0].as_mut().unwrap().0 == 0 {
                    if let Some(item) = self.inp1_item.as_ref() {
                        if item.forget_after_merge_process() {
                            self.inp1_item.take();
                        }
                    }
                }
                if tsj[0].as_mut().unwrap().0 == 1 {
                    if let Some(item) = self.inp2_item.as_ref() {
                        if item.forget_after_merge_process() {
                            self.inp2_item.take();
                        }
                    }
                }
            }
            Break(Ready(Some(Ok(itemout))))
        } else {
            let (ts_end, itemref) = if tsj[0].as_mut().unwrap().0 == 0 {
                (tsj[1].as_ref().unwrap().1, self.inp1_item.as_mut())
            } else {
                (tsj[1].as_ref().unwrap().1, self.inp2_item.as_mut())
            };
            if itemref.is_none() {
                panic!("logic error");
            }
            // Precondition: at least one event is before the requested range.
            use ChannelEvents::*;
            let itemout = match itemref {
                Some(Events(k)) => Events(k.take_new_events_until_ts(ts_end)),
                Some(Status(k)) => Status(k.clone()),
                Some(RangeComplete) => RangeComplete,
                None => panic!(),
            };
            {
                // TODO refactor
                if tsj[0].as_mut().unwrap().0 == 0 {
                    if let Some(item) = self.inp1_item.as_ref() {
                        if item.forget_after_merge_process() {
                            self.inp1_item.take();
                        }
                    }
                }
                if tsj[0].as_mut().unwrap().0 == 1 {
                    if let Some(item) = self.inp2_item.as_ref() {
                        if item.forget_after_merge_process() {
                            self.inp2_item.take();
                        }
                    }
                }
            }
            Break(Ready(Some(Ok(itemout))))
        }
    }

    fn poll2(mut self: Pin<&mut Self>, cx: &mut Context) -> ControlFlow<Poll<Option<<Self as Stream>::Item>>> {
        use ControlFlow::*;
        use Poll::*;
        if self.inp1_item.is_none() && !self.inp1_done {
            match Pin::new(&mut self.inp1).poll_next(cx) {
                Ready(Some(Ok(k))) => {
                    self.inp1_item = Some(k);
                    Continue(())
                }
                Ready(Some(Err(e))) => Break(Ready(Some(Err(e)))),
                Ready(None) => {
                    self.inp1_done = true;
                    Continue(())
                }
                Pending => Break(Pending),
            }
        } else if self.inp2_item.is_none() && !self.inp2_done {
            match Pin::new(&mut self.inp2).poll_next(cx) {
                Ready(Some(Ok(k))) => {
                    self.inp2_item = Some(k);
                    Continue(())
                }
                Ready(Some(Err(e))) => Break(Ready(Some(Err(e)))),
                Ready(None) => {
                    self.inp2_done = true;
                    Continue(())
                }
                Pending => Break(Pending),
            }
        } else if self.inp1_item.is_some() || self.inp2_item.is_some() {
            let process_res = Self::process(self.as_mut(), cx);
            match process_res {
                Continue(()) => Continue(()),
                Break(k) => Break(k),
            }
        } else {
            self.done = true;
            Continue(())
        }
    }
}

impl fmt::Debug for ChannelEventsMerger {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct(std::any::type_name::<Self>())
            .field("inp1_done", &self.inp1_done)
            .field("inp2_done", &self.inp2_done)
            .field("inp1_item", &self.inp1_item)
            .field("inp2_item", &self.inp2_item)
            .field("out", &self.out)
            .field("range_complete", &self.range_complete)
            .field("done", &self.done)
            .field("complete", &self.complete)
            .finish()
    }
}

impl Stream for ChannelEventsMerger {
    type Item = Result<ChannelEvents, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        eprintln!("ChannelEventsMerger  poll_next");
        loop {
            break if self.complete {
                panic!("poll after complete");
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else {
                match Self::poll2(self.as_mut(), cx) {
                    ControlFlow::Continue(()) => continue,
                    ControlFlow::Break(k) => break k,
                }
            };
        }
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
