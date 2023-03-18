pub mod collect_s;
pub mod framable;
pub mod isodate;
pub mod scalar_ops;
pub mod streamitem;
pub mod subfr;
pub mod transform;

pub mod bincode {
    pub use bincode::*;
}

use collect_s::Collectable;
use collect_s::ToJsonResult;
use netpod::range::evrange::SeriesRange;
use netpod::BinnedRangeEnum;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;

pub trait WithLen {
    fn len(&self) -> usize;
}

// TODO can probably be removed.
pub trait TimeBins {
    fn ts_min(&self) -> Option<u64>;
    fn ts_max(&self) -> Option<u64>;
    fn ts_min_max(&self) -> Option<(u64, u64)>;
}

pub trait RangeOverlapInfo {
    fn ends_before(&self, range: &SeriesRange) -> bool;
    fn ends_after(&self, range: &SeriesRange) -> bool;
    fn starts_after(&self, range: &SeriesRange) -> bool;
}

pub trait Empty {
    fn empty() -> Self;
}

pub trait Appendable<STY>: Empty + WithLen {
    fn push(&mut self, ts: u64, pulse: u64, value: STY);
}

pub trait TypeName {
    fn type_name(&self) -> String;
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

impl<T> AsAnyRef for Box<T>
where
    T: AsAnyRef + ?Sized,
{
    fn as_any_ref(&self) -> &dyn Any {
        self.as_ref().as_any_ref()
    }
}

impl<T> AsAnyMut for Box<T>
where
    T: AsAnyMut + ?Sized,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self.as_mut().as_any_mut()
    }
}

/// Data in time-binned form.
pub trait TimeBinned: Any + TimeBinnable + Collectable + erased_serde::Serialize {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnable;
    fn as_collectable_mut(&mut self) -> &mut dyn Collectable;
    fn edges_slice(&self) -> (&[u64], &[u64]);
    fn counts(&self) -> &[u64];
    fn mins(&self) -> Vec<f32>;
    fn maxs(&self) -> Vec<f32>;
    fn avgs(&self) -> Vec<f32>;
    fn validate(&self) -> Result<(), String>;
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

    fn empty(&self) -> Box<dyn TimeBinned>;
}

// TODO remove the Any bound. Factor out into custom AsAny trait.

/// Provides a time-binned representation of the implementing type.
/// In contrast to `TimeBinnableType` this is meant for trait objects.
pub trait TimeBinnable: fmt::Debug + WithLen + RangeOverlapInfo + Any + AsAnyRef + AsAnyMut + Send {
    // TODO implementors may fail if edges contain not at least 2 entries.
    fn time_binner_new(&self, binrange: BinnedRangeEnum, do_time_weight: bool) -> Box<dyn TimeBinner>;
    // TODO just a helper for the empty result.
    fn to_box_to_json_result(&self) -> Box<dyn ToJsonResult>;
}

#[derive(Debug)]
pub enum MergeError {
    NotCompatible,
    Full,
}

impl From<MergeError> for err::Error {
    fn from(e: MergeError) -> Self {
        format!("{e:?}").into()
    }
}

// TODO can I remove the Any bound?

/// Container of some form of events, for use as trait object.
pub trait Events:
    fmt::Debug + TypeName + Any + Collectable + TimeBinnable + WithLen + Send + erased_serde::Serialize + EventsNonObj
{
    fn as_time_binnable(&self) -> &dyn TimeBinnable;
    fn verify(&self) -> bool;
    fn output_info(&self);
    fn as_collectable_mut(&mut self) -> &mut dyn Collectable;
    fn as_collectable_with_default_ref(&self) -> &dyn Collectable;
    fn as_collectable_with_default_mut(&mut self) -> &mut dyn Collectable;
    fn ts_min(&self) -> Option<u64>;
    fn ts_max(&self) -> Option<u64>;
    // TODO is this used?
    fn take_new_events_until_ts(&mut self, ts_end: u64) -> Box<dyn Events>;
    fn new_empty(&self) -> Box<dyn Events>;
    fn drain_into(&mut self, dst: &mut Box<dyn Events>, range: (usize, usize)) -> Result<(), MergeError>;
    fn find_lowest_index_gt(&self, ts: u64) -> Option<usize>;
    fn find_lowest_index_ge(&self, ts: u64) -> Option<usize>;
    fn find_highest_index_lt(&self, ts: u64) -> Option<usize>;
    fn clone_dyn(&self) -> Box<dyn Events>;
    fn partial_eq_dyn(&self, other: &dyn Events) -> bool;
    fn serde_id(&self) -> &'static str;
    fn nty_id(&self) -> u32;
    fn tss(&self) -> &VecDeque<u64>;
    fn pulses(&self) -> &VecDeque<u64>;
}

impl WithLen for Box<dyn Events> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

pub trait EventsNonObj {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>);
}

erased_serde::serialize_trait_object!(Events);

impl PartialEq for Box<dyn Events> {
    fn eq(&self, other: &Self) -> bool {
        Events::partial_eq_dyn(self.as_ref(), other.as_ref())
    }
}

pub struct TransformProperties {
    pub needs_one_before_range: bool,
    pub needs_value: bool,
}

pub trait Transformer {
    fn query_transform_properties(&self) -> TransformProperties;
}

impl<T> Transformer for Box<T>
where
    T: Transformer,
{
    fn query_transform_properties(&self) -> TransformProperties {
        self.as_ref().query_transform_properties()
    }
}

impl<T> Transformer for std::pin::Pin<Box<T>>
where
    T: Transformer,
{
    fn query_transform_properties(&self) -> TransformProperties {
        self.as_ref().query_transform_properties()
    }
}
