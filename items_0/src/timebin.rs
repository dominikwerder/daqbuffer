use crate::collect_s::Collectable;
use crate::collect_s::ToJsonResult;
use crate::AsAnyMut;
use crate::AsAnyRef;
use crate::RangeOverlapInfo;
use crate::TypeName;
use crate::WithLen;
use netpod::BinnedRangeEnum;
use std::any::Any;
use std::fmt;

// TODO can probably be removed.
pub trait TimeBins {
    fn ts_min(&self) -> Option<u64>;
    fn ts_max(&self) -> Option<u64>;
    fn ts_min_max(&self) -> Option<(u64, u64)>;
}

pub trait TimeBinnerTy: fmt::Debug + Unpin {
    type Input: fmt::Debug;
    type Output: fmt::Debug;

    fn ingest(&mut self, item: &mut Self::Input);

    fn set_range_complete(&mut self);

    fn bins_ready_count(&self) -> usize;

    fn bins_ready(&mut self) -> Option<Self::Output>;

    /// If there is a bin in progress with non-zero count, push it to the result set.
    /// With push_empty == true, a bin in progress is pushed even if it contains no counts.
    fn push_in_progress(&mut self, push_empty: bool);

    /// Implies `Self::push_in_progress` but in addition, pushes a zero-count bin if the call
    /// to `push_in_progress` did not change the result count, as long as edges are left.
    /// The next call to `Self::bins_ready_count` must return one higher count than before.
    fn cycle(&mut self);

    fn empty(&self) -> Option<Self::Output>;
}

pub trait TimeBinnableTy: fmt::Debug + Sized {
    type TimeBinner: TimeBinnerTy<Input = Self>;

    fn time_binner_new(&self, binrange: BinnedRangeEnum, do_time_weight: bool) -> Self::TimeBinner;
}

/// Data in time-binned form.
pub trait TimeBinned: Any + TypeName + TimeBinnable + Collectable + erased_serde::Serialize {
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
pub struct TimeBinnerDyn {}

impl TimeBinnerTy for TimeBinnerDyn {
    type Input = Box<dyn TimeBinnable>;
    type Output = Box<dyn TimeBinned>;

    fn ingest(&mut self, item: &mut Self::Input) {
        todo!()
    }

    fn set_range_complete(&mut self) {
        todo!()
    }

    fn bins_ready_count(&self) -> usize {
        todo!()
    }

    fn bins_ready(&mut self) -> Option<Self::Output> {
        todo!()
    }

    fn push_in_progress(&mut self, push_empty: bool) {
        todo!()
    }

    fn cycle(&mut self) {
        todo!()
    }

    fn empty(&self) -> Option<Self::Output> {
        todo!()
    }
}

impl TimeBinnableTy for Box<dyn TimeBinnable> {
    type TimeBinner = TimeBinnerDyn;

    fn time_binner_new(&self, binrange: BinnedRangeEnum, do_time_weight: bool) -> Self::TimeBinner {
        todo!()
    }
}
