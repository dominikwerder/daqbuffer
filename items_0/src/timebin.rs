use crate::collect_s::Collectable;
use crate::collect_s::ToJsonResult;
use crate::AsAnyMut;
use crate::AsAnyRef;
use crate::Events;
use crate::RangeOverlapInfo;
use crate::TypeName;
use crate::WithLen;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
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

pub trait TimeBinner: fmt::Debug + Send {
    fn ingest(&mut self, item: &mut dyn TimeBinnable);
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

impl WithLen for Box<dyn TimeBinnable> {
    fn len(&self) -> usize {
        todo!()
    }
}

impl RangeOverlapInfo for Box<dyn TimeBinnable> {
    fn ends_before(&self, range: &SeriesRange) -> bool {
        todo!()
    }

    fn ends_after(&self, range: &SeriesRange) -> bool {
        todo!()
    }

    fn starts_after(&self, range: &SeriesRange) -> bool {
        todo!()
    }
}

impl TimeBinnable for Box<dyn TimeBinnable> {
    fn time_binner_new(&self, binrange: BinnedRangeEnum, do_time_weight: bool) -> Box<dyn TimeBinner> {
        todo!()
    }

    fn to_box_to_json_result(&self) -> Box<dyn ToJsonResult> {
        todo!()
    }
}

impl RangeOverlapInfo for Box<dyn Events> {
    fn ends_before(&self, range: &SeriesRange) -> bool {
        todo!()
    }

    fn ends_after(&self, range: &SeriesRange) -> bool {
        todo!()
    }

    fn starts_after(&self, range: &SeriesRange) -> bool {
        todo!()
    }
}

impl TimeBinnable for Box<dyn Events> {
    fn time_binner_new(&self, binrange: BinnedRangeEnum, do_time_weight: bool) -> Box<dyn TimeBinner> {
        todo!()
    }

    fn to_box_to_json_result(&self) -> Box<dyn ToJsonResult> {
        todo!()
    }
}

impl TypeName for Box<dyn TimeBinnable> {
    fn type_name(&self) -> String {
        format!("Box<dyn TimeBinnable> TODO TypeName for Box<dyn TimeBinnable>")
    }
}

impl Collectable for Box<dyn TimeBinnable> {
    fn new_collector(&self) -> Box<dyn crate::collect_s::Collector> {
        todo!()
    }
}

#[derive(Debug)]
pub struct TimeBinnerDynStruct {
    binrange: BinnedRangeEnum,
    do_time_weight: bool,
    binner: Option<Box<dyn TimeBinner>>,
}

impl TimeBinnerDynStruct {
    pub fn new(binrange: BinnedRangeEnum, do_time_weight: bool, binner: Box<dyn TimeBinner>) -> Self {
        Self {
            binrange,
            do_time_weight,
            binner: Some(binner),
        }
    }
}

impl TimeBinnerTy for TimeBinnerDynStruct {
    type Input = Box<dyn TimeBinnable>;
    type Output = Box<dyn TimeBinned>;

    fn ingest(&mut self, item: &mut Self::Input) {
        if self.binner.is_none() {
            self.binner = Some(Box::new(TimeBinnableTy::time_binner_new(
                item,
                self.binrange.clone(),
                self.do_time_weight,
            )));
        }
        self.binner.as_mut().unwrap().as_mut().ingest(item.as_mut())
    }

    fn set_range_complete(&mut self) {
        if let Some(k) = self.binner.as_mut() {
            k.set_range_complete()
        }
    }

    fn bins_ready_count(&self) -> usize {
        if let Some(k) = self.binner.as_ref() {
            k.bins_ready_count()
        } else {
            0
        }
    }

    fn bins_ready(&mut self) -> Option<Self::Output> {
        if let Some(k) = self.binner.as_mut() {
            k.bins_ready()
        } else {
            None
        }
    }

    fn push_in_progress(&mut self, push_empty: bool) {
        if let Some(k) = self.binner.as_mut() {
            k.push_in_progress(push_empty)
        }
    }

    fn cycle(&mut self) {
        if let Some(k) = self.binner.as_mut() {
            k.cycle()
        }
    }

    fn empty(&self) -> Option<Self::Output> {
        if let Some(k) = self.binner.as_ref() {
            Some(k.empty())
        } else {
            warn!("TimeBinnerDynStruct::empty called with binner None");
            None
        }
    }
}

impl TimeBinner for TimeBinnerDynStruct {
    fn ingest(&mut self, item: &mut dyn TimeBinnable) {
        todo!()
    }

    fn bins_ready_count(&self) -> usize {
        todo!()
    }

    fn bins_ready(&mut self) -> Option<Box<dyn TimeBinned>> {
        todo!()
    }

    fn push_in_progress(&mut self, push_empty: bool) {
        todo!()
    }

    fn cycle(&mut self) {
        todo!()
    }

    fn set_range_complete(&mut self) {
        todo!()
    }

    fn empty(&self) -> Box<dyn TimeBinned> {
        todo!()
    }
}

impl TimeBinnableTy for Box<dyn TimeBinnable> {
    type TimeBinner = TimeBinnerDynStruct;

    fn time_binner_new(&self, binrange: BinnedRangeEnum, do_time_weight: bool) -> Self::TimeBinner {
        let binner = self.as_ref().time_binner_new(binrange.clone(), do_time_weight);
        TimeBinnerDynStruct::new(binrange, do_time_weight, binner)
    }
}
