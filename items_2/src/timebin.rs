use netpod::BinnedRangeEnum;
use std::fmt;

pub trait TimeBinner: fmt::Debug + Unpin {
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

pub trait TimeBinnable: fmt::Debug + Sized {
    type TimeBinner: TimeBinner<Input = Self>;

    fn time_binner_new(&self, binrange: BinnedRangeEnum, do_time_weight: bool) -> Self::TimeBinner;
}
