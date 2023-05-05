use items_0::overlap::RangeOverlapInfo;
use items_0::timebin::TimeBinnable;
use items_0::AppendEmptyBin;
use items_0::Empty;
use items_0::HasNonemptyFirstBin;
use items_0::WithLen;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use std::any;
use std::collections::VecDeque;
use std::ops::Range;

#[allow(unused)]
macro_rules! trace_ingest {
    ($($arg:tt)*) => {};
    ($($arg:tt)*) => { trace!($($arg)*); };
}

#[allow(unused)]
macro_rules! trace_ingest_item {
    ($($arg:tt)*) => {};
    ($($arg:tt)*) => { trace!($($arg)*); };
}

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => {};
    ($($arg:tt)*) => { trace!($($arg)*); };
}

pub trait TimeBinnerCommonV0Trait {
    type Input: RangeOverlapInfo + 'static;
    type Output: WithLen + Empty + AppendEmptyBin + HasNonemptyFirstBin + 'static;
    fn type_name() -> &'static str;
    fn common_bins_ready_count(&self) -> usize;
    fn common_range_current(&self) -> &SeriesRange;
    fn common_has_more_range(&self) -> bool;
    fn common_next_bin_range(&mut self) -> Option<SeriesRange>;
    fn common_set_current_range(&mut self, range: Option<SeriesRange>);
    fn common_take_or_append_all_from(&mut self, item: Self::Output);
    fn common_result_reset(&mut self, range: Option<SeriesRange>) -> Self::Output;
    fn common_agg_ingest(&mut self, item: &mut Self::Input);
}

pub struct TimeBinnerCommonV0Func {}

impl TimeBinnerCommonV0Func {
    pub fn agg_ingest<B>(binner: &mut B, item: &mut <B as TimeBinnerCommonV0Trait>::Input)
    where
        B: TimeBinnerCommonV0Trait,
    {
        //self.agg.ingest(item);
        <B as TimeBinnerCommonV0Trait>::common_agg_ingest(binner, item)
    }

    pub fn ingest<B>(binner: &mut B, item: &mut dyn TimeBinnable)
    where
        B: TimeBinnerCommonV0Trait,
    {
        let self_name = B::type_name();
        trace_ingest_item!(
            "TimeBinner for {} ingest  agg.range {:?}  item {:?}",
            Self::type_name(),
            self.agg.range(),
            item
        );
        if item.len() == 0 {
            // Return already here, RangeOverlapInfo would not give much sense.
            return;
        }
        // TODO optimize by remembering at which event array index we have arrived.
        // That needs modified interfaces which can take and yield the start and latest index.
        // Or consume the input data.
        loop {
            while item.starts_after(B::common_range_current(binner)) {
                trace_ingest_item!("{self_name}  ignore item and cycle  starts_after");
                TimeBinnerCommonV0Func::cycle(binner);
                if !B::common_has_more_range(binner) {
                    debug!("{self_name}  no more bin in edges after starts_after");
                    return;
                }
            }
            if item.ends_before(B::common_range_current(binner)) {
                trace_ingest_item!("{self_name}  ignore item  ends_before");
                return;
            } else {
                if !B::common_has_more_range(binner) {
                    trace_ingest_item!("{self_name}  no more bin in edges");
                    return;
                } else {
                    if let Some(item) = item
                        .as_any_mut()
                        // TODO make statically sure that we attempt to cast to the correct type here:
                        .downcast_mut::<B::Input>()
                    {
                        // TODO collect statistics associated with this request:
                        trace_ingest_item!("{self_name}  FEED THE ITEM...");
                        TimeBinnerCommonV0Func::agg_ingest(binner, item);
                        if item.ends_after(B::common_range_current(binner)) {
                            trace_ingest_item!(
                                "{self_name}  FED ITEM, ENDS AFTER  agg-range {:?}",
                                B::common_range_current(binner)
                            );
                            TimeBinnerCommonV0Func::cycle(binner);
                            if !B::common_has_more_range(binner) {
                                warn!("{self_name}  no more bin in edges after ingest and cycle");
                                return;
                            } else {
                                trace_ingest_item!("{self_name}  item fed, cycled, continue");
                            }
                        } else {
                            trace_ingest_item!("{self_name}  item fed, break");
                            break;
                        }
                    } else {
                        error!("{self_name}::ingest  unexpected item type");
                    };
                }
            }
        }
    }

    pub fn push_in_progress<B>(binner: &mut B, push_empty: bool)
    where
        B: TimeBinnerCommonV0Trait,
    {
        let self_name = B::type_name();
        trace_ingest_item!("{self_name}::push_in_progress  push_empty {push_empty}");
        // TODO expand should be derived from AggKind. Is it still required after all?
        // TODO here, the expand means that agg will assume that the current value is kept constant during
        // the rest of the time range.
        if B::common_has_more_range(binner) {
            let range_next = TimeBinnerCommonV0Trait::common_next_bin_range(binner);
            B::common_set_current_range(binner, range_next.clone());
            let bins = TimeBinnerCommonV0Trait::common_result_reset(binner, range_next);
            if bins.len() != 1 {
                error!("{self_name}::push_in_progress  bins.len() {}", bins.len());
                return;
            } else {
                if push_empty || HasNonemptyFirstBin::has_nonempty_first_bin(&bins) {
                    TimeBinnerCommonV0Trait::common_take_or_append_all_from(binner, bins);
                }
            }
        }
    }

    pub fn cycle<B>(binner: &mut B)
    where
        B: TimeBinnerCommonV0Trait,
    {
        let self_name = any::type_name::<Self>();
        trace_ingest_item!("{self_name}::cycle");
        // TODO refactor this logic.
        let n = TimeBinnerCommonV0Trait::common_bins_ready_count(binner);
        TimeBinnerCommonV0Func::push_in_progress(binner, true);
        if TimeBinnerCommonV0Trait::common_bins_ready_count(binner) == n {
            let range_next = TimeBinnerCommonV0Trait::common_next_bin_range(binner);
            B::common_set_current_range(binner, range_next.clone());
            if let Some(range) = range_next {
                let mut bins = <B as TimeBinnerCommonV0Trait>::Output::empty();
                if range.is_time() {
                    bins.append_empty_bin(range.beg_u64(), range.end_u64());
                } else {
                    error!("TODO  {self_name}::cycle  is_pulse");
                }
                TimeBinnerCommonV0Trait::common_take_or_append_all_from(binner, bins);
                if TimeBinnerCommonV0Trait::common_bins_ready_count(binner) <= n {
                    error!("failed to push a zero bin");
                }
            } else {
                warn!("cycle: no in-progress bin pushed, but also no more bin to add as zero-bin");
            }
        }
    }
}

pub trait ChooseIndicesForTimeBin {
    fn choose_indices_unweight(&self, beg: u64, end: u64) -> (Option<usize>, usize, usize);
    fn choose_indices_timeweight(&self, beg: u64, end: u64) -> (Option<usize>, usize, usize);
}

pub struct ChooseIndicesForTimeBinEvents {}

impl ChooseIndicesForTimeBinEvents {
    pub fn choose_unweight(beg: u64, end: u64, tss: &VecDeque<u64>) -> (Option<usize>, usize, usize) {
        // TODO improve via binary search.
        let mut one_before = None;
        let mut j = 0;
        let mut k = tss.len();
        for (i1, &ts) in tss.iter().enumerate() {
            if ts >= end {
                break;
            } else if ts >= beg {
            } else {
                one_before = Some(i1);
                j = i1 + 1;
            }
        }
        (one_before, j, k)
    }

    pub fn choose_timeweight(beg: u64, end: u64, tss: &VecDeque<u64>) -> (Option<usize>, usize, usize) {
        // TODO improve via binary search.
        let mut one_before = None;
        let mut j = 0;
        let mut k = tss.len();
        for (i1, &ts) in tss.iter().enumerate() {
            if ts >= end {
                trace_ingest!("{self_name} ingest  {:6}  {:20}  {:10?}  AFTER", i1, ts, val);
                // TODO count all the ignored events for stats
                k = i1;
                break;
            } else if ts >= beg {
                trace_ingest!("{self_name} ingest  {:6}  {:20}  {:10?}  INSIDE", i1, ts, val);
            } else {
                trace_ingest!("{self_name} ingest  {:6}  {:20}  {:10?}  BEFORE", i1, ts, val);
                one_before = Some(i1);
                j = i1 + 1;
            }
        }
        (one_before, j, k)
    }
}

pub trait TimeAggregatorCommonV0Trait {
    type Input: RangeOverlapInfo + ChooseIndicesForTimeBin + 'static;
    type Output: WithLen + Empty + AppendEmptyBin + HasNonemptyFirstBin + 'static;
    fn type_name() -> &'static str;
    fn common_range_current(&self) -> &SeriesRange;
    fn common_ingest_unweight_range(&mut self, item: &Self::Input, r: Range<usize>);
    fn common_ingest_one_before(&mut self, item: &Self::Input, j: usize);
    fn common_ingest_range(&mut self, item: &Self::Input, r: Range<usize>);
}

pub struct TimeAggregatorCommonV0Func {}

impl TimeAggregatorCommonV0Func {
    pub fn ingest_unweight<B>(binner: &mut B, item: &B::Input)
    where
        B: TimeAggregatorCommonV0Trait,
    {
        let self_name = B::type_name();
        trace_ingest!(
            "{self_name}::ingest_unweight  item len {}  items_seen {}",
            item.len(),
            self.items_seen
        );
        let rng = B::common_range_current(binner);
        if rng.is_time() {
            let beg = rng.beg_u64();
            let end = rng.end_u64();
            let (one_before, j, k) = item.choose_indices_unweight(beg, end);
            if let Some(j) = one_before {
                //<B as TimeAggregatorCommonV0Trait>::common_ingest_one_before(binner, item, j);
            }
            <B as TimeAggregatorCommonV0Trait>::common_ingest_unweight_range(binner, item, j..k);
        } else {
            error!("TODO ingest_unweight for pulse range");
            err::todo();
        }
    }

    pub fn ingest_time_weight<B>(binner: &mut B, item: &B::Input)
    where
        B: TimeAggregatorCommonV0Trait,
    {
        let self_name = B::type_name();
        trace_ingest!(
            "{self_name}::ingest_time_weight  item len {}  items_seen {}",
            item.len(),
            self.items_seen
        );
        let rng = B::common_range_current(binner);
        if rng.is_time() {
            let beg = rng.beg_u64();
            let end = rng.end_u64();
            let (one_before, j, k) = item.choose_indices_timeweight(beg, end);
            if let Some(j) = one_before {
                <B as TimeAggregatorCommonV0Trait>::common_ingest_one_before(binner, item, j);
            }
            <B as TimeAggregatorCommonV0Trait>::common_ingest_range(binner, item, j..k);
        } else {
            error!("TODO ingest_time_weight for pulse range");
            err::todo();
        }
    }
}
