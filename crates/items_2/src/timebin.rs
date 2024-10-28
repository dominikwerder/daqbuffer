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
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest_item { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest_event { ($($arg:tt)*) => ( if false { trace!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest_detail { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

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
        let self_name = "choose_timeweight";
        // TODO improve via binary search.
        let mut one_before = None;
        let mut j = 0;
        let mut k = tss.len();
        for (i1, &ts) in tss.iter().enumerate() {
            if ts >= end {
                trace_ingest_event!("{self_name} ingest  {:6}  {:20}  AFTER", i1, ts);
                // TODO count all the ignored events for stats
                k = i1;
                break;
            } else if ts >= beg {
                trace_ingest_event!("{self_name} ingest  {:6}  {:20}  INSIDE", i1, ts);
            } else {
                trace_ingest_event!("{self_name} ingest  {:6}  {:20}  BEFORE", i1, ts);
                one_before = Some(i1);
                j = i1 + 1;
            }
        }
        trace_ingest_item!("{self_name}  chosen {one_before:?} {j:?} {k:?}");
        (one_before, j, k)
    }
}

pub trait TimeAggregatorCommonV0Trait {
    type Input: WithLen + ChooseIndicesForTimeBin + 'static;
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
        // TODO
        let items_seen = 777;
        trace_ingest_item!(
            "{self_name}::ingest_unweight  item len {}  items_seen {}",
            item.len(),
            items_seen
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
        // TODO
        let items_seen = 777;
        trace_ingest_item!(
            "{self_name}::ingest_time_weight  item len {}  items_seen {}",
            item.len(),
            items_seen
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
