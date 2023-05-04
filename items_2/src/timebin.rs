use crate::eventsdim0::EventsDim0TimeBinner;
use items_0::overlap::RangeOverlapInfo;
use items_0::scalar_ops::ScalarOps;
use items_0::timebin::TimeBinnable;
use items_0::AppendEmptyBin;
use items_0::Appendable;
use items_0::Empty;
use items_0::WithLen;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
use std::any;

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
    type Output: WithLen + Empty + AppendEmptyBin + 'static;
    fn type_name() -> &'static str;
    fn common_bins_ready_count(&self) -> usize;
    fn common_range_current(&self) -> &SeriesRange;
    fn common_next_bin_range(&mut self) -> Option<SeriesRange>;
    fn common_cycle(&mut self);
    fn common_has_more_range(&self) -> bool;
    fn common_take_or_append_all_from(&mut self, item: Self::Output);
    fn common_result_reset(&mut self, range: SeriesRange) -> Self::Output;
}

pub struct TimeBinnerCommonV0Func {}

impl TimeBinnerCommonV0Func {
    pub fn agg_ingest<B>(binner: &mut B, item: &mut <B as TimeBinnerCommonV0Trait>::Input)
    where
        B: TimeBinnerCommonV0Trait,
    {
        //self.agg.ingest(item);
        todo!()
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

    fn push_in_progress<B>(binner: &mut B, push_empty: bool)
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
            let bins = if let Some(range_next) = range_next {
                TimeBinnerCommonV0Trait::common_result_reset(binner, range_next)
                //self.agg.result_reset(range_next, expand)
            } else {
                // Acts as placeholder
                // TODO clean up
                let range_next = NanoRange {
                    beg: u64::MAX - 1,
                    end: u64::MAX,
                };
                TimeBinnerCommonV0Trait::common_result_reset(binner, range_next.into())
                //self.agg.result_reset(range_next.into(), expand)
            };
            if bins.len() != 1 {
                error!("{self_name}::push_in_progress  bins.len() {}", bins.len());
                return;
            } else {
                //if push_empty || bins.counts[0] != 0 {
                if push_empty {
                    TimeBinnerCommonV0Trait::common_take_or_append_all_from(binner, bins);
                }
            }
        }
    }

    fn cycle<B>(binner: &mut B)
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
