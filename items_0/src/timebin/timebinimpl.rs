#![allow(unused)]
use crate::timebin::TimeBinnable;
use crate::timebin::TimeBinned;
use crate::timebin::TimeBinner;
use crate::timebin::TimeBinnerIngest;
use crate::TypeName;
use netpod::log::*;
use netpod::range::evrange::NanoRange;

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => { trace!($($arg)*) };
}

#[allow(unused)]
macro_rules! trace_ingest {
    ($($arg:tt)*) => { trace!($($arg)*) };
}

#[cfg(DISABLED)]
impl<T> TimeBinner for T
where
    T: TimeBinnerIngest,
{
    fn bins_ready_count(&self) -> usize {
        match &self.ready {
            Some(k) => k.len(),
            None => 0,
        }
    }

    fn bins_ready(&mut self) -> Option<Box<dyn TimeBinned>> {
        match self.ready.take() {
            Some(k) => Some(Box::new(k)),
            None => None,
        }
    }

    fn ingest(&mut self, item: &mut dyn TimeBinnable) {
        trace2!(
            "TimeBinner for {} ingest  agg.range {:?}  item {:?}",
            self.type_name(),
            self.agg.range(),
            item
        );
        if item.len() == 0 {
            // Return already here, RangeOverlapInfo would not give much sense.
            return;
        }
        // TODO optimize by remembering at which event array index we have arrived.
        // That needs modified interfaces which can take and yield the start and latest index.
        loop {
            while item.starts_after(self.agg.range()) {
                trace!(
                    "{}  IGNORE ITEM  AND CYCLE  BECAUSE item.starts_after",
                    self.type_name()
                );
                self.cycle();
                if self.rng.is_none() {
                    warn!("{}  no more bin in edges B", self.type_name());
                    return;
                }
            }
            if item.ends_before(self.agg.range()) {
                trace!("{}  IGNORE ITEM  BECAUSE ends_before", self.type_name());
                return;
            } else {
                if self.rng.is_none() {
                    trace!("{}  no more bin in edges D", self.type_name());
                    return;
                } else {
                    match TimeBinnerIngest::ingest_inrange(self, item) {
                        Ok(()) => {
                            if item.ends_after(self.agg.range()) {
                                trace_ingest!("{}  FED ITEM, ENDS AFTER.", self.type_name());
                                self.cycle();
                                if self.rng.is_none() {
                                    warn!("{}  no more bin in edges C", self.type_name());
                                    return;
                                } else {
                                    trace_ingest!("{}  FED ITEM, CYCLED, CONTINUE.", self.type_name());
                                }
                            } else {
                                trace_ingest!("{}  FED ITEM.", self.type_name());
                                break;
                            }
                        }
                        Err(e) => {
                            error!("{}::ingest  {}", self.type_name(), e);
                        }
                    }
                    /*
                    // Move to TimeBinnerIngest
                    if let Some(item) = item
                        .as_any_ref()
                        // TODO make statically sure that we attempt to cast to the correct type here:
                        .downcast_ref::<<EventsDim0Aggregator<STY> as TimeBinnableTypeAggregator>::Input>()
                    {
                        // TODO collect statistics associated with this request:
                        trace_ingest!("{self_name}  FEED THE ITEM...");
                        self.agg.ingest(item);
                    } else {
                        error!("{self_name}::ingest  unexpected item type");
                    };
                    */
                }
            }
        }
    }

    fn push_in_progress(&mut self, push_empty: bool) {
        trace!("{}::push_in_progress  push_empty {push_empty}", self.type_name());
        // TODO expand should be derived from AggKind. Is it still required after all?
        // TODO here, the expand means that agg will assume that the current value is kept constant during
        // the rest of the time range.
        if self.rng.is_none() {
        } else {
            let expand = true;
            let range_next = self.next_bin_range();
            self.rng = range_next.clone();
            let mut bins = if let Some(range_next) = range_next {
                self.agg.result_reset(range_next, expand)
            } else {
                // Acts as placeholder
                let range_next = NanoRange {
                    beg: u64::MAX - 1,
                    end: u64::MAX,
                };
                self.agg.result_reset(range_next.into(), expand)
            };
            if bins.len() != 1 {
                error!("{}::push_in_progress  bins.len() {}", self.type_name(), bins.len());
                return;
            } else {
                if push_empty || bins.counts[0] != 0 {
                    match self.ready.as_mut() {
                        Some(ready) => {
                            ready.append_all_from(&mut bins);
                        }
                        None => {
                            self.ready = Some(bins);
                        }
                    }
                }
            }
        }
    }

    fn cycle(&mut self) {
        trace!("{}::cycle", self.type_name());
        // TODO refactor this logic.
        let n = self.bins_ready_count();
        self.push_in_progress(true);
        if self.bins_ready_count() == n {
            let range_next = self.next_bin_range();
            self.rng = range_next.clone();
            if let Some(range) = range_next {
                /*
                TODO Move out to trait.
                let mut bins = BinsDim0::empty();
                if range.is_time() {
                    bins.append_zero(range.beg_u64(), range.end_u64());
                } else {
                    error!("TODO  {self_name}::cycle  is_pulse");
                }
                match self.ready.as_mut() {
                    Some(ready) => {
                        ready.append_all_from(&mut bins);
                    }
                    None => {
                        self.ready = Some(bins);
                    }
                }
                */
                if self.bins_ready_count() <= n {
                    error!("{}::cycle  failed to push a zero bin", self.type_name());
                }
            } else {
                warn!(
                    "{}::cycle  no in-progress bin pushed, but also no more bin to add as zero-bin",
                    self.type_name()
                );
            }
        }
    }

    fn set_range_complete(&mut self) {
        self.range_final = true;
    }

    fn empty(&self) -> Box<dyn TimeBinned> {
        /*
        TODO factor out to trait.
        let ret = <EventsDim0Aggregator<STY> as TimeBinnableTypeAggregator>::Output::empty();
        */
        let ret = todo!();
        Box::new(ret)
    }
}
