use crate::agg::AggregatableXdim1Bin;
use crate::streamlog::LogItem;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{BinnedRange, EventDataReadStats};
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

pub trait AggregatorTdim {
    type InputValue;
    type OutputValue: AggregatableXdim1Bin + AggregatableTdim + Unpin;
    fn ends_before(&self, inp: &Self::InputValue) -> bool;
    fn ends_after(&self, inp: &Self::InputValue) -> bool;
    fn starts_after(&self, inp: &Self::InputValue) -> bool;
    fn ingest(&mut self, inp: &mut Self::InputValue);
    fn result(self) -> Vec<Self::OutputValue>;
}

pub trait AggregatableTdim: Sized {
    type Output: AggregatableXdim1Bin + AggregatableTdim;
    type Aggregator: AggregatorTdim<InputValue = Self>;
    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator;
    fn is_range_complete(&self) -> bool;
    fn make_range_complete_item() -> Option<Self>;
    fn is_log_item(&self) -> bool;
    fn log_item(self) -> Option<LogItem>;
    fn make_log_item(item: LogItem) -> Option<Self>;
    fn is_stats_item(&self) -> bool;
    fn stats_item(self) -> Option<EventDataReadStats>;
    fn make_stats_item(item: EventDataReadStats) -> Option<Self>;
}

pub trait IntoBinnedT {
    type StreamOut: Stream;
    fn into_binned_t(self, spec: BinnedRange) -> Self::StreamOut;
}

impl<S, I> IntoBinnedT for S
where
    S: Stream<Item = Result<I, Error>> + Unpin,
    I: AggregatableTdim + Unpin,
    //I: AggregatableTdim,
    I::Aggregator: Unpin,
{
    type StreamOut = IntoBinnedTDefaultStream<S, I>;

    fn into_binned_t(self, spec: BinnedRange) -> Self::StreamOut {
        IntoBinnedTDefaultStream::new(self, spec)
    }
}

pub struct IntoBinnedTDefaultStream<S, I>
where
    S: Stream<Item = Result<I, Error>>,
    I: AggregatableTdim,
{
    inp: S,
    aggtor: Option<I::Aggregator>,
    spec: BinnedRange,
    curbin: u32,
    inp_completed: bool,
    all_bins_emitted: bool,
    range_complete_observed: bool,
    range_complete_emitted: bool,
    left: Option<Poll<Option<Result<I, Error>>>>,
    errored: bool,
    completed: bool,
    tmp_agg_results: VecDeque<<I::Aggregator as AggregatorTdim>::OutputValue>,
}

impl<S, I> IntoBinnedTDefaultStream<S, I>
where
    S: Stream<Item = Result<I, Error>> + Unpin,
    I: AggregatableTdim,
{
    pub fn new(inp: S, spec: BinnedRange) -> Self {
        let range = spec.get_range(0);
        Self {
            inp,
            aggtor: Some(I::aggregator_new_static(range.beg, range.end)),
            spec,
            curbin: 0,
            inp_completed: false,
            all_bins_emitted: false,
            range_complete_observed: false,
            range_complete_emitted: false,
            left: None,
            errored: false,
            completed: false,
            tmp_agg_results: VecDeque::new(),
        }
    }

    fn cur(&mut self, cx: &mut Context) -> Poll<Option<Result<I, Error>>> {
        if let Some(cur) = self.left.take() {
            cur
        } else if self.inp_completed {
            Poll::Ready(None)
        } else {
            let inp_poll_span = span!(Level::TRACE, "into_t_inp_poll");
            inp_poll_span.in_scope(|| self.inp.poll_next_unpin(cx))
        }
    }

    fn cycle_current_bin(&mut self) {
        self.curbin += 1;
        let range = self.spec.get_range(self.curbin);
        let ret = self
            .aggtor
            .replace(I::aggregator_new_static(range.beg, range.end))
            // TODO handle None case, or remove Option if Agg is always present
            .unwrap()
            .result();
        self.tmp_agg_results = ret.into();
        if self.curbin >= self.spec.count as u32 {
            self.all_bins_emitted = true;
        }
    }

    fn handle(
        &mut self,
        cur: Poll<Option<Result<I, Error>>>,
    ) -> Option<Poll<Option<Result<<I::Aggregator as AggregatorTdim>::OutputValue, Error>>>> {
        use Poll::*;
        match cur {
            Ready(Some(Ok(k))) => {
                if k.is_range_complete() {
                    self.range_complete_observed = true;
                    None
                } else if k.is_log_item() {
                    if let Some(item) = k.log_item() {
                        if let Some(item) = <I::Aggregator as AggregatorTdim>::OutputValue::make_log_item(item) {
                            Some(Ready(Some(Ok(item))))
                        } else {
                            error!("IntoBinnedTDefaultStream  can not create log item");
                            None
                        }
                    } else {
                        error!("supposed to be log item but can't take it");
                        None
                    }
                } else if k.is_stats_item() {
                    if let Some(item) = k.stats_item() {
                        if let Some(item) = <I::Aggregator as AggregatorTdim>::OutputValue::make_stats_item(item) {
                            Some(Ready(Some(Ok(item))))
                        } else {
                            error!("IntoBinnedTDefaultStream  can not create stats item");
                            None
                        }
                    } else {
                        error!("supposed to be stats item but can't take it");
                        None
                    }
                } else if self.all_bins_emitted {
                    // Just drop the item because we will not emit anymore data.
                    // Could also at least gather some stats.
                    None
                } else {
                    let ag = self.aggtor.as_mut().unwrap();
                    if ag.ends_before(&k) {
                        None
                    } else if ag.starts_after(&k) {
                        self.left = Some(Ready(Some(Ok(k))));
                        self.cycle_current_bin();
                        // TODO cycle_current_bin enqueues the bin, can I return here instead?
                        None
                    } else {
                        let mut k = k;
                        ag.ingest(&mut k);
                        let k = k;
                        if ag.ends_after(&k) {
                            self.left = Some(Ready(Some(Ok(k))));
                            self.cycle_current_bin();
                        }
                        // TODO cycle_current_bin enqueues the bin, can I return here instead?
                        None
                    }
                }
            }
            Ready(Some(Err(e))) => {
                self.errored = true;
                Some(Ready(Some(Err(e))))
            }
            Ready(None) => {
                self.inp_completed = true;
                if self.all_bins_emitted {
                    None
                } else {
                    self.cycle_current_bin();
                    // TODO cycle_current_bin enqueues the bin, can I return here instead?
                    None
                }
            }
            Pending => Some(Pending),
        }
    }
}

impl<S, I> Stream for IntoBinnedTDefaultStream<S, I>
where
    S: Stream<Item = Result<I, Error>> + Unpin,
    //I: AggregatableTdim,
    I: AggregatableTdim + Unpin,
    I::Aggregator: Unpin,
{
    type Item = Result<<I::Aggregator as AggregatorTdim>::OutputValue, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("IntoBinnedTDefaultStream  poll_next on completed");
            } else if self.errored {
                self.completed = true;
                Ready(None)
            } else if let Some(item) = self.tmp_agg_results.pop_front() {
                Ready(Some(Ok(item)))
            } else if self.range_complete_emitted {
                self.completed = true;
                Ready(None)
            } else if self.inp_completed && self.all_bins_emitted {
                self.range_complete_emitted = true;
                if self.range_complete_observed {
                    // TODO why can't I declare that type alias?
                    //type TT = I;
                    if let Some(item) = <I::Aggregator as AggregatorTdim>::OutputValue::make_range_complete_item() {
                        Ready(Some(Ok(item)))
                    } else {
                        warn!("IntoBinnedTDefaultStream  should emit RangeComplete but it doesn't have one");
                        continue 'outer;
                    }
                } else {
                    continue 'outer;
                }
            } else {
                let cur = self.cur(cx);
                match self.handle(cur) {
                    Some(item) => item,
                    None => continue 'outer,
                }
            };
        }
    }
}
