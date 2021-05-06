use crate::agg::AggregatableXdim1Bin;
use crate::streamlog::LogItem;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::BinnedRange;
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
}

pub trait IntoBinnedT {
    type StreamOut: Stream;
    fn into_binned_t(self, spec: BinnedRange) -> Self::StreamOut;
}

impl<T, I> IntoBinnedT for T
where
    I: AggregatableTdim + Unpin,
    T: Stream<Item = Result<I, Error>> + Unpin,
    I::Aggregator: Unpin,
{
    type StreamOut = IntoBinnedTDefaultStream<T, I>;

    fn into_binned_t(self, spec: BinnedRange) -> Self::StreamOut {
        IntoBinnedTDefaultStream::new(self, spec)
    }
}

pub struct IntoBinnedTDefaultStream<S, I>
where
    I: AggregatableTdim,
    S: Stream<Item = Result<I, Error>>,
{
    inp: S,
    aggtor: Option<I::Aggregator>,
    spec: BinnedRange,
    curbin: u32,
    data_completed: bool,
    range_complete: bool,
    range_complete_emitted: bool,
    left: Option<Poll<Option<Result<I, Error>>>>,
    errored: bool,
    completed: bool,
    inp_completed: bool,
    tmp_agg_results: VecDeque<<I::Aggregator as AggregatorTdim>::OutputValue>,
}

impl<S, I> IntoBinnedTDefaultStream<S, I>
where
    I: AggregatableTdim,
    S: Stream<Item = Result<I, Error>>,
{
    pub fn new(inp: S, spec: BinnedRange) -> Self {
        let range = spec.get_range(0);
        Self {
            inp,
            aggtor: Some(I::aggregator_new_static(range.beg, range.end)),
            spec,
            curbin: 0,
            data_completed: false,
            range_complete: false,
            range_complete_emitted: false,
            left: None,
            errored: false,
            completed: false,
            inp_completed: false,
            tmp_agg_results: VecDeque::new(),
        }
    }
}

impl<S, I> Stream for IntoBinnedTDefaultStream<S, I>
where
    I: AggregatableTdim + Unpin,
    S: Stream<Item = Result<I, Error>> + Unpin,
    I::Aggregator: Unpin,
{
    type Item = Result<<I::Aggregator as AggregatorTdim>::OutputValue, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("IntoBinnedTDefaultStream  poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        'outer: loop {
            if let Some(item) = self.tmp_agg_results.pop_front() {
                return Ready(Some(Ok(item)));
            } else if self.data_completed {
                if self.range_complete {
                    if self.range_complete_emitted {
                        self.completed = true;
                        return Ready(None);
                    } else {
                        self.range_complete_emitted = true;
                        // TODO why can't I declare that type?
                        //type TT = <I::Aggregator as AggregatorTdim>::OutputValue;
                        if let Some(item) = <I::Aggregator as AggregatorTdim>::OutputValue::make_range_complete_item() {
                            return Ready(Some(Ok(item)));
                        } else {
                            warn!("IntoBinnedTDefaultStream  should emit RangeComplete but it doesn't have one");
                            self.completed = true;
                            return Ready(None);
                        }
                    }
                } else {
                    self.completed = true;
                    return Ready(None);
                }
            }
            let cur = if let Some(k) = self.left.take() {
                k
            } else if self.inp_completed {
                Ready(None)
            } else {
                let inp_poll_span = span!(Level::TRACE, "into_t_inp_poll");
                inp_poll_span.in_scope(|| self.inp.poll_next_unpin(cx))
            };
            break match cur {
                Ready(Some(Ok(k))) => {
                    if k.is_range_complete() {
                        self.range_complete = true;
                        continue 'outer;
                    } else if k.is_log_item() {
                        if let Some(item) = k.log_item() {
                            if let Some(item) =
                                <I::Aggregator as AggregatorTdim>::OutputValue::make_log_item(item.clone())
                            {
                                Ready(Some(Ok(item)))
                            } else {
                                warn!("IntoBinnedTDefaultStream  can not create log item");
                                continue 'outer;
                            }
                        } else {
                            panic!()
                        }
                    } else {
                        let ag = self.aggtor.as_mut().unwrap();
                        if ag.ends_before(&k) {
                            //info!("ENDS BEFORE");
                            continue 'outer;
                        } else if ag.starts_after(&k) {
                            self.left = Some(Ready(Some(Ok(k))));
                            self.curbin += 1;
                            let range = self.spec.get_range(self.curbin);
                            let ret = self
                                .aggtor
                                .replace(I::aggregator_new_static(range.beg, range.end))
                                .unwrap()
                                .result();
                            self.tmp_agg_results = ret.into();
                            if self.curbin as u64 >= self.spec.count {
                                self.data_completed = true;
                            }
                            continue 'outer;
                        } else {
                            let mut k = k;
                            ag.ingest(&mut k);
                            let k = k;
                            if ag.ends_after(&k) {
                                self.left = Some(Ready(Some(Ok(k))));
                                self.curbin += 1;
                                let range = self.spec.get_range(self.curbin);
                                let ret = self
                                    .aggtor
                                    .replace(I::aggregator_new_static(range.beg, range.end))
                                    .unwrap()
                                    .result();
                                self.tmp_agg_results = ret.into();
                                if self.curbin as u64 >= self.spec.count {
                                    self.data_completed = true;
                                }
                                continue 'outer;
                            } else {
                                continue 'outer;
                            }
                        }
                    }
                }
                Ready(Some(Err(e))) => {
                    self.errored = true;
                    Ready(Some(Err(e)))
                }
                Ready(None) => {
                    self.inp_completed = true;
                    if self.curbin as u64 >= self.spec.count {
                        self.data_completed = true;
                        continue 'outer;
                    } else {
                        self.curbin += 1;
                        let range = self.spec.get_range(self.curbin);
                        match self.aggtor.replace(I::aggregator_new_static(range.beg, range.end)) {
                            Some(ag) => {
                                let ret = ag.result();
                                self.tmp_agg_results = ret.into();
                                continue 'outer;
                            }
                            None => {
                                panic!();
                            }
                        }
                    }
                }
                Pending => Pending,
            };
        }
    }
}
