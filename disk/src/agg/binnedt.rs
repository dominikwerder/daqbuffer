use crate::agg::{AggregatableTdim, AggregatorTdim};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::BinnedRange;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

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
                    let ag = self.aggtor.as_mut().unwrap();
                    if ag.ends_before(&k) {
                        //info!("ENDS BEFORE");
                        continue 'outer;
                    } else if ag.starts_after(&k) {
                        //info!("STARTS AFTER");
                        self.left = Some(Ready(Some(Ok(k))));
                        self.curbin += 1;
                        let range = self.spec.get_range(self.curbin);
                        let ret = self
                            .aggtor
                            .replace(I::aggregator_new_static(range.beg, range.end))
                            .unwrap()
                            .result();
                        //Ready(Some(Ok(ret)))
                        self.tmp_agg_results = ret.into();
                        continue 'outer;
                    } else {
                        //info!("INGEST");
                        let mut k = k;
                        ag.ingest(&mut k);
                        // if this input contains also data after the current bin, then I need to keep
                        // it for the next round.
                        if ag.ends_after(&k) {
                            //info!("ENDS AFTER");
                            self.left = Some(Ready(Some(Ok(k))));
                            self.curbin += 1;
                            let range = self.spec.get_range(self.curbin);
                            let ret = self
                                .aggtor
                                .replace(I::aggregator_new_static(range.beg, range.end))
                                .unwrap()
                                .result();
                            //Ready(Some(Ok(ret)))
                            self.tmp_agg_results = ret.into();
                            continue 'outer;
                        } else {
                            //info!("ENDS WITHIN");
                            continue 'outer;
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
                        warn!("IntoBinnedTDefaultStream  curbin out of spec, END");
                        self.completed = true;
                        Ready(None)
                    } else {
                        self.curbin += 1;
                        let range = self.spec.get_range(self.curbin);
                        match self.aggtor.replace(I::aggregator_new_static(range.beg, range.end)) {
                            Some(ag) => {
                                let ret = ag.result();
                                //Ready(Some(Ok(ag.result())))
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
