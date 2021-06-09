use crate::agg::enp::XBinnedScalarEvents;
use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::{Appendable, StreamItem};
use crate::binned::{
    FilterFittingInside, MinMaxAvgAggregator, MinMaxAvgBins, NumOps, RangeCompletableItem, RangeOverlapInfo, ReadPbv,
    ReadableFromFile, SingleXBinAggregator,
};
use crate::decode::EventValues;
use crate::frame::makeframe::Framable;
use crate::Sitemty;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{BinnedRange, NanoRange};
use serde::Serialize;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;

pub struct DefaultBinsTimeBinner<NTY> {
    _m1: PhantomData<NTY>,
}

pub trait TimeBinnableTypeAggregator: Send {
    type Input: TimeBinnableType;
    type Output: TimeBinnableType;
    fn range(&self) -> &NanoRange;
    fn ingest(&mut self, item: &Self::Input);
    fn result(self) -> Self::Output;
}

pub trait TimeBinnableType:
    Send + Unpin + RangeOverlapInfo + FilterFittingInside + Appendable + Serialize + ReadableFromFile
{
    type Output: TimeBinnableType;
    type Aggregator: TimeBinnableTypeAggregator<Input = Self, Output = Self::Output> + Send + Unpin;
    fn aggregator(range: NanoRange) -> Self::Aggregator;
}

pub struct TBinnerStream<S, TBT>
where
    S: Stream<Item = Sitemty<TBT>>,
    TBT: TimeBinnableType,
{
    inp: Pin<Box<S>>,
    spec: BinnedRange,
    curbin: u32,
    left: Option<Poll<Option<Sitemty<TBT>>>>,
    aggtor: Option<<TBT as TimeBinnableType>::Aggregator>,
    tmp_agg_results: VecDeque<<<TBT as TimeBinnableType>::Aggregator as TimeBinnableTypeAggregator>::Output>,
    inp_completed: bool,
    all_bins_emitted: bool,
    range_complete_observed: bool,
    range_complete_emitted: bool,
    errored: bool,
    completed: bool,
}

impl<S, TBT> TBinnerStream<S, TBT>
where
    S: Stream<Item = Sitemty<TBT>> + Send + Unpin + 'static,
    TBT: TimeBinnableType,
{
    pub fn new(inp: S, spec: BinnedRange) -> Self {
        let range = spec.get_range(0);
        Self {
            inp: Box::pin(inp),
            spec,
            curbin: 0,
            left: None,
            aggtor: Some(<TBT as TimeBinnableType>::aggregator(range)),
            tmp_agg_results: VecDeque::new(),
            inp_completed: false,
            all_bins_emitted: false,
            range_complete_observed: false,
            range_complete_emitted: false,
            errored: false,
            completed: false,
        }
    }

    fn cur(&mut self, cx: &mut Context) -> Poll<Option<Sitemty<TBT>>> {
        if let Some(cur) = self.left.take() {
            cur
        } else if self.inp_completed {
            Poll::Ready(None)
        } else {
            let inp_poll_span = span!(Level::TRACE, "into_t_inp_poll");
            inp_poll_span.in_scope(|| self.inp.poll_next_unpin(cx))
        }
    }

    // TODO handle unwrap error, or use a mem replace type instead of option:
    fn cycle_current_bin(&mut self) {
        self.curbin += 1;
        let range = self.spec.get_range(self.curbin);
        let ret = self
            .aggtor
            .replace(<TBT as TimeBinnableType>::aggregator(range))
            .unwrap()
            .result();
        // TODO should we accumulate bins before emit? Maybe not, we want to stay responsive.
        // Only if the frequency would be high, that would require cpu time checks. Worth it? Measure..
        self.tmp_agg_results.push_back(ret);
        if self.curbin >= self.spec.count as u32 {
            self.all_bins_emitted = true;
        }
    }

    fn handle(
        &mut self,
        cur: Poll<Option<Sitemty<TBT>>>,
    ) -> Option<Poll<Option<Sitemty<<<TBT as TimeBinnableType>::Aggregator as TimeBinnableTypeAggregator>::Output>>>>
    {
        use Poll::*;
        match cur {
            Ready(Some(Ok(item))) => match item {
                StreamItem::Log(item) => Some(Ready(Some(Ok(StreamItem::Log(item))))),
                StreamItem::Stats(item) => Some(Ready(Some(Ok(StreamItem::Stats(item))))),
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => {
                        self.range_complete_observed = true;
                        None
                    }
                    RangeCompletableItem::Data(item) => {
                        if self.all_bins_emitted {
                            // Just drop the item because we will not emit anymore data.
                            // TODO gather stats.
                            None
                        } else {
                            let ag = self.aggtor.as_mut().unwrap();
                            if item.ends_before(ag.range().clone()) {
                                None
                            } else if item.starts_after(ag.range().clone()) {
                                self.left =
                                    Some(Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))))));
                                self.cycle_current_bin();
                                // TODO cycle_current_bin enqueues the bin, can I return here instead?
                                None
                            } else {
                                ag.ingest(&item);
                                if item.ends_after(ag.range().clone()) {
                                    self.left =
                                        Some(Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))))));
                                    self.cycle_current_bin();
                                }
                                // TODO cycle_current_bin enqueues the bin, can I return here instead?
                                None
                            }
                        }
                    }
                },
            },
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

impl<S, TBT> Stream for TBinnerStream<S, TBT>
where
    S: Stream<Item = Sitemty<TBT>> + Send + Unpin + 'static,
    TBT: TimeBinnableType + Send + Unpin + 'static,
    <TBT as TimeBinnableType>::Aggregator: Unpin,
    <<TBT as TimeBinnableType>::Aggregator as TimeBinnableTypeAggregator>::Output: Unpin,
{
    type Item = Sitemty<<<TBT as TimeBinnableType>::Aggregator as TimeBinnableTypeAggregator>::Output>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("poll_next on completed");
            } else if self.errored {
                self.completed = true;
                Ready(None)
            } else if let Some(item) = self.tmp_agg_results.pop_front() {
                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))))
            } else if self.range_complete_emitted {
                self.completed = true;
                Ready(None)
            } else if self.inp_completed && self.all_bins_emitted {
                self.range_complete_emitted = true;
                if self.range_complete_observed {
                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
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
