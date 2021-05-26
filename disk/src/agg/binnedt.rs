use crate::agg::streams::StreamItem;
use crate::agg::AggregatableXdim1Bin;
use crate::binned::{BinnedStreamKind, RangeCompletableItem};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::BinnedRange;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

pub trait AggregatorTdim<SK>: Sized + Unpin
where
    SK: BinnedStreamKind,
{
    type InputValue;
    type OutputValue;
    fn ends_before(&self, inp: &Self::InputValue) -> bool;
    fn ends_after(&self, inp: &Self::InputValue) -> bool;
    fn starts_after(&self, inp: &Self::InputValue) -> bool;
    fn ingest(&mut self, inp: &mut Self::InputValue);
    fn result(self) -> Vec<Self::OutputValue>;
}

pub trait AggregatableTdim<SK>: Sized
where
    SK: BinnedStreamKind,
{
    type Aggregator: AggregatorTdim<SK>;
    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator;
}

pub trait IntoBinnedT<SK, S>
where
    SK: BinnedStreamKind,
    S: Stream<Item = Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::XBinnedEvents>>, Error>> + Unpin,
{
    fn into_binned_t(self, spec: BinnedRange) -> IntoBinnedTDefaultStream<SK, S>;
}

impl<SK, S> IntoBinnedT<SK, S> for S
where
    SK: BinnedStreamKind,
    S: Stream<Item = Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::XBinnedEvents>>, Error>> + Unpin,
{
    fn into_binned_t(self, spec: BinnedRange) -> IntoBinnedTDefaultStream<SK, S> {
        IntoBinnedTDefaultStream::new(self, spec)
    }
}

pub struct IntoBinnedTDefaultStream<SK, S>
where
    SK: BinnedStreamKind,
    S: Stream<Item = Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::XBinnedEvents>>, Error>> + Unpin,
{
    inp: S,
    aggtor: Option<<<SK as BinnedStreamKind>::XBinnedEvents as AggregatableTdim<SK>>::Aggregator>,
    spec: BinnedRange,
    curbin: u32,
    inp_completed: bool,
    all_bins_emitted: bool,
    range_complete_observed: bool,
    range_complete_emitted: bool,
    left:
        Option<Poll<Option<Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::XBinnedEvents>>, Error>>>>,
    errored: bool,
    completed: bool,
    tmp_agg_results: VecDeque<<SK as BinnedStreamKind>::TBinnedBins>,
    _marker: std::marker::PhantomData<SK>,
}

impl<SK, S> IntoBinnedTDefaultStream<SK, S>
where
    SK: BinnedStreamKind,
    S: Stream<Item = Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::XBinnedEvents>>, Error>> + Unpin,
{
    pub fn new(inp: S, spec: BinnedRange) -> Self {
        let range = spec.get_range(0);
        Self {
            inp,
            aggtor: Some(
                <<SK as BinnedStreamKind>::XBinnedEvents as AggregatableTdim<SK>>::aggregator_new_static(
                    range.beg, range.end,
                ),
            ),
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
            _marker: std::marker::PhantomData::default(),
        }
    }

    fn cur(
        &mut self,
        cx: &mut Context,
    ) -> Poll<Option<Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::XBinnedEvents>>, Error>>> {
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
            .replace(
                <<SK as BinnedStreamKind>::XBinnedEvents as AggregatableTdim<SK>>::aggregator_new_static(
                    range.beg, range.end,
                ),
            )
            // TODO handle None case, or remove Option if Agg is always present
            .unwrap()
            .result();
        //self.tmp_agg_results = VecDeque::from(ret);
        self.tmp_agg_results = VecDeque::new();
        if self.curbin >= self.spec.count as u32 {
            self.all_bins_emitted = true;
        }
    }

    fn handle(
        &mut self,
        cur: Poll<Option<Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::XBinnedEvents>>, Error>>>,
    ) -> Option<Poll<Option<Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::TBinnedBins>>, Error>>>>
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
                            // Could also at least gather some stats.
                            None
                        } else {
                            let ag = self.aggtor.as_mut().unwrap();
                            //if ag.ends_before(&item) {
                            if ag.ends_before(err::todoval()) {
                                None
                            //} else if ag.starts_after(&item) {
                            } else if ag.starts_after(err::todoval()) {
                                self.left =
                                    Some(Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))))));
                                self.cycle_current_bin();
                                // TODO cycle_current_bin enqueues the bin, can I return here instead?
                                None
                            } else {
                                let mut item = item;
                                //ag.ingest(&mut item);
                                ag.ingest(err::todoval());
                                let item = item;
                                //if ag.ends_after(&item) {
                                if ag.ends_after(err::todoval()) {
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

impl<SK, S> Stream for IntoBinnedTDefaultStream<SK, S>
where
    SK: BinnedStreamKind,
    S: Stream<Item = Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::XBinnedEvents>>, Error>> + Unpin,
{
    type Item = Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::TBinnedBins>>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("IntoBinnedTDefaultStream  poll_next on completed");
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
