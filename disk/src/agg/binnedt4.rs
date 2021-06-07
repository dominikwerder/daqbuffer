use crate::agg::enp::XBinnedScalarEvents;
use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::StreamItem;
use crate::binned::{BinsTimeBinner, EventsTimeBinner, MinMaxAvgBins, NumOps, RangeCompletableItem, RangeOverlapInfo};
use crate::decode::EventValues;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{BinnedRange, NanoRange};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct DefaultScalarEventsTimeBinner<VT> {
    _m1: PhantomData<VT>,
}

impl<NTY> EventsTimeBinner for DefaultScalarEventsTimeBinner<NTY>
where
    NTY: NumOps,
{
    type Input = EventValues<NTY>;
    type Output = MinMaxAvgBins<NTY>;

    fn process(inp: Self::Input) -> Self::Output {
        todo!()
    }
}

pub struct DefaultSingleXBinTimeBinner<VT> {
    _m1: PhantomData<VT>,
}

impl<NTY> EventsTimeBinner for DefaultSingleXBinTimeBinner<NTY>
where
    NTY: NumOps,
{
    type Input = XBinnedScalarEvents<NTY>;
    // TODO is that output type good enough for now?
    type Output = MinMaxAvgBins<NTY>;

    fn process(inp: Self::Input) -> Self::Output {
        todo!()
    }
}

pub struct DefaultBinsTimeBinner<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> BinsTimeBinner for DefaultBinsTimeBinner<NTY>
where
    NTY: NumOps,
{
    type Input = MinMaxAvgBins<NTY>;
    type Output = MinMaxAvgBins<NTY>;

    fn process(inp: Self::Input) -> Self::Output {
        todo!()
    }
}

pub trait Aggregator3Tdim {
    type InputValue;
    type OutputValue;
}

pub struct Agg3 {
    range: NanoRange,
    count: u64,
    min: f32,
    max: f32,
    sum: f32,
    sumc: u64,
}

impl Agg3 {
    fn new(range: NanoRange) -> Self {
        Self {
            range,
            count: 0,
            min: f32::MAX,
            max: f32::MIN,
            sum: f32::NAN,
            sumc: 0,
        }
    }

    fn ingest(&mut self, item: &MinMaxAvgScalarEventBatch) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            if ts < self.range.beg {
                continue;
            } else if ts >= self.range.end {
                continue;
            } else {
                self.count += 1;
                self.min = self.min.min(item.mins[i1]);
                self.max = self.max.max(item.maxs[i1]);
                let x = item.avgs[i1];
                if x.is_nan() {
                } else {
                    if self.sum.is_nan() {
                        self.sum = x;
                    } else {
                        self.sum += x;
                    }
                    self.sumc += 1;
                }
            }
        }
    }

    fn result(self) -> Vec<MinMaxAvgScalarBinBatch> {
        let min = if self.min == f32::MAX { f32::NAN } else { self.min };
        let max = if self.max == f32::MIN { f32::NAN } else { self.max };
        let avg = if self.sumc == 0 {
            f32::NAN
        } else {
            self.sum / self.sumc as f32
        };
        let v = MinMaxAvgScalarBinBatch {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![min],
            maxs: vec![max],
            avgs: vec![avg],
        };
        vec![v]
    }
}

impl Aggregator3Tdim for Agg3 {
    type InputValue = MinMaxAvgScalarEventBatch;
    type OutputValue = MinMaxAvgScalarBinBatch;
}

pub struct BinnedT3Stream {
    // TODO get rid of box:
    inp: Pin<Box<dyn Stream<Item = Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarEventBatch>>, Error>> + Send>>,
    //aggtor: Option<<<SK as BinnedStreamKind>::XBinnedEvents as AggregatableTdim<SK>>::Aggregator>,
    aggtor: Option<Agg3>,
    spec: BinnedRange,
    curbin: u32,
    inp_completed: bool,
    all_bins_emitted: bool,
    range_complete_observed: bool,
    range_complete_emitted: bool,
    left: Option<Poll<Option<Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarEventBatch>>, Error>>>>,
    errored: bool,
    completed: bool,
    tmp_agg_results: VecDeque<MinMaxAvgScalarBinBatch>,
}

impl BinnedT3Stream {
    pub fn new<S>(inp: S, spec: BinnedRange) -> Self
    where
        S: Stream<Item = Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarEventBatch>>, Error>> + Send + 'static,
    {
        let range = spec.get_range(0);
        Self {
            inp: Box::pin(inp),
            aggtor: Some(Agg3::new(range)),
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

    fn cur(
        &mut self,
        cx: &mut Context,
    ) -> Poll<Option<Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarEventBatch>>, Error>>> {
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
            .replace(Agg3::new(range))
            // TODO handle None case, or remove Option if Agg is always present
            .unwrap()
            .result();
        self.tmp_agg_results = VecDeque::from(ret);
        if self.curbin >= self.spec.count as u32 {
            self.all_bins_emitted = true;
        }
    }

    fn handle(
        &mut self,
        cur: Poll<Option<Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarEventBatch>>, Error>>>,
    ) -> Option<Poll<Option<Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarBinBatch>>, Error>>>> {
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
                            if item.ends_before(ag.range.clone()) {
                                None
                            } else if item.starts_after(ag.range.clone()) {
                                self.left =
                                    Some(Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))))));
                                self.cycle_current_bin();
                                // TODO cycle_current_bin enqueues the bin, can I return here instead?
                                None
                            } else {
                                ag.ingest(&item);
                                if item.ends_after(ag.range.clone()) {
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

impl Stream for BinnedT3Stream {
    type Item = Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarBinBatch>>, Error>;

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
