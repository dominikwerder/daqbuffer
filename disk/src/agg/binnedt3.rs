use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::StreamItem;
use crate::binned::{BinnedStreamKind, RangeCompletableItem};
use err::Error;
use futures_core::Stream;
use netpod::BinnedRange;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

pub trait Aggregator3Tdim {
    type InputValue;
    type OutputValue;
}

pub struct Agg3 {}

impl Aggregator3Tdim for Agg3 {
    type InputValue = MinMaxAvgScalarEventBatch;
    type OutputValue = MinMaxAvgScalarBinBatch;
}

pub struct BinnedT3Stream {
    // TODO get rid of box:
    inp: Pin<Box<dyn Stream<Item = MinMaxAvgScalarEventBatch> + Send>>,
    //aggtor: Option<<<SK as BinnedStreamKind>::XBinnedEvents as AggregatableTdim<SK>>::Aggregator>,
    aggtor: Option<()>,
    spec: BinnedRange,
    curbin: u32,
    inp_completed: bool,
    all_bins_emitted: bool,
    range_complete_observed: bool,
    range_complete_emitted: bool,
    //left: Option<Poll<Option<Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::XBinnedEvents>>, Error>>>>,
    left: Option<()>,
    errored: bool,
    completed: bool,
    tmp_agg_results: VecDeque<MinMaxAvgScalarBinBatch>,
}

impl BinnedT3Stream {
    pub fn new<S>(inp: S, spec: BinnedRange) -> Self
    where
        S: Stream<Item = MinMaxAvgScalarEventBatch> + Send + 'static,
    {
        // TODO simplify here, get rid of numeric parameter:
        let range = spec.get_range(0);
        Self {
            inp: Box::pin(inp),
            aggtor: None,
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
                err::todo();
                Pending
                // TODO `cur` and `handle` are not yet taken over from binnedt.rs
                /*let cur = self.cur(cx);
                match self.handle(cur) {
                    Some(item) => item,
                    None => continue 'outer,
                }*/
            };
        }
    }
}
