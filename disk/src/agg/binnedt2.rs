use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::StreamItem;
use crate::binned::RangeCompletableItem;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::BinnedRange;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

pub trait AggregatorTdim2: Sized + Send + Unpin {
    type InputValue;
    fn ends_before(&self, inp: &Self::InputValue) -> bool;
    fn ends_after(&self, inp: &Self::InputValue) -> bool;
    fn starts_after(&self, inp: &Self::InputValue) -> bool;
    fn ingest(&mut self, inp: &mut Self::InputValue);
    fn result(self) -> Vec<Self::InputValue>;
}

pub trait AggregatableTdim2: Sized {
    type Aggregator: AggregatorTdim2<InputValue = Self>;
    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator;
}

pub struct MinMaxAvgScalarBinBatchAgg {
    ts1: u64,
    ts2: u64,
    count: u64,
    min: f32,
    max: f32,
    sum: f32,
    sumc: u64,
}

impl MinMaxAvgScalarBinBatchAgg {
    pub fn new(ts1: u64, ts2: u64) -> Self {
        Self {
            ts1,
            ts2,
            count: 0,
            min: f32::MAX,
            max: f32::MIN,
            sum: 0f32,
            sumc: 0,
        }
    }
}

impl AggregatorTdim2 for MinMaxAvgScalarBinBatchAgg {
    type InputValue = MinMaxAvgScalarBinBatch;

    fn ends_before(&self, inp: &Self::InputValue) -> bool {
        match inp.ts2s.last() {
            Some(&ts) => ts <= self.ts1,
            None => true,
        }
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        match inp.ts2s.last() {
            Some(&ts) => ts >= self.ts2,
            _ => panic!(),
        }
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        match inp.ts1s.first() {
            Some(&ts) => ts >= self.ts2,
            _ => panic!(),
        }
    }

    fn ingest(&mut self, v: &mut Self::InputValue) {
        for i1 in 0..v.ts1s.len() {
            let ts1 = v.ts1s[i1];
            let ts2 = v.ts2s[i1];
            if ts2 <= self.ts1 {
                continue;
            } else if ts1 >= self.ts2 {
                continue;
            } else {
                self.count += v.counts[i1];
                self.min = self.min.min(v.mins[i1]);
                self.max = self.max.max(v.maxs[i1]);
                let x = v.avgs[i1];
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

    fn result(self) -> Vec<Self::InputValue> {
        let min = if self.min == f32::MAX { f32::NAN } else { self.min };
        let max = if self.max == f32::MIN { f32::NAN } else { self.max };
        let avg = if self.sumc == 0 {
            f32::NAN
        } else {
            self.sum / self.sumc as f32
        };
        let v = MinMaxAvgScalarBinBatch {
            ts1s: vec![self.ts1],
            ts2s: vec![self.ts2],
            counts: vec![self.count],
            mins: vec![min],
            maxs: vec![max],
            avgs: vec![avg],
        };
        vec![v]
    }
}

impl AggregatableTdim2 for MinMaxAvgScalarBinBatch {
    type Aggregator = MinMaxAvgScalarBinBatchAgg;

    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator {
        Self::Aggregator::new(ts1, ts2)
    }
}
