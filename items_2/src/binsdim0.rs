use crate::streams::{CollectableType, CollectorType, ToJsonResult};
use crate::{
    ts_offs_from_abs, AppendEmptyBin, Empty, IsoDateTime, ScalarOps, TimeBinnable, TimeBinnableType,
    TimeBinnableTypeAggregator, TimeBinned, TimeBinner, TimeSeries, WithLen,
};
use chrono::{TimeZone, Utc};
use err::Error;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::NanoRange;
use num_traits::Zero;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::{fmt, mem};

#[derive(Clone, Serialize, Deserialize)]
pub struct MinMaxAvgDim0Bins<NTY> {
    pub ts1s: Vec<u64>,
    pub ts2s: Vec<u64>,
    pub counts: Vec<u64>,
    pub mins: Vec<NTY>,
    pub maxs: Vec<NTY>,
    pub avgs: Vec<f32>,
}

impl<NTY> fmt::Debug for MinMaxAvgDim0Bins<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let self_name = std::any::type_name::<Self>();
        write!(
            fmt,
            "{self_name}  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  mins {:?}  maxs {:?}  avgs {:?}",
            self.ts1s.len(),
            self.ts1s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.ts2s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.counts,
            self.mins,
            self.maxs,
            self.avgs,
        )
    }
}

impl<NTY> MinMaxAvgDim0Bins<NTY> {
    pub fn empty() -> Self {
        Self {
            ts1s: vec![],
            ts2s: vec![],
            counts: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
        }
    }
}

impl<NTY> WithLen for MinMaxAvgDim0Bins<NTY> {
    fn len(&self) -> usize {
        self.ts1s.len()
    }
}

impl<NTY> Empty for MinMaxAvgDim0Bins<NTY> {
    fn empty() -> Self {
        Self {
            ts1s: Vec::new(),
            ts2s: Vec::new(),
            counts: Vec::new(),
            mins: Vec::new(),
            maxs: Vec::new(),
            avgs: Vec::new(),
        }
    }
}

impl<NTY: ScalarOps> AppendEmptyBin for MinMaxAvgDim0Bins<NTY> {
    fn append_empty_bin(&mut self, ts1: u64, ts2: u64) {
        self.ts1s.push(ts1);
        self.ts2s.push(ts2);
        self.counts.push(0);
        self.mins.push(NTY::zero());
        self.maxs.push(NTY::zero());
        self.avgs.push(0.);
    }
}

impl<NTY: ScalarOps> TimeSeries for MinMaxAvgDim0Bins<NTY> {
    fn ts_min(&self) -> Option<u64> {
        todo!("collection of bins can not be TimeSeries")
    }

    fn ts_max(&self) -> Option<u64> {
        todo!("collection of bins can not be TimeSeries")
    }

    fn ts_min_max(&self) -> Option<(u64, u64)> {
        todo!("collection of bins can not be TimeSeries")
    }
}

impl<NTY: ScalarOps> TimeBinnableType for MinMaxAvgDim0Bins<NTY> {
    type Output = MinMaxAvgDim0Bins<NTY>;
    type Aggregator = MinMaxAvgDim0BinsAggregator<NTY>;

    fn aggregator(range: NanoRange, x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        let self_name = std::any::type_name::<Self>();
        debug!(
            "TimeBinnableType for {self_name}  aggregator()  range {:?}  x_bin_count {}  do_time_weight {}",
            range, x_bin_count, do_time_weight
        );
        Self::Aggregator::new(range, do_time_weight)
    }
}

pub struct MinMaxAvgBinsCollected<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> MinMaxAvgBinsCollected<NTY> {
    pub fn new() -> Self {
        Self { _m1: PhantomData }
    }
}

#[derive(Serialize)]
pub struct MinMaxAvgBinsCollectedResult<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: Vec<u64>,
    counts: Vec<u64>,
    mins: Vec<NTY>,
    maxs: Vec<NTY>,
    avgs: Vec<f32>,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "finalisedRange")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "Zero::is_zero", rename = "missingBins")]
    missing_bins: u32,
    #[serde(skip_serializing_if = "Option::is_none", rename = "continueAt")]
    continue_at: Option<IsoDateTime>,
}

impl<NTY: ScalarOps> ToJsonResult for MinMaxAvgBinsCollectedResult<NTY> {
    fn to_json_result(&self) -> Result<Box<dyn crate::streams::ToJsonBytes>, Error> {
        let k = serde_json::to_value(self)?;
        Ok(Box::new(k))
    }
}

pub struct MinMaxAvgBinsCollector<NTY> {
    timed_out: bool,
    range_complete: bool,
    vals: MinMaxAvgDim0Bins<NTY>,
}

impl<NTY> MinMaxAvgBinsCollector<NTY> {
    pub fn new() -> Self {
        Self {
            timed_out: false,
            range_complete: false,
            vals: MinMaxAvgDim0Bins::<NTY>::empty(),
        }
    }
}

impl<NTY> WithLen for MinMaxAvgBinsCollector<NTY> {
    fn len(&self) -> usize {
        self.vals.ts1s.len()
    }
}

impl<NTY: ScalarOps> CollectorType for MinMaxAvgBinsCollector<NTY> {
    type Input = MinMaxAvgDim0Bins<NTY>;
    type Output = MinMaxAvgBinsCollectedResult<NTY>;

    fn ingest(&mut self, _src: &mut Self::Input) {
        err::todo();
    }

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(&mut self) -> Result<Self::Output, Error> {
        let bin_count = self.vals.ts1s.len() as u32;
        // TODO could save the copy:
        let mut ts_all = self.vals.ts1s.clone();
        if self.vals.ts2s.len() > 0 {
            ts_all.push(*self.vals.ts2s.last().unwrap());
        }
        info!("TODO return proper continueAt");
        let bin_count_exp = 100 as u32;
        let continue_at = if self.vals.ts1s.len() < bin_count_exp as usize {
            match ts_all.last() {
                Some(&k) => {
                    let iso = IsoDateTime(Utc.timestamp_nanos(k as i64));
                    Some(iso)
                }
                None => Err(Error::with_msg("partial_content but no bin in result"))?,
            }
        } else {
            None
        };
        let tst = ts_offs_from_abs(&ts_all);
        let counts = mem::replace(&mut self.vals.counts, Vec::new());
        let mins = mem::replace(&mut self.vals.mins, Vec::new());
        let maxs = mem::replace(&mut self.vals.maxs, Vec::new());
        let avgs = mem::replace(&mut self.vals.avgs, Vec::new());
        let ret = MinMaxAvgBinsCollectedResult::<NTY> {
            ts_anchor_sec: tst.0,
            ts_off_ms: tst.1,
            ts_off_ns: tst.2,
            counts,
            mins,
            maxs,
            avgs,
            finalised_range: self.range_complete,
            missing_bins: bin_count_exp - bin_count,
            continue_at,
        };
        Ok(ret)
    }
}

impl<NTY: ScalarOps> CollectableType for MinMaxAvgDim0Bins<NTY> {
    type Collector = MinMaxAvgBinsCollector<NTY>;

    fn new_collector() -> Self::Collector {
        Self::Collector::new()
    }
}

pub struct MinMaxAvgDim0BinsAggregator<NTY> {
    range: NanoRange,
    count: u64,
    min: NTY,
    max: NTY,
    // Carry over to next bin:
    avg: f32,
    sumc: u64,
    sum: f32,
}

impl<NTY: ScalarOps> MinMaxAvgDim0BinsAggregator<NTY> {
    pub fn new(range: NanoRange, _do_time_weight: bool) -> Self {
        Self {
            range,
            count: 0,
            min: NTY::zero(),
            max: NTY::zero(),
            avg: 0.,
            sumc: 0,
            sum: 0f32,
        }
    }
}

impl<NTY: ScalarOps> TimeBinnableTypeAggregator for MinMaxAvgDim0BinsAggregator<NTY> {
    type Input = MinMaxAvgDim0Bins<NTY>;
    type Output = MinMaxAvgDim0Bins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        for i1 in 0..item.ts1s.len() {
            if item.counts[i1] == 0 {
            } else if item.ts2s[i1] <= self.range.beg {
            } else if item.ts1s[i1] >= self.range.end {
            } else {
                if self.count == 0 {
                    self.min = item.mins[i1].clone();
                    self.max = item.maxs[i1].clone();
                } else {
                    if self.min > item.mins[i1] {
                        self.min = item.mins[i1].clone();
                    }
                    if self.max < item.maxs[i1] {
                        self.max = item.maxs[i1].clone();
                    }
                }
                self.count += item.counts[i1];
                self.sum += item.avgs[i1];
                self.sumc += 1;
            }
        }
    }

    fn result_reset(&mut self, range: NanoRange, _expand: bool) -> Self::Output {
        if self.sumc > 0 {
            self.avg = self.sum / self.sumc as f32;
        }
        let ret = Self::Output {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![self.min.clone()],
            maxs: vec![self.max.clone()],
            avgs: vec![self.avg],
        };
        self.range = range;
        self.count = 0;
        self.sum = 0f32;
        self.sumc = 0;
        ret
    }
}

impl<NTY: ScalarOps> TimeBinnable for MinMaxAvgDim0Bins<NTY> {
    fn time_binner_new(&self, edges: Vec<u64>, do_time_weight: bool) -> Box<dyn TimeBinner> {
        let ret = MinMaxAvgDim0BinsTimeBinner::<NTY>::new(edges.into(), do_time_weight);
        Box::new(ret)
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

pub struct MinMaxAvgDim0BinsTimeBinner<NTY: ScalarOps> {
    edges: VecDeque<u64>,
    do_time_weight: bool,
    agg: Option<MinMaxAvgDim0BinsAggregator<NTY>>,
    ready: Option<<MinMaxAvgDim0BinsAggregator<NTY> as TimeBinnableTypeAggregator>::Output>,
}

impl<NTY: ScalarOps> MinMaxAvgDim0BinsTimeBinner<NTY> {
    fn new(edges: VecDeque<u64>, do_time_weight: bool) -> Self {
        Self {
            edges,
            do_time_weight,
            agg: None,
            ready: None,
        }
    }

    fn next_bin_range(&mut self) -> Option<NanoRange> {
        if self.edges.len() >= 2 {
            let ret = NanoRange {
                beg: self.edges[0],
                end: self.edges[1],
            };
            self.edges.pop_front();
            Some(ret)
        } else {
            None
        }
    }
}

impl<NTY: ScalarOps> TimeBinner for MinMaxAvgDim0BinsTimeBinner<NTY> {
    fn ingest(&mut self, item: &dyn TimeBinnable) {
        let self_name = std::any::type_name::<Self>();
        if item.len() == 0 {
            // Return already here, RangeOverlapInfo would not give much sense.
            return;
        }
        if self.edges.len() < 2 {
            warn!("TimeBinnerDyn for {self_name}  no more bin in edges A");
            return;
        }
        // TODO optimize by remembering at which event array index we have arrived.
        // That needs modified interfaces which can take and yield the start and latest index.
        loop {
            while item.starts_after(NanoRange {
                beg: 0,
                end: self.edges[1],
            }) {
                self.cycle();
                if self.edges.len() < 2 {
                    warn!("TimeBinnerDyn for {self_name}  no more bin in edges B");
                    return;
                }
            }
            if item.ends_before(NanoRange {
                beg: self.edges[0],
                end: u64::MAX,
            }) {
                return;
            } else {
                if self.edges.len() < 2 {
                    warn!("TimeBinnerDyn for {self_name}  edge list exhausted");
                    return;
                } else {
                    let agg = if let Some(agg) = self.agg.as_mut() {
                        agg
                    } else {
                        self.agg = Some(MinMaxAvgDim0BinsAggregator::new(
                            // We know here that we have enough edges for another bin.
                            // and `next_bin_range` will pop the first edge.
                            self.next_bin_range().unwrap(),
                            self.do_time_weight,
                        ));
                        self.agg.as_mut().unwrap()
                    };
                    if let Some(item) = item
                        .as_any()
                        // TODO make statically sure that we attempt to cast to the correct type here:
                        .downcast_ref::<<MinMaxAvgDim0BinsAggregator<NTY> as TimeBinnableTypeAggregator>::Input>()
                    {
                        agg.ingest(item);
                    } else {
                        let tyid_item = std::any::Any::type_id(item.as_any());
                        error!("not correct item type  {:?}", tyid_item);
                    };
                    if item.ends_after(agg.range().clone()) {
                        self.cycle();
                        if self.edges.len() < 2 {
                            warn!("TimeBinnerDyn for {self_name}  no more bin in edges C");
                            return;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    fn bins_ready_count(&self) -> usize {
        match &self.ready {
            Some(k) => k.len(),
            None => 0,
        }
    }

    fn bins_ready(&mut self) -> Option<Box<dyn crate::TimeBinned>> {
        match self.ready.take() {
            Some(k) => Some(Box::new(k)),
            None => None,
        }
    }

    // TODO there is too much common code between implementors:
    fn push_in_progress(&mut self, push_empty: bool) {
        // TODO expand should be derived from AggKind. Is it still required after all?
        let expand = true;
        if let Some(agg) = self.agg.as_mut() {
            let dummy_range = NanoRange { beg: 4, end: 5 };
            let bins = agg.result_reset(dummy_range, expand);
            self.agg = None;
            assert_eq!(bins.len(), 1);
            if push_empty || bins.counts[0] != 0 {
                match self.ready.as_mut() {
                    Some(_ready) => {
                        err::todo();
                        //ready.append(&mut bins);
                    }
                    None => {
                        self.ready = Some(bins);
                    }
                }
            }
        }
    }

    // TODO there is too much common code between implementors:
    fn cycle(&mut self) {
        let n = self.bins_ready_count();
        self.push_in_progress(true);
        if self.bins_ready_count() == n {
            if let Some(_range) = self.next_bin_range() {
                let bins = MinMaxAvgDim0Bins::<NTY>::empty();
                err::todo();
                //bins.append_zero(range.beg, range.end);
                match self.ready.as_mut() {
                    Some(_ready) => {
                        err::todo();
                        //ready.append(&mut bins);
                    }
                    None => {
                        self.ready = Some(bins);
                    }
                }
                if self.bins_ready_count() <= n {
                    error!("failed to push a zero bin");
                }
            } else {
                warn!("cycle: no in-progress bin pushed, but also no more bin to add as zero-bin");
            }
        }
    }
}

impl<NTY: ScalarOps> TimeBinned for MinMaxAvgDim0Bins<NTY> {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnable {
        self as &dyn TimeBinnable
    }

    fn edges_slice(&self) -> (&[u64], &[u64]) {
        (&self.ts1s[..], &self.ts2s[..])
    }

    fn counts(&self) -> &[u64] {
        &self.counts[..]
    }

    fn mins(&self) -> Vec<f32> {
        self.mins.iter().map(|x| x.clone().as_prim_f32()).collect()
    }

    fn maxs(&self) -> Vec<f32> {
        self.maxs.iter().map(|x| x.clone().as_prim_f32()).collect()
    }

    fn avgs(&self) -> Vec<f32> {
        self.avgs.clone()
    }

    fn validate(&self) -> Result<(), String> {
        use std::fmt::Write;
        let mut msg = String::new();
        if self.ts1s.len() != self.ts2s.len() {
            write!(&mut msg, "ts1s ≠ ts2s\n").unwrap();
        }
        for (i, ((count, min), max)) in self.counts.iter().zip(&self.mins).zip(&self.maxs).enumerate() {
            if min.as_prim_f32() < 1. && *count != 0 {
                write!(&mut msg, "i {}  count {}  min {:?}  max {:?}\n", i, count, min, max).unwrap();
            }
        }
        if msg.is_empty() {
            Ok(())
        } else {
            Err(msg)
        }
    }
}
