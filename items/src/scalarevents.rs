use crate::binsdim0::MinMaxAvgDim0Bins;
use crate::numops::NumOps;
use crate::streams::{Collectable, Collector};
use crate::{
    pulse_offs_from_abs, ts_offs_from_abs, Appendable, ByteEstimate, Clearable, EventAppendable, EventsDyn,
    FilterFittingInside, Fits, FitsInside, FrameTypeStaticSYC, NewEmpty, PushableIndex, RangeOverlapInfo, ReadPbv,
    ReadableFromFile, SitemtyFrameType, TimeBinnableDyn, TimeBinnableType, TimeBinnableTypeAggregator, TimeBinnerDyn,
    WithLen, WithTimestamps,
};
use err::Error;
use netpod::log::*;
use netpod::{NanoRange, Shape};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use tokio::fs::File;

// TODO in this module reduce clones.

#[derive(Serialize, Deserialize)]
pub struct ScalarEvents<NTY> {
    pub tss: Vec<u64>,
    pub pulses: Vec<u64>,
    pub values: Vec<NTY>,
}

impl<NTY> ScalarEvents<NTY> {
    #[inline(always)]
    pub fn push(&mut self, ts: u64, pulse: u64, value: NTY) {
        self.tss.push(ts);
        self.pulses.push(pulse);
        self.values.push(value);
    }

    // TODO should avoid the copies.
    #[inline(always)]
    pub fn extend_from_slice(&mut self, src: &Self)
    where
        NTY: Clone,
    {
        self.tss.extend_from_slice(&src.tss);
        self.pulses.extend_from_slice(&src.pulses);
        self.values.extend_from_slice(&src.values);
    }

    #[inline(always)]
    pub fn clearx(&mut self) {
        self.tss.clear();
        self.pulses.clear();
        self.values.clear();
    }
}

impl<NTY> FrameTypeStaticSYC for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    const FRAME_TYPE_ID: u32 = crate::EVENTS_0D_FRAME_TYPE_ID + NTY::SUB;
}

impl<NTY> SitemtyFrameType for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeStaticSYC>::FRAME_TYPE_ID
    }
}

impl<NTY> ScalarEvents<NTY> {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            pulses: vec![],
            values: vec![],
        }
    }
}

impl<NTY> fmt::Debug for ScalarEvents<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "count {}  ts {:?} .. {:?}  vals {:?} .. {:?}",
            self.tss.len(),
            self.tss.first(),
            self.tss.last(),
            self.values.first(),
            self.values.last(),
        )
    }
}

impl<NTY> WithLen for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<NTY> WithTimestamps for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

impl<NTY> ByteEstimate for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn byte_estimate(&self) -> u64 {
        if self.tss.len() == 0 {
            0
        } else {
            // TODO improve via a const fn on NTY
            self.tss.len() as u64 * 16
        }
    }
}

impl<NTY> RangeOverlapInfo for ScalarEvents<NTY> {
    fn ends_before(&self, range: NanoRange) -> bool {
        match self.tss.last() {
            Some(&ts) => ts < range.beg,
            None => true,
        }
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        match self.tss.last() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        match self.tss.first() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }
}

impl<NTY> FitsInside for ScalarEvents<NTY> {
    fn fits_inside(&self, range: NanoRange) -> Fits {
        if self.tss.is_empty() {
            Fits::Empty
        } else {
            let t1 = *self.tss.first().unwrap();
            let t2 = *self.tss.last().unwrap();
            if t2 < range.beg {
                Fits::Lower
            } else if t1 > range.end {
                Fits::Greater
            } else if t1 < range.beg && t2 > range.end {
                Fits::PartlyLowerAndGreater
            } else if t1 < range.beg {
                Fits::PartlyLower
            } else if t2 > range.end {
                Fits::PartlyGreater
            } else {
                Fits::Inside
            }
        }
    }
}

impl<NTY> FilterFittingInside for ScalarEvents<NTY> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> PushableIndex for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.push(src.tss[ix], src.pulses[ix], src.values[ix].clone());
    }
}

impl<NTY> NewEmpty for ScalarEvents<NTY> {
    fn empty(_shape: Shape) -> Self {
        Self {
            tss: Vec::new(),
            pulses: Vec::new(),
            values: Vec::new(),
        }
    }
}

impl<NTY> Appendable for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn empty_like_self(&self) -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.extend_from_slice(src);
    }

    fn append_zero(&mut self, ts1: u64, _ts2: u64) {
        self.tss.push(ts1);
        self.pulses.push(0);
        self.values.push(NTY::zero());
    }
}

impl<NTY> Clearable for ScalarEvents<NTY> {
    fn clear(&mut self) {
        ScalarEvents::<NTY>::clearx(self);
    }
}

impl<NTY> ReadableFromFile for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn read_from_file(_file: File) -> Result<ReadPbv<Self>, Error> {
        // TODO refactor types such that this can be removed.
        panic!()
    }

    fn from_buf(_buf: &[u8]) -> Result<Self, Error> {
        panic!()
    }
}

impl<NTY> TimeBinnableType for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgDim0Bins<NTY>;
    type Aggregator = EventValuesAggregator<NTY>;

    fn aggregator(range: NanoRange, x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        debug!(
            "TimeBinnableType for EventValues  aggregator()  range {:?}  x_bin_count {}  do_time_weight {}",
            range, x_bin_count, do_time_weight
        );
        Self::Aggregator::new(range, do_time_weight)
    }
}

pub struct EventValuesCollector<NTY> {
    vals: ScalarEvents<NTY>,
    range_complete: bool,
    timed_out: bool,
}

impl<NTY> EventValuesCollector<NTY> {
    pub fn new() -> Self {
        Self {
            vals: ScalarEvents::empty(),
            range_complete: false,
            timed_out: false,
        }
    }
}

impl<NTY> WithLen for EventValuesCollector<NTY> {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

#[derive(Serialize)]
pub struct EventValuesCollectorOutput<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: Vec<u64>,
    #[serde(rename = "pulseAnchor")]
    pulse_anchor: u64,
    #[serde(rename = "pulseOff")]
    pulse_off: Vec<u64>,
    values: Vec<NTY>,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "finalisedRange")]
    range_complete: bool,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "timedOut")]
    timed_out: bool,
}

impl<NTY> Collector for EventValuesCollector<NTY>
where
    NTY: NumOps,
{
    type Input = ScalarEvents<NTY>;
    type Output = EventValuesCollectorOutput<NTY>;

    fn ingest(&mut self, src: &Self::Input) {
        self.vals.append(src);
    }

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(self) -> Result<Self::Output, Error> {
        let tst = ts_offs_from_abs(&self.vals.tss);
        let (pulse_anchor, pulse_off) = pulse_offs_from_abs(&self.vals.pulses);
        let ret = Self::Output {
            ts_anchor_sec: tst.0,
            ts_off_ms: tst.1,
            ts_off_ns: tst.2,
            pulse_anchor,
            pulse_off,
            values: self.vals.values,
            range_complete: self.range_complete,
            timed_out: self.timed_out,
        };
        Ok(ret)
    }
}

impl<NTY> Collectable for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    type Collector = EventValuesCollector<NTY>;

    fn new_collector(_bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new()
    }
}

pub struct EventValuesAggregator<NTY> {
    range: NanoRange,
    count: u64,
    min: NTY,
    max: NTY,
    sumc: u64,
    sum: f32,
    int_ts: u64,
    last_ts: u64,
    last_val: Option<NTY>,
    do_time_weight: bool,
    events_taken_count: u64,
    events_ignored_count: u64,
}

impl<NTY> Drop for EventValuesAggregator<NTY> {
    fn drop(&mut self) {
        // TODO collect as stats for the request context:
        trace!(
            "taken {}  ignored {}",
            self.events_taken_count,
            self.events_ignored_count
        );
    }
}

impl<NTY> EventValuesAggregator<NTY>
where
    NTY: NumOps,
{
    pub fn new(range: NanoRange, do_time_weight: bool) -> Self {
        let int_ts = range.beg;
        Self {
            range,
            count: 0,
            min: NTY::zero(),
            max: NTY::zero(),
            sum: 0.,
            sumc: 0,
            int_ts,
            last_ts: 0,
            last_val: None,
            do_time_weight,
            events_taken_count: 0,
            events_ignored_count: 0,
        }
    }

    // TODO reduce clone.. optimize via more traits to factor the trade-offs?
    fn apply_min_max(&mut self, val: NTY) {
        if self.count == 0 {
            self.min = val.clone();
            self.max = val.clone();
        } else {
            if self.min > val {
                self.min = val.clone();
            }
            if self.max < val {
                self.max = val.clone();
            }
        }
    }

    fn apply_event_unweight(&mut self, val: NTY) {
        let vf = val.as_prim_f32();
        self.apply_min_max(val);
        if vf.is_nan() {
        } else {
            self.sum += vf;
            self.sumc += 1;
        }
    }

    fn apply_event_time_weight(&mut self, ts: u64) {
        if let Some(v) = &self.last_val {
            let vf = v.as_prim_f32();
            let v2 = v.clone();
            self.apply_min_max(v2);
            let w = if self.do_time_weight {
                (ts - self.int_ts) as f32 * 1e-9
            } else {
                1.
            };
            if vf.is_nan() {
            } else {
                self.sum += vf * w;
                self.sumc += 1;
            }
            self.int_ts = ts;
        } else {
            debug!(
                "apply_event_time_weight NO VALUE  {}",
                ts as i64 - self.range.beg as i64
            );
        }
    }

    fn ingest_unweight(&mut self, item: &<Self as TimeBinnableTypeAggregator>::Input) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let val = item.values[i1].clone();
            if ts < self.range.beg {
                self.events_ignored_count += 1;
            } else if ts >= self.range.end {
                self.events_ignored_count += 1;
                return;
            } else {
                self.apply_event_unweight(val);
                self.count += 1;
                self.events_taken_count += 1;
            }
        }
    }

    fn ingest_time_weight(&mut self, item: &<Self as TimeBinnableTypeAggregator>::Input) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let val = item.values[i1].clone();
            if ts < self.int_ts {
                if self.last_val.is_none() {
                    info!(
                        "ingest_time_weight event before range, only set last  ts {}  val {:?}",
                        ts, val
                    );
                }
                self.events_ignored_count += 1;
                self.last_ts = ts;
                self.last_val = Some(val);
            } else if ts >= self.range.end {
                self.events_ignored_count += 1;
                return;
            } else {
                self.apply_event_time_weight(ts);
                if self.last_val.is_none() {
                    info!(
                        "call apply_min_max without last val, use current instead  {}  {:?}",
                        ts, val
                    );
                    self.apply_min_max(val.clone());
                }
                self.count += 1;
                self.last_ts = ts;
                self.last_val = Some(val);
                self.events_taken_count += 1;
            }
        }
    }

    fn result_reset_unweight(&mut self, range: NanoRange, _expand: bool) -> MinMaxAvgDim0Bins<NTY> {
        let (min, max, avg) = if self.sumc > 0 {
            let avg = self.sum / self.sumc as f32;
            (self.min.clone(), self.max.clone(), avg)
        } else {
            let g = match &self.last_val {
                Some(x) => x.clone(),
                None => NTY::zero(),
            };
            (g.clone(), g.clone(), g.as_prim_f32())
        };
        let ret = MinMaxAvgDim0Bins {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![min],
            maxs: vec![max],
            avgs: vec![avg],
        };
        self.int_ts = range.beg;
        self.range = range;
        self.count = 0;
        self.sum = 0f32;
        self.sumc = 0;
        ret
    }

    fn result_reset_time_weight(&mut self, range: NanoRange, expand: bool) -> MinMaxAvgDim0Bins<NTY> {
        // TODO check callsite for correct expand status.
        if expand {
            debug!("result_reset_time_weight calls apply_event_time_weight");
            self.apply_event_time_weight(self.range.end);
        } else {
            debug!("result_reset_time_weight NO EXPAND");
        }
        let (min, max, avg) = if self.sumc > 0 {
            let avg = self.sum / (self.range.delta() as f32 * 1e-9);
            (self.min.clone(), self.max.clone(), avg)
        } else {
            let g = match &self.last_val {
                Some(x) => x.clone(),
                None => NTY::zero(),
            };
            (g.clone(), g.clone(), g.as_prim_f32())
        };
        let ret = MinMaxAvgDim0Bins {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![min],
            maxs: vec![max],
            avgs: vec![avg],
        };
        self.int_ts = range.beg;
        self.range = range;
        self.count = 0;
        self.sum = 0f32;
        self.sumc = 0;
        ret
    }
}

impl<NTY> TimeBinnableTypeAggregator for EventValuesAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = ScalarEvents<NTY>;
    type Output = MinMaxAvgDim0Bins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        debug!("ingest  len {}", item.len());
        if self.do_time_weight {
            self.ingest_time_weight(item)
        } else {
            self.ingest_unweight(item)
        }
    }

    fn result_reset(&mut self, range: NanoRange, expand: bool) -> Self::Output {
        debug!("Produce for {:?}   next {:?}", self.range, range);
        if self.do_time_weight {
            self.result_reset_time_weight(range, expand)
        } else {
            self.result_reset_unweight(range, expand)
        }
    }
}

impl<NTY> EventAppendable for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    type Value = NTY;

    fn append_event(ret: Option<Self>, ts: u64, pulse: u64, value: Self::Value) -> Self {
        let mut ret = if let Some(ret) = ret { ret } else { Self::empty() };
        ret.push(ts, pulse, value);
        ret
    }
}

impl<NTY: NumOps + 'static> TimeBinnableDyn for ScalarEvents<NTY> {
    fn time_binner_new(&self, edges: Vec<u64>, do_time_weight: bool) -> Box<dyn TimeBinnerDyn> {
        let ret = ScalarEventsTimeBinner::<NTY>::new(edges.into(), do_time_weight);
        Box::new(ret)
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

impl<NTY: NumOps + 'static> EventsDyn for ScalarEvents<NTY> {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnableDyn {
        self as &dyn TimeBinnableDyn
    }

    fn verify(&self) {
        let mut ts_max = 0;
        for ts in &self.tss {
            let ts = *ts;
            if ts < ts_max {
                error!("unordered event data  ts {}  ts_max {}", ts, ts_max);
            }
            ts_max = ts_max.max(ts);
        }
    }

    fn output_info(&self) {
        if false {
            info!("output_info  len {}", self.tss.len());
            if self.tss.len() == 1 {
                info!(
                    "  only:  ts {}  pulse {}  value {:?}",
                    self.tss[0], self.pulses[0], self.values[0]
                );
            } else if self.tss.len() > 1 {
                info!(
                    "  first: ts {}  pulse {}  value {:?}",
                    self.tss[0], self.pulses[0], self.values[0]
                );
                let n = self.tss.len() - 1;
                info!(
                    "  last:  ts {}  pulse {}  value {:?}",
                    self.tss[n], self.pulses[n], self.values[n]
                );
            }
        }
    }
}

pub struct ScalarEventsTimeBinner<NTY: NumOps> {
    // The first two edges are used the next time that we create an aggregator, or push a zero bin.
    edges: VecDeque<u64>,
    do_time_weight: bool,
    agg: Option<EventValuesAggregator<NTY>>,
    ready: Option<<EventValuesAggregator<NTY> as TimeBinnableTypeAggregator>::Output>,
}

impl<NTY: NumOps> ScalarEventsTimeBinner<NTY> {
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

impl<NTY: NumOps + 'static> TimeBinnerDyn for ScalarEventsTimeBinner<NTY> {
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

    fn ingest(&mut self, item: &dyn TimeBinnableDyn) {
        const SELF: &str = "ScalarEventsTimeBinner";
        if item.len() == 0 {
            // Return already here, RangeOverlapInfo would not give much sense.
            return;
        }
        if self.edges.len() < 2 {
            warn!("TimeBinnerDyn for {SELF}  no more bin in edges A");
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
                    warn!("TimeBinnerDyn for {SELF}  no more bin in edges B");
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
                    warn!("TimeBinnerDyn for {SELF}  edge list exhausted");
                    return;
                } else {
                    let agg = if let Some(agg) = self.agg.as_mut() {
                        agg
                    } else {
                        self.agg = Some(EventValuesAggregator::new(
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
                        .downcast_ref::<<EventValuesAggregator<NTY> as TimeBinnableTypeAggregator>::Input>()
                    {
                        // TODO collect statistics associated with this request:
                        agg.ingest(item);
                    } else {
                        error!("not correct item type");
                    };
                    if item.ends_after(agg.range().clone()) {
                        self.cycle();
                        if self.edges.len() < 2 {
                            warn!("TimeBinnerDyn for {SELF}  no more bin in edges C");
                            return;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    fn push_in_progress(&mut self, push_empty: bool) {
        // TODO expand should be derived from AggKind. Is it still required after all?
        // TODO here, the expand means that agg will assume that the current value is kept constant during
        // the rest of the time range.
        let expand = true;
        let range_next = if self.agg.is_some() {
            if let Some(x) = self.next_bin_range() {
                Some(x)
            } else {
                None
            }
        } else {
            None
        };
        if let Some(agg) = self.agg.as_mut() {
            let mut bins;
            if let Some(range_next) = range_next {
                bins = agg.result_reset(range_next, expand);
            } else {
                let range_next = NanoRange { beg: 4, end: 5 };
                bins = agg.result_reset(range_next, expand);
                self.agg = None;
            }
            assert_eq!(bins.len(), 1);
            if push_empty || bins.counts[0] != 0 {
                match self.ready.as_mut() {
                    Some(ready) => {
                        ready.append(&mut bins);
                    }
                    None => {
                        self.ready = Some(bins);
                    }
                }
            }
        }
    }

    fn cycle(&mut self) {
        let n = self.bins_ready_count();
        self.push_in_progress(true);
        if self.bins_ready_count() == n {
            if let Some(range) = self.next_bin_range() {
                let mut bins = MinMaxAvgDim0Bins::<NTY>::empty();
                bins.append_zero(range.beg, range.end);
                match self.ready.as_mut() {
                    Some(ready) => {
                        ready.append(&mut bins);
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
