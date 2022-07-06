use crate::binsdim0::MinMaxAvgDim0Bins;
use crate::numops::NumOps;
use crate::streams::{Collectable, Collector};
use crate::{
    pulse_offs_from_abs, ts_offs_from_abs, Appendable, ByteEstimate, Clearable, EventAppendable, EventsDyn,
    FilterFittingInside, Fits, FitsInside, FrameTypeStatic, NewEmpty, PushableIndex, RangeOverlapInfo, ReadPbv,
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

impl<NTY> FrameTypeStatic for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    const FRAME_TYPE_ID: u32 = crate::EVENT_VALUES_FRAME_TYPE_ID + NTY::SUB;

    fn from_error(_: err::Error) -> Self {
        // TODO this method should not be used, remove.
        error!("impl<NTY> FrameTypeStatic for ScalarEvents<NTY>");
        panic!()
    }
}

impl<NTY> SitemtyFrameType for ScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeStatic>::FRAME_TYPE_ID
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
        warn!(
            "taken {}  ignored {}",
            self.events_taken_count, self.events_ignored_count
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
            sum: 0f32,
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
            self.max = val;
        } else {
            if val < self.min {
                self.min = val.clone();
            }
            if val > self.max {
                self.max = val;
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
                self.events_ignored_count += 1;
                self.last_ts = ts;
                self.last_val = Some(val);
            } else if ts >= self.range.end {
                self.events_ignored_count += 1;
                return;
            } else {
                debug!("regular");
                self.apply_event_time_weight(ts);
                self.count += 1;
                self.last_ts = ts;
                self.last_val = Some(val);
                self.events_taken_count += 1;
            }
        }
    }

    fn result_reset_unweight(&mut self, range: NanoRange, _expand: bool) -> MinMaxAvgDim0Bins<NTY> {
        let avg = if self.sumc == 0 {
            0f32
        } else {
            self.sum / self.sumc as f32
        };
        let ret = MinMaxAvgDim0Bins {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![self.min.clone()],
            maxs: vec![self.max.clone()],
            avgs: vec![avg],
        };
        self.int_ts = range.beg;
        self.range = range;
        self.count = 0;
        self.min = NTY::zero();
        self.max = NTY::zero();
        self.sum = 0f32;
        self.sumc = 0;
        ret
    }

    fn result_reset_time_weight(&mut self, range: NanoRange, expand: bool) -> MinMaxAvgDim0Bins<NTY> {
        // TODO check callsite for correct expand status.
        if true || expand {
            debug!("result_reset_time_weight calls apply_event_time_weight");
            self.apply_event_time_weight(self.range.end);
        } else {
            debug!("result_reset_time_weight NO EXPAND");
        }
        let avg = {
            let sc = self.range.delta() as f32 * 1e-9;
            self.sum / sc
        };
        let ret = MinMaxAvgDim0Bins {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![self.min.clone()],
            maxs: vec![self.max.clone()],
            avgs: vec![avg],
        };
        self.int_ts = range.beg;
        self.range = range;
        self.count = 0;
        self.min = NTY::zero();
        self.max = NTY::zero();
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
        eprintln!("ScalarEvents time_binner_new");
        info!("ScalarEvents time_binner_new");
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
}

pub struct ScalarEventsTimeBinner<NTY: NumOps> {
    edges: VecDeque<u64>,
    do_time_weight: bool,
    range: NanoRange,
    agg: Option<EventValuesAggregator<NTY>>,
    ready: Option<<EventValuesAggregator<NTY> as TimeBinnableTypeAggregator>::Output>,
}

impl<NTY: NumOps> ScalarEventsTimeBinner<NTY> {
    fn new(edges: VecDeque<u64>, do_time_weight: bool) -> Self {
        let range = if edges.len() >= 2 {
            NanoRange {
                beg: edges[0],
                end: edges[1],
            }
        } else {
            // Using a dummy for this case.
            NanoRange { beg: 1, end: 2 }
        };
        Self {
            edges,
            do_time_weight,
            range,
            agg: None,
            ready: None,
        }
    }

    // Move the bin from the current aggregator (if any) to our output collection,
    // and step forward in our bin list.
    fn cycle(&mut self) {
        // TODO expand should be derived from AggKind. Is it still required after all?
        let expand = true;
        if let Some(agg) = self.agg.as_mut() {
            let mut h = agg.result_reset(self.range.clone(), expand);
            match self.ready.as_mut() {
                Some(fin) => {
                    fin.append(&mut h);
                }
                None => {
                    self.ready = Some(h);
                }
            }
        } else {
            let mut h = MinMaxAvgDim0Bins::<NTY>::empty();
            h.append_zero(self.range.beg, self.range.end);
            match self.ready.as_mut() {
                Some(fin) => {
                    fin.append(&mut h);
                }
                None => {
                    self.ready = Some(h);
                }
            }
        }
        self.edges.pop_front();
        if self.edges.len() >= 2 {
            self.range = NanoRange {
                beg: self.edges[0],
                end: self.edges[1],
            };
        } else {
            // Using a dummy for this case.
            self.range = NanoRange { beg: 1, end: 2 };
        }
    }
}

impl<NTY: NumOps + 'static> TimeBinnerDyn for ScalarEventsTimeBinner<NTY> {
    fn cycle(&mut self) {
        Self::cycle(self)
    }

    fn ingest(&mut self, item: &dyn TimeBinnableDyn) {
        if item.len() == 0 {
            // Return already here, RangeOverlapInfo would not give much sense.
            return;
        }
        if self.edges.len() < 2 {
            warn!("TimeBinnerDyn for ScalarEventsTimeBinner  no more bin in edges A");
            return;
        }
        // TODO optimize by remembering at which event array index we have arrived.
        // That needs modified interfaces which can take and yield the start and latest index.
        loop {
            while item.starts_after(self.range.clone()) {
                self.cycle();
                if self.edges.len() < 2 {
                    warn!("TimeBinnerDyn for ScalarEventsTimeBinner  no more bin in edges B");
                    return;
                }
            }
            if item.ends_before(self.range.clone()) {
                return;
            } else {
                if self.edges.len() < 2 {
                    warn!("TimeBinnerDyn for ScalarEventsTimeBinner  edge list exhausted");
                    return;
                } else {
                    if self.agg.is_none() {
                        self.agg = Some(EventValuesAggregator::new(self.range.clone(), self.do_time_weight));
                    }
                    let agg = self.agg.as_mut().unwrap();
                    if let Some(item) = item
                        .as_any()
                        .downcast_ref::<<EventValuesAggregator<NTY> as TimeBinnableTypeAggregator>::Input>()
                    {
                        // TODO collect statistics associated with this request:
                        agg.ingest(item);
                    } else {
                        error!("not correct item type");
                    };
                    if item.ends_after(self.range.clone()) {
                        self.cycle();
                        if self.edges.len() < 2 {
                            warn!("TimeBinnerDyn for ScalarEventsTimeBinner  no more bin in edges C");
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
}
