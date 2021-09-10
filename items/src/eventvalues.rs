use crate::minmaxavgbins::MinMaxAvgBins;
use crate::numops::NumOps;
use crate::streams::{Collectable, Collector};
use crate::{
    ts_offs_from_abs, Appendable, EventAppendable, FilterFittingInside, Fits, FitsInside, PushableIndex,
    RangeOverlapInfo, ReadPbv, ReadableFromFile, SitemtyFrameType, TimeBinnableType, TimeBinnableTypeAggregator,
    WithLen, WithTimestamps,
};
use err::Error;
use netpod::timeunits::*;
use netpod::NanoRange;
use serde::{Deserialize, Serialize};
use std::fmt;
use tokio::fs::File;

// TODO add pulse.
// TODO change name, it's not only about values, but more like batch of whole events.
#[derive(Serialize, Deserialize)]
pub struct EventValues<VT> {
    pub tss: Vec<u64>,
    pub values: Vec<VT>,
}

impl<NTY> SitemtyFrameType for EventValues<NTY>
where
    NTY: NumOps,
{
    const FRAME_TYPE_ID: u32 = crate::EVENT_VALUES_FRAME_TYPE_ID + NTY::SUB;
}

impl<VT> EventValues<VT> {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            values: vec![],
        }
    }
}

impl<VT> fmt::Debug for EventValues<VT>
where
    VT: fmt::Debug,
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
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

impl<VT> WithLen for EventValues<VT> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<VT> WithTimestamps for EventValues<VT> {
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

impl<VT> RangeOverlapInfo for EventValues<VT> {
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

impl<VT> FitsInside for EventValues<VT> {
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

impl<VT> FilterFittingInside for EventValues<VT> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> PushableIndex for EventValues<NTY>
where
    NTY: NumOps,
{
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push(src.tss[ix]);
        self.values.push(src.values[ix]);
    }
}

impl<NTY> Appendable for EventValues<NTY>
where
    NTY: NumOps,
{
    fn empty() -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.tss.extend_from_slice(&src.tss);
        self.values.extend_from_slice(&src.values);
    }
}

impl<NTY> ReadableFromFile for EventValues<NTY>
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

impl<NTY> TimeBinnableType for EventValues<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgBins<NTY>;
    type Aggregator = EventValuesAggregator<NTY>;

    fn aggregator(range: NanoRange, _bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        // TODO remove output
        if range.delta() > SEC * 5000 {
            netpod::log::info!("TimeBinnableType for EventValues  aggregator()  range {:?}", range);
        }
        Self::Aggregator::new(range, do_time_weight)
    }
}

pub struct EventValuesCollector<NTY> {
    vals: EventValues<NTY>,
    range_complete: bool,
    timed_out: bool,
}

impl<NTY> EventValuesCollector<NTY> {
    pub fn new() -> Self {
        Self {
            vals: EventValues::empty(),
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
    type Input = EventValues<NTY>;
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
        let ret = Self::Output {
            ts_anchor_sec: tst.0,
            ts_off_ms: tst.1,
            ts_off_ns: tst.2,
            values: self.vals.values,
            range_complete: self.range_complete,
            timed_out: self.timed_out,
        };
        Ok(ret)
    }
}

impl<NTY> Collectable for EventValues<NTY>
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
    min: Option<NTY>,
    max: Option<NTY>,
    sumc: u64,
    sum: f32,
    int_ts: u64,
    last_ts: u64,
    last_val: Option<NTY>,
    do_time_weight: bool,
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
            min: None,
            max: None,
            sum: 0f32,
            sumc: 0,
            int_ts,
            last_ts: 0,
            last_val: None,
            do_time_weight,
        }
    }

    fn apply_min_max(&mut self, val: NTY) {
        self.min = match self.min {
            None => Some(val),
            Some(min) => {
                if val < min {
                    Some(val)
                } else {
                    Some(min)
                }
            }
        };
        self.max = match self.max {
            None => Some(val),
            Some(max) => {
                if val > max {
                    Some(val)
                } else {
                    Some(max)
                }
            }
        };
    }

    fn apply_event_unweight(&mut self, val: NTY) {
        self.apply_min_max(val);
        let vf = val.as_();
        if vf.is_nan() {
        } else {
            self.sum += vf;
            self.sumc += 1;
        }
    }

    fn apply_event_time_weight(&mut self, ts: u64, val: Option<NTY>) {
        if let Some(v) = self.last_val {
            self.apply_min_max(v);
            let w = if self.do_time_weight {
                (ts - self.int_ts) as f32 * 1e-9
            } else {
                1.
            };
            let vf = v.as_();
            if vf.is_nan() {
            } else {
                self.sum += vf * w;
                self.sumc += 1;
            }
            self.int_ts = ts;
        }
        self.last_ts = ts;
        self.last_val = val;
    }

    fn ingest_unweight(&mut self, item: &<Self as TimeBinnableTypeAggregator>::Input) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let val = item.values[i1];
            if ts < self.range.beg {
            } else if ts >= self.range.end {
            } else {
                self.count += 1;
                self.apply_event_unweight(val);
            }
        }
    }

    fn ingest_time_weight(&mut self, item: &<Self as TimeBinnableTypeAggregator>::Input) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let val = item.values[i1];
            if ts < self.int_ts {
                self.last_ts = ts;
                self.last_val = Some(val);
            } else if ts >= self.range.end {
                return;
            } else {
                self.count += 1;
                self.apply_event_time_weight(ts, Some(val));
            }
        }
    }

    fn result_reset_unweight(&mut self, range: NanoRange, _expand: bool) -> MinMaxAvgBins<NTY> {
        let avg = if self.sumc == 0 {
            None
        } else {
            Some(self.sum / self.sumc as f32)
        };
        let ret = MinMaxAvgBins {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![self.min],
            maxs: vec![self.max],
            avgs: vec![avg],
        };
        self.range = range;
        self.count = 0;
        self.min = None;
        self.max = None;
        self.sum = 0f32;
        self.sumc = 0;
        ret
    }

    fn result_reset_time_weight(&mut self, range: NanoRange, expand: bool) -> MinMaxAvgBins<NTY> {
        if expand {
            self.apply_event_time_weight(self.range.end, self.last_val);
        }
        let avg = {
            let sc = self.range.delta() as f32 * 1e-9;
            Some(self.sum / sc)
        };
        let ret = MinMaxAvgBins {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![self.min],
            maxs: vec![self.max],
            avgs: vec![avg],
        };
        self.range = range;
        self.count = 0;
        self.min = None;
        self.max = None;
        self.sum = 0f32;
        self.sumc = 0;
        ret
    }
}

impl<NTY> TimeBinnableTypeAggregator for EventValuesAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = EventValues<NTY>;
    type Output = MinMaxAvgBins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        if self.do_time_weight {
            self.ingest_time_weight(item)
        } else {
            self.ingest_unweight(item)
        }
    }

    fn result_reset(&mut self, range: NanoRange, expand: bool) -> Self::Output {
        if self.do_time_weight {
            self.result_reset_time_weight(range, expand)
        } else {
            self.result_reset_unweight(range, expand)
        }
    }
}

impl<NTY> EventAppendable for EventValues<NTY>
where
    NTY: NumOps,
{
    type Value = NTY;

    fn append_event(&mut self, ts: u64, value: Self::Value) {
        self.tss.push(ts);
        self.values.push(value);
    }
}
