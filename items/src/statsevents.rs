use crate::streams::{Collectable, Collector};
use crate::{
    ts_offs_from_abs, Appendable, ByteEstimate, Clearable, EventAppendable, FilterFittingInside, Fits, FitsInside,
    FrameTypeStatic, NewEmpty, PushableIndex, RangeOverlapInfo, ReadPbv, ReadableFromFile, SitemtyFrameType,
    TimeBinnableType, TimeBinnableTypeAggregator, WithLen, WithTimestamps,
};
use err::Error;
use netpod::log::*;
use netpod::{NanoRange, Shape};
use serde::{Deserialize, Serialize};
use std::fmt;
use tokio::fs::File;

#[derive(Serialize, Deserialize)]
pub struct StatsEvents {
    pub tss: Vec<u64>,
    pub pulses: Vec<u64>,
}

impl FrameTypeStatic for StatsEvents {
    const FRAME_TYPE_ID: u32 = crate::STATS_EVENTS_FRAME_TYPE_ID;

    fn from_error(_: err::Error) -> Self {
        // TODO remove usage of this
        panic!()
    }
}

impl SitemtyFrameType for StatsEvents {
    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeStatic>::FRAME_TYPE_ID
    }
}

impl StatsEvents {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            pulses: vec![],
        }
    }
}

impl fmt::Debug for StatsEvents {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "count {}  tss {:?} .. {:?}  pulses {:?} .. {:?}",
            self.tss.len(),
            self.tss.first(),
            self.tss.last(),
            self.pulses.first(),
            self.pulses.last(),
        )
    }
}

impl WithLen for StatsEvents {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl WithTimestamps for StatsEvents {
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

impl ByteEstimate for StatsEvents {
    fn byte_estimate(&self) -> u64 {
        if self.tss.len() == 0 {
            0
        } else {
            // TODO improve via a const fn on NTY
            self.tss.len() as u64 * 16
        }
    }
}

impl RangeOverlapInfo for StatsEvents {
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

impl FitsInside for StatsEvents {
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

impl FilterFittingInside for StatsEvents {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl PushableIndex for StatsEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push(src.tss[ix]);
        self.pulses.push(src.pulses[ix]);
    }
}

impl NewEmpty for StatsEvents {
    fn empty(_shape: Shape) -> Self {
        Self {
            tss: Vec::new(),
            pulses: Vec::new(),
        }
    }
}

impl Appendable for StatsEvents {
    fn empty_like_self(&self) -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.tss.extend_from_slice(&src.tss);
        self.pulses.extend_from_slice(&src.pulses);
    }

    fn append_zero(&mut self, ts1: u64, _ts2: u64) {
        self.tss.push(ts1);
        self.pulses.push(0);
    }
}

impl Clearable for StatsEvents {
    fn clear(&mut self) {
        self.tss.clear();
        self.pulses.clear();
    }
}

impl ReadableFromFile for StatsEvents {
    fn read_from_file(_file: File) -> Result<ReadPbv<Self>, Error> {
        // TODO refactor types such that this can be removed.
        panic!()
    }

    fn from_buf(_buf: &[u8]) -> Result<Self, Error> {
        panic!()
    }
}

impl TimeBinnableType for StatsEvents {
    type Output = StatsEvents;
    type Aggregator = StatsEventsAggregator;

    fn aggregator(range: NanoRange, _x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        Self::Aggregator::new(range, do_time_weight)
    }
}

pub struct StatsEventsCollector {
    vals: StatsEvents,
    range_complete: bool,
    timed_out: bool,
}

impl StatsEventsCollector {
    pub fn new() -> Self {
        Self {
            vals: StatsEvents::empty(),
            range_complete: false,
            timed_out: false,
        }
    }
}

impl WithLen for StatsEventsCollector {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

#[derive(Serialize)]
pub struct StatsEventsCollectorOutput {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: Vec<u64>,
    // TODO what to collect? pulse min/max
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "finalisedRange")]
    range_complete: bool,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "timedOut")]
    timed_out: bool,
}

impl Collector for StatsEventsCollector {
    type Input = StatsEvents;
    type Output = StatsEventsCollectorOutput;

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
            range_complete: self.range_complete,
            timed_out: self.timed_out,
        };
        Ok(ret)
    }
}

impl Collectable for StatsEvents {
    type Collector = StatsEventsCollector;

    fn new_collector(_bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new()
    }
}

pub struct StatsEventsAggregator {
    range: NanoRange,
    count: u64,
    sumc: u64,
    sum: f32,
    int_ts: u64,
    last_ts: u64,
    do_time_weight: bool,
}

impl StatsEventsAggregator {
    pub fn new(range: NanoRange, do_time_weight: bool) -> Self {
        let int_ts = range.beg;
        Self {
            range,
            count: 0,
            sum: 0f32,
            sumc: 0,
            int_ts,
            last_ts: 0,
            do_time_weight,
        }
    }

    fn apply_min_max(&mut self, _val: f32) {
        // TODO currently no values to min/max
    }

    fn apply_event_unweight(&mut self, val: f32) {
        self.apply_min_max(val);
        let vf = val;
        if vf.is_nan() {
        } else {
            self.sum += vf;
            self.sumc += 1;
        }
    }

    fn apply_event_time_weight(&mut self, _ts: u64) {
        // TODO currently no value to weight.
    }

    fn ingest_unweight(&mut self, item: &<Self as TimeBinnableTypeAggregator>::Input) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let val = 0.0;
            if ts < self.range.beg {
            } else if ts >= self.range.end {
            } else {
                self.apply_event_unweight(val);
                self.count += 1;
            }
        }
    }

    fn ingest_time_weight(&mut self, item: &<Self as TimeBinnableTypeAggregator>::Input) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            //let val = 0.0;
            if ts < self.int_ts {
                self.last_ts = ts;
                //self.last_val = Some(val);
            } else if ts >= self.range.end {
                return;
            } else {
                self.apply_event_time_weight(ts);
                self.count += 1;
                self.last_ts = ts;
                //self.last_val = Some(val);
            }
        }
    }

    fn reset(&mut self, range: NanoRange) {
        self.int_ts = range.beg;
        self.range = range;
        self.count = 0;
        //self.min = None;
        //self.max = None;
        self.sum = 0f32;
        self.sumc = 0;
    }

    fn result_reset_unweight(
        &mut self,
        range: NanoRange,
        _expand: bool,
    ) -> <Self as TimeBinnableTypeAggregator>::Output {
        let _avg = if self.sumc == 0 {
            None
        } else {
            Some(self.sum / self.sumc as f32)
        };
        // TODO return some meaningful value
        let ret = StatsEvents::empty();
        self.reset(range);
        ret
    }

    fn result_reset_time_weight(
        &mut self,
        range: NanoRange,
        expand: bool,
    ) -> <Self as TimeBinnableTypeAggregator>::Output {
        // TODO check callsite for correct expand status.
        if true || expand {
            debug!("result_reset_time_weight calls apply_event_time_weight");
            self.apply_event_time_weight(self.range.end);
        } else {
            debug!("result_reset_time_weight NO EXPAND");
        }
        let _avg = {
            let sc = self.range.delta() as f32 * 1e-9;
            Some(self.sum / sc)
        };
        // TODO return some meaningful value
        let ret = StatsEvents::empty();
        self.reset(range);
        ret
    }
}

impl TimeBinnableTypeAggregator for StatsEventsAggregator {
    type Input = StatsEvents;
    type Output = StatsEvents;

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

impl EventAppendable for StatsEvents {
    type Value = f32;

    fn append_event(ret: Option<Self>, _ts: u64, _pulse: u64, _value: Self::Value) -> Self {
        let ret = if let Some(ret) = ret { ret } else { Self::empty() };
        // TODO
        error!("TODO statsevents append_event");
        err::todo();
        ret
    }
}
