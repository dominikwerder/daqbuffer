use crate::minmaxavgbins::MinMaxAvgBins;
use crate::numops::NumOps;
use crate::streams::{Collectable, Collector};
use crate::{
    ts_offs_from_abs, Appendable, ByteEstimate, Clearable, FilterFittingInside, Fits, FitsInside, PushableIndex,
    RangeOverlapInfo, ReadPbv, ReadableFromFile, SitemtyFrameType, SubFrId, TimeBinnableType,
    TimeBinnableTypeAggregator, WithLen, WithTimestamps,
};
use err::Error;
use netpod::timeunits::SEC;
use netpod::NanoRange;
use serde::{Deserialize, Serialize};
use tokio::fs::File;

// TODO rename Scalar -> Dim0
#[derive(Debug, Serialize, Deserialize)]
pub struct XBinnedScalarEvents<NTY> {
    pub tss: Vec<u64>,
    pub mins: Vec<NTY>,
    pub maxs: Vec<NTY>,
    pub avgs: Vec<f32>,
}

impl<NTY> SitemtyFrameType for XBinnedScalarEvents<NTY>
where
    NTY: SubFrId,
{
    const FRAME_TYPE_ID: u32 = crate::X_BINNED_SCALAR_EVENTS_FRAME_TYPE_ID + NTY::SUB;
}

impl<NTY> XBinnedScalarEvents<NTY> {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
        }
    }
}

impl<NTY> WithLen for XBinnedScalarEvents<NTY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<NTY> WithTimestamps for XBinnedScalarEvents<NTY> {
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

impl<NTY> ByteEstimate for XBinnedScalarEvents<NTY> {
    fn byte_estimate(&self) -> u64 {
        if self.tss.len() == 0 {
            0
        } else {
            // TODO improve via a const fn on NTY
            self.tss.len() as u64 * 28
        }
    }
}

impl<NTY> RangeOverlapInfo for XBinnedScalarEvents<NTY> {
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

impl<NTY> FitsInside for XBinnedScalarEvents<NTY> {
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

impl<NTY> FilterFittingInside for XBinnedScalarEvents<NTY> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> PushableIndex for XBinnedScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push(src.tss[ix]);
        self.mins.push(src.mins[ix]);
        self.maxs.push(src.maxs[ix]);
        self.avgs.push(src.avgs[ix]);
    }
}

impl<NTY> Appendable for XBinnedScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn empty_like_self(&self) -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.tss.extend_from_slice(&src.tss);
        self.mins.extend_from_slice(&src.mins);
        self.maxs.extend_from_slice(&src.maxs);
        self.avgs.extend_from_slice(&src.avgs);
    }
}

impl<NTY> Clearable for XBinnedScalarEvents<NTY> {
    fn clear(&mut self) {
        self.tss.clear();
        self.avgs.clear();
        self.mins.clear();
        self.maxs.clear();
    }
}

impl<NTY> ReadableFromFile for XBinnedScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn read_from_file(_file: File) -> Result<ReadPbv<Self>, Error> {
        // TODO refactor types such that this impl is not needed.
        panic!()
    }

    fn from_buf(_buf: &[u8]) -> Result<Self, Error> {
        panic!()
    }
}

impl<NTY> TimeBinnableType for XBinnedScalarEvents<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgBins<NTY>;
    type Aggregator = XBinnedScalarEventsAggregator<NTY>;

    fn aggregator(range: NanoRange, _x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        // TODO remove output
        if range.delta() > SEC * 0 {
            netpod::log::info!(
                "TimeBinnableType for XBinnedScalarEvents  aggregator()  range {:?}",
                range
            );
        }
        Self::Aggregator::new(range, do_time_weight)
    }
}

pub struct XBinnedScalarEventsAggregator<NTY>
where
    NTY: NumOps,
{
    range: NanoRange,
    count: u64,
    min: Option<NTY>,
    max: Option<NTY>,
    sumc: u64,
    sum: f32,
    int_ts: u64,
    last_ts: u64,
    last_avg: Option<f32>,
    last_min: Option<NTY>,
    last_max: Option<NTY>,
    do_time_weight: bool,
}

impl<NTY> XBinnedScalarEventsAggregator<NTY>
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
            sumc: 0,
            sum: 0f32,
            int_ts,
            last_ts: 0,
            last_avg: None,
            last_min: None,
            last_max: None,
            do_time_weight,
        }
    }

    fn apply_min_max(&mut self, min: NTY, max: NTY) {
        self.min = match self.min {
            None => Some(min),
            Some(cmin) => {
                if min < cmin {
                    Some(min)
                } else {
                    Some(cmin)
                }
            }
        };
        self.max = match self.max {
            None => Some(max),
            Some(cmax) => {
                if max > cmax {
                    Some(max)
                } else {
                    Some(cmax)
                }
            }
        };
    }

    fn apply_event_unweight(&mut self, avg: f32, min: NTY, max: NTY) {
        self.apply_min_max(min, max);
        let vf = avg;
        if vf.is_nan() {
        } else {
            self.sum += vf;
            self.sumc += 1;
        }
    }

    fn apply_event_time_weight(&mut self, ts: u64, avg: Option<f32>, min: Option<NTY>, max: Option<NTY>) {
        if let Some(v) = self.last_avg {
            self.apply_min_max(min.unwrap(), max.unwrap());
            let w = if self.do_time_weight {
                (ts - self.int_ts) as f32 * 1e-9
            } else {
                1.
            };
            let vf = v;
            if vf.is_nan() {
            } else {
                self.sum += vf * w;
                self.sumc += 1;
            }
            self.int_ts = ts;
        }
        self.last_ts = ts;
        self.last_avg = avg;
        self.last_min = min;
        self.last_max = max;
    }

    fn ingest_unweight(&mut self, item: &XBinnedScalarEvents<NTY>) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let avg = item.avgs[i1];
            let min = item.mins[i1];
            let max = item.maxs[i1];
            if ts < self.range.beg {
            } else if ts >= self.range.end {
            } else {
                self.count += 1;
                self.apply_event_unweight(avg, min, max);
            }
        }
    }

    fn ingest_time_weight(&mut self, item: &XBinnedScalarEvents<NTY>) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let avg = item.avgs[i1];
            let min = item.mins[i1];
            let max = item.maxs[i1];
            if ts < self.int_ts {
                self.last_ts = ts;
                self.last_avg = Some(avg);
                self.last_min = Some(min);
                self.last_max = Some(max);
            } else if ts >= self.range.end {
                return;
            } else {
                self.count += 1;
                self.apply_event_time_weight(ts, Some(avg), Some(min), Some(max));
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
            self.apply_event_time_weight(self.range.end, self.last_avg, self.last_min, self.last_max);
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

impl<NTY> TimeBinnableTypeAggregator for XBinnedScalarEventsAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = XBinnedScalarEvents<NTY>;
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

#[derive(Serialize, Deserialize)]
pub struct XBinnedScalarEventsCollectedResult<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: Vec<u64>,
    mins: Vec<NTY>,
    maxs: Vec<NTY>,
    avgs: Vec<f32>,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "finalisedRange")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "timedOut")]
    timed_out: bool,
}

pub struct XBinnedScalarEventsCollector<NTY> {
    vals: XBinnedScalarEvents<NTY>,
    finalised_range: bool,
    timed_out: bool,
    #[allow(dead_code)]
    bin_count_exp: u32,
}

impl<NTY> XBinnedScalarEventsCollector<NTY> {
    pub fn new(bin_count_exp: u32) -> Self {
        Self {
            finalised_range: false,
            timed_out: false,
            vals: XBinnedScalarEvents::empty(),
            bin_count_exp,
        }
    }
}

impl<NTY> WithLen for XBinnedScalarEventsCollector<NTY> {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

impl<NTY> Collector for XBinnedScalarEventsCollector<NTY>
where
    NTY: NumOps,
{
    type Input = XBinnedScalarEvents<NTY>;
    type Output = XBinnedScalarEventsCollectedResult<NTY>;

    fn ingest(&mut self, src: &Self::Input) {
        self.vals.append(src);
    }

    fn set_range_complete(&mut self) {
        self.finalised_range = true;
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
            mins: self.vals.mins,
            maxs: self.vals.maxs,
            avgs: self.vals.avgs,
            finalised_range: self.finalised_range,
            timed_out: self.timed_out,
        };
        Ok(ret)
    }
}

impl<NTY> Collectable for XBinnedScalarEvents<NTY>
where
    NTY: NumOps,
{
    type Collector = XBinnedScalarEventsCollector<NTY>;

    fn new_collector(bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new(bin_count_exp)
    }
}
