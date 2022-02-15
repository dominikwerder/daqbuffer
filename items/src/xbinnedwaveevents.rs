use std::mem;

use crate::minmaxavgwavebins::MinMaxAvgWaveBins;
use crate::numops::NumOps;
use crate::streams::{Collectable, Collector};
use crate::{
    Appendable, ByteEstimate, Clearable, FilterFittingInside, Fits, FitsInside, PushableIndex, RangeOverlapInfo,
    ReadPbv, ReadableFromFile, SitemtyFrameType, SubFrId, TimeBinnableType, TimeBinnableTypeAggregator, WithLen,
    WithTimestamps,
};
use err::Error;
use netpod::log::*;
use netpod::timeunits::{MS, SEC};
use netpod::NanoRange;
use serde::{Deserialize, Serialize};
use tokio::fs::File;

// TODO  rename Wave -> Dim1
#[derive(Debug, Serialize, Deserialize)]
pub struct XBinnedWaveEvents<NTY> {
    pub tss: Vec<u64>,
    pub mins: Vec<Vec<NTY>>,
    pub maxs: Vec<Vec<NTY>>,
    pub avgs: Vec<Vec<f32>>,
}

impl<NTY> SitemtyFrameType for XBinnedWaveEvents<NTY>
where
    NTY: SubFrId,
{
    const FRAME_TYPE_ID: u32 = crate::X_BINNED_WAVE_EVENTS_FRAME_TYPE_ID + NTY::SUB;
}

impl<NTY> XBinnedWaveEvents<NTY> {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
        }
    }
}

impl<NTY> WithLen for XBinnedWaveEvents<NTY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<NTY> WithTimestamps for XBinnedWaveEvents<NTY> {
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

impl<NTY> ByteEstimate for XBinnedWaveEvents<NTY> {
    fn byte_estimate(&self) -> u64 {
        if self.tss.len() == 0 {
            0
        } else {
            // TODO improve via a const fn on NTY
            self.tss.len() as u64 * 20 * self.avgs[0].len() as u64
        }
    }
}

impl<NTY> RangeOverlapInfo for XBinnedWaveEvents<NTY> {
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

impl<NTY> FitsInside for XBinnedWaveEvents<NTY> {
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

impl<NTY> FilterFittingInside for XBinnedWaveEvents<NTY> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> PushableIndex for XBinnedWaveEvents<NTY>
where
    NTY: NumOps,
{
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push(src.tss[ix]);
        // TODO not nice.
        self.mins.push(src.mins[ix].clone());
        self.maxs.push(src.maxs[ix].clone());
        self.avgs.push(src.avgs[ix].clone());
    }
}

impl<NTY> Appendable for XBinnedWaveEvents<NTY>
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

impl<NTY> Clearable for XBinnedWaveEvents<NTY> {
    fn clear(&mut self) {
        self.tss.clear();
        self.mins.clear();
        self.maxs.clear();
        self.avgs.clear();
    }
}

impl<NTY> ReadableFromFile for XBinnedWaveEvents<NTY>
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

impl<NTY> TimeBinnableType for XBinnedWaveEvents<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgWaveBins<NTY>;
    type Aggregator = XBinnedWaveEventsAggregator<NTY>;

    fn aggregator(range: NanoRange, x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        debug!(
            "TimeBinnableType for XBinnedWaveEvents  aggregator()  range {:?}  x_bin_count {}  do_time_weight {}",
            range, x_bin_count, do_time_weight
        );
        Self::Aggregator::new(range, x_bin_count, do_time_weight)
    }
}

pub struct XBinnedWaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    range: NanoRange,
    count: u64,
    min: Option<Vec<NTY>>,
    max: Option<Vec<NTY>>,
    sumc: u64,
    sum: Vec<f32>,
    int_ts: u64,
    last_ts: u64,
    last_avg: Option<Vec<f32>>,
    last_min: Option<Vec<NTY>>,
    last_max: Option<Vec<NTY>>,
    do_time_weight: bool,
}

impl<NTY> XBinnedWaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    pub fn new(range: NanoRange, x_bin_count: usize, do_time_weight: bool) -> Self {
        Self {
            int_ts: range.beg,
            range,
            count: 0,
            min: None,
            max: None,
            sumc: 0,
            sum: vec![0f32; x_bin_count],
            last_ts: 0,
            last_avg: None,
            last_min: None,
            last_max: None,
            do_time_weight,
        }
    }

    // TODO get rid of clones.
    fn apply_min_max(&mut self, min: &Vec<NTY>, max: &Vec<NTY>) {
        self.min = match self.min.take() {
            None => Some(min.clone()),
            Some(cmin) => {
                let a = cmin
                    .into_iter()
                    .zip(min)
                    .map(|(a, b)| if a < *b { a } else { b.clone() })
                    .collect();
                Some(a)
            }
        };
        self.max = match self.max.take() {
            None => Some(max.clone()),
            Some(cmax) => {
                let a = cmax
                    .into_iter()
                    .zip(min)
                    .map(|(a, b)| if a > *b { a } else { b.clone() })
                    .collect();
                Some(a)
            }
        };
    }

    fn apply_event_unweight(&mut self, avg: &Vec<f32>, min: &Vec<NTY>, max: &Vec<NTY>) {
        //debug!("apply_event_unweight");
        self.apply_min_max(&min, &max);
        let sum = mem::replace(&mut self.sum, vec![]);
        self.sum = sum
            .into_iter()
            .zip(avg)
            .map(|(a, &b)| if b.is_nan() { a } else { a + b })
            .collect();
        self.sumc += 1;
    }

    fn apply_event_time_weight(&mut self, ts: u64) {
        //debug!("apply_event_time_weight");
        if let (Some(avg), Some(min), Some(max)) = (self.last_avg.take(), self.last_min.take(), self.last_max.take()) {
            self.apply_min_max(&min, &max);
            let w = (ts - self.int_ts) as f32 / self.range.delta() as f32;
            let sum = mem::replace(&mut self.sum, vec![]);
            self.sum = sum
                .into_iter()
                .zip(&avg)
                .map(|(a, &b)| if b.is_nan() { a } else { a + b * w })
                .collect();
            self.sumc += 1;
            self.int_ts = ts;
            self.last_avg = Some(avg);
            self.last_min = Some(min);
            self.last_max = Some(max);
        }
    }

    fn ingest_unweight(&mut self, item: &XBinnedWaveEvents<NTY>) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let avg = &item.avgs[i1];
            let min = &item.mins[i1];
            let max = &item.maxs[i1];
            if ts < self.range.beg {
            } else if ts >= self.range.end {
            } else {
                self.apply_event_unweight(avg, min, max);
                self.count += 1;
            }
        }
    }

    fn ingest_time_weight(&mut self, item: &XBinnedWaveEvents<NTY>) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let avg = &item.avgs[i1];
            let min = &item.mins[i1];
            let max = &item.maxs[i1];
            if ts < self.int_ts {
                self.last_ts = ts;
                self.last_avg = Some(avg.clone());
                self.last_min = Some(min.clone());
                self.last_max = Some(max.clone());
            } else if ts >= self.range.end {
                return;
            } else {
                self.apply_event_time_weight(ts);
                self.count += 1;
                self.last_ts = ts;
                self.last_avg = Some(avg.clone());
                self.last_min = Some(min.clone());
                self.last_max = Some(max.clone());
            }
        }
    }

    fn result_reset_unweight(&mut self, range: NanoRange, _expand: bool) -> MinMaxAvgWaveBins<NTY> {
        let avg = if self.sumc == 0 {
            None
        } else {
            Some(self.sum.iter().map(|k| *k / self.sumc as f32).collect())
        };
        let min = mem::replace(&mut self.min, None);
        let max = mem::replace(&mut self.max, None);
        let ret = MinMaxAvgWaveBins {
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
        self.min = None;
        self.max = None;
        self.sumc = 0;
        self.sum = vec![0f32; ret.avgs.len()];
        ret
    }

    fn result_reset_time_weight(&mut self, range: NanoRange, expand: bool) -> MinMaxAvgWaveBins<NTY> {
        // TODO check callsite for correct expand status.
        if true || expand {
            self.apply_event_time_weight(self.range.end);
        }
        let avg = if self.sumc == 0 {
            None
        } else {
            let n = self.sum.len();
            Some(mem::replace(&mut self.sum, vec![0f32; n]))
        };
        let min = mem::replace(&mut self.min, None);
        let max = mem::replace(&mut self.max, None);
        let ret = MinMaxAvgWaveBins {
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
        //self.min = None;
        //self.max = None;
        //self.sum = vec![0f32; ret.avgs.len()];
        self.sumc = 0;
        ret
    }
}

impl<NTY> TimeBinnableTypeAggregator for XBinnedWaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = XBinnedWaveEvents<NTY>;
    type Output = MinMaxAvgWaveBins<NTY>;

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
pub struct XBinnedWaveEventsCollectedResult<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: Vec<u64>,
    mins: Vec<Vec<NTY>>,
    maxs: Vec<Vec<NTY>>,
    avgs: Vec<Vec<f32>>,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "finalisedRange")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "timedOut")]
    timed_out: bool,
}

pub struct XBinnedWaveEventsCollector<NTY> {
    vals: XBinnedWaveEvents<NTY>,
    finalised_range: bool,
    timed_out: bool,
    #[allow(dead_code)]
    bin_count_exp: u32,
}

impl<NTY> XBinnedWaveEventsCollector<NTY> {
    pub fn new(bin_count_exp: u32) -> Self {
        Self {
            finalised_range: false,
            timed_out: false,
            vals: XBinnedWaveEvents::empty(),
            bin_count_exp,
        }
    }
}

impl<NTY> WithLen for XBinnedWaveEventsCollector<NTY> {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

impl<NTY> Collector for XBinnedWaveEventsCollector<NTY>
where
    NTY: NumOps,
{
    type Input = XBinnedWaveEvents<NTY>;
    type Output = XBinnedWaveEventsCollectedResult<NTY>;

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
        let ts_anchor_sec = self.vals.tss.first().map_or(0, |&k| k) / SEC;
        let ts_anchor_ns = ts_anchor_sec * SEC;
        let ts_off_ms: Vec<_> = self.vals.tss.iter().map(|&k| (k - ts_anchor_ns) / MS).collect();
        let ts_off_ns = self
            .vals
            .tss
            .iter()
            .zip(ts_off_ms.iter().map(|&k| k * MS))
            .map(|(&j, k)| (j - ts_anchor_ns - k))
            .collect();
        let ret = Self::Output {
            finalised_range: self.finalised_range,
            timed_out: self.timed_out,
            ts_anchor_sec,
            ts_off_ms,
            ts_off_ns,
            mins: self.vals.mins,
            maxs: self.vals.maxs,
            avgs: self.vals.avgs,
        };
        Ok(ret)
    }
}

impl<NTY> Collectable for XBinnedWaveEvents<NTY>
where
    NTY: NumOps,
{
    type Collector = XBinnedWaveEventsCollector<NTY>;

    fn new_collector(bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new(bin_count_exp)
    }
}
