use crate::frame::bincode_from_slice;
use crate::numops::NumOps;
use crate::streams::{Collectable, Collector, ToJsonBytes, ToJsonResult};
use crate::ts_offs_from_abs;
use crate::waveevents::WaveEvents;
use crate::Appendable;
use crate::FilterFittingInside;
use crate::FrameTypeInnerStatic;
use crate::IsoDateTime;
use crate::RangeOverlapInfo;
use crate::ReadableFromFile;
use crate::TimeBinnableDyn;
use crate::TimeBinnableType;
use crate::TimeBinnableTypeAggregator;
use crate::TimeBins;
use crate::{pulse_offs_from_abs, FrameType};
use crate::{Fits, FitsInside, NewEmpty, ReadPbv, Sitemty, TimeBinned, WithLen};
use chrono::{TimeZone, Utc};
use err::Error;
use items_0::subfr::SubFrId;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{NanoRange, Shape};
use num_traits::Zero;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::marker::PhantomData;
use tokio::fs::File;

#[derive(Serialize, Deserialize)]
pub struct MinMaxAvgDim1Bins<NTY> {
    pub ts1s: Vec<u64>,
    pub ts2s: Vec<u64>,
    pub counts: Vec<u64>,
    pub mins: Vec<Option<Vec<NTY>>>,
    pub maxs: Vec<Option<Vec<NTY>>>,
    pub avgs: Vec<Option<Vec<f32>>>,
}

impl<NTY> FrameTypeInnerStatic for MinMaxAvgDim1Bins<NTY>
where
    NTY: SubFrId,
{
    const FRAME_TYPE_ID: u32 = crate::MIN_MAX_AVG_DIM_1_BINS_FRAME_TYPE_ID + NTY::SUB;
}

impl<NTY> FrameType for MinMaxAvgDim1Bins<NTY>
where
    NTY: SubFrId,
{
    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeInnerStatic>::FRAME_TYPE_ID
    }
}

impl<NTY> fmt::Debug for MinMaxAvgDim1Bins<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "MinMaxAvgDim1Bins  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  mins {:?}  maxs {:?}  avgs {:?}",
            self.ts1s.len(),
            self.ts1s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.ts2s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.counts,
            self.mins.first(),
            self.maxs.first(),
            self.avgs.first(),
        )
    }
}

impl<NTY> MinMaxAvgDim1Bins<NTY> {
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

impl<NTY> FitsInside for MinMaxAvgDim1Bins<NTY> {
    fn fits_inside(&self, range: NanoRange) -> Fits {
        if self.ts1s.is_empty() {
            Fits::Empty
        } else {
            let t1 = *self.ts1s.first().unwrap();
            let t2 = *self.ts2s.last().unwrap();
            if t2 <= range.beg {
                Fits::Lower
            } else if t1 >= range.end {
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

impl<NTY> FilterFittingInside for MinMaxAvgDim1Bins<NTY> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> RangeOverlapInfo for MinMaxAvgDim1Bins<NTY> {
    fn ends_before(&self, range: NanoRange) -> bool {
        match self.ts2s.last() {
            Some(&ts) => ts <= range.beg,
            None => true,
        }
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        match self.ts2s.last() {
            Some(&ts) => ts > range.end,
            None => panic!(),
        }
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        match self.ts1s.first() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }
}

impl<NTY> TimeBins for MinMaxAvgDim1Bins<NTY>
where
    NTY: NumOps,
{
    fn ts1s(&self) -> &Vec<u64> {
        &self.ts1s
    }

    fn ts2s(&self) -> &Vec<u64> {
        &self.ts2s
    }
}

impl<NTY> WithLen for MinMaxAvgDim1Bins<NTY> {
    fn len(&self) -> usize {
        self.ts1s.len()
    }
}

impl<NTY> NewEmpty for MinMaxAvgDim1Bins<NTY> {
    fn empty(_shape: Shape) -> Self {
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

impl<NTY> Appendable for MinMaxAvgDim1Bins<NTY>
where
    NTY: NumOps,
{
    fn empty_like_self(&self) -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.ts1s.extend_from_slice(&src.ts1s);
        self.ts2s.extend_from_slice(&src.ts2s);
        self.counts.extend_from_slice(&src.counts);
        self.mins.extend_from_slice(&src.mins);
        self.maxs.extend_from_slice(&src.maxs);
        self.avgs.extend_from_slice(&src.avgs);
    }

    fn append_zero(&mut self, ts1: u64, ts2: u64) {
        self.ts1s.push(ts1);
        self.ts2s.push(ts2);
        self.counts.push(0);
        self.avgs.push(None);
        self.mins.push(None);
        self.maxs.push(None);
    }
}

impl<NTY> ReadableFromFile for MinMaxAvgDim1Bins<NTY>
where
    NTY: NumOps,
{
    // TODO this function is not needed in the trait:
    fn read_from_file(file: File) -> Result<ReadPbv<Self>, Error> {
        Ok(ReadPbv::new(file))
    }

    fn from_buf(buf: &[u8]) -> Result<Self, Error> {
        let dec = bincode_from_slice(buf)?;
        Ok(dec)
    }
}

impl<NTY> TimeBinnableType for MinMaxAvgDim1Bins<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgDim1Bins<NTY>;
    type Aggregator = MinMaxAvgDim1BinsAggregator<NTY>;

    fn aggregator(range: NanoRange, x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        debug!(
            "TimeBinnableType for MinMaxAvgDim1Bins  aggregator()  range {:?}  x_bin_count {}  do_time_weight {}",
            range, x_bin_count, do_time_weight
        );
        Self::Aggregator::new(range, x_bin_count, do_time_weight)
    }
}

impl<NTY> ToJsonResult for Sitemty<MinMaxAvgDim1Bins<NTY>>
where
    NTY: NumOps,
{
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        Ok(Box::new(serde_json::Value::String(format!(
            "MinMaxAvgDim1Bins/non-json-item"
        ))))
    }
}

pub struct MinMaxAvgDim1BinsCollected<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> MinMaxAvgDim1BinsCollected<NTY> {
    pub fn new() -> Self {
        Self { _m1: PhantomData }
    }
}

#[derive(Serialize)]
pub struct MinMaxAvgDim1BinsCollectedResult<NTY> {
    ts_bin_edges: Vec<IsoDateTime>,
    counts: Vec<u64>,
    mins: Vec<Option<Vec<NTY>>>,
    maxs: Vec<Option<Vec<NTY>>>,
    avgs: Vec<Option<Vec<f32>>>,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "rangeFinal")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "Zero::is_zero", rename = "missingBins")]
    missing_bins: u32,
    #[serde(skip_serializing_if = "Option::is_none", rename = "continueAt")]
    continue_at: Option<IsoDateTime>,
}

pub struct MinMaxAvgDim1BinsCollector<NTY> {
    bin_count_exp: u32,
    timed_out: bool,
    range_complete: bool,
    vals: MinMaxAvgDim1Bins<NTY>,
    _m1: PhantomData<NTY>,
}

impl<NTY> MinMaxAvgDim1BinsCollector<NTY> {
    pub fn new(bin_count_exp: u32) -> Self {
        Self {
            bin_count_exp,
            timed_out: false,
            range_complete: false,
            vals: MinMaxAvgDim1Bins::<NTY>::empty(),
            _m1: PhantomData,
        }
    }
}

impl<NTY> WithLen for MinMaxAvgDim1BinsCollector<NTY>
where
    NTY: NumOps + Serialize,
{
    fn len(&self) -> usize {
        self.vals.ts1s.len()
    }
}

impl<NTY> Collector for MinMaxAvgDim1BinsCollector<NTY>
where
    NTY: NumOps + Serialize,
{
    type Input = MinMaxAvgDim1Bins<NTY>;
    type Output = MinMaxAvgDim1BinsCollectedResult<NTY>;

    fn ingest(&mut self, src: &Self::Input) {
        Appendable::append(&mut self.vals, src);
    }

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(self) -> Result<Self::Output, Error> {
        let bin_count = self.vals.ts1s.len() as u32;
        let mut tsa: Vec<_> = self
            .vals
            .ts1s
            .iter()
            .map(|&k| IsoDateTime(Utc.timestamp_nanos(k as i64)))
            .collect();
        if let Some(&z) = self.vals.ts2s.last() {
            tsa.push(IsoDateTime(Utc.timestamp_nanos(z as i64)));
        }
        let tsa = tsa;
        let continue_at = if self.vals.ts1s.len() < self.bin_count_exp as usize {
            match tsa.last() {
                Some(k) => Some(k.clone()),
                None => Err(Error::with_msg("partial_content but no bin in result"))?,
            }
        } else {
            None
        };
        let ret = MinMaxAvgDim1BinsCollectedResult::<NTY> {
            ts_bin_edges: tsa,
            counts: self.vals.counts,
            mins: self.vals.mins,
            maxs: self.vals.maxs,
            avgs: self.vals.avgs,
            finalised_range: self.range_complete,
            missing_bins: self.bin_count_exp - bin_count,
            continue_at,
        };
        Ok(ret)
    }
}

impl<NTY> Collectable for MinMaxAvgDim1Bins<NTY>
where
    NTY: NumOps + Serialize,
{
    type Collector = MinMaxAvgDim1BinsCollector<NTY>;

    fn new_collector(bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new(bin_count_exp)
    }
}

pub struct MinMaxAvgDim1BinsAggregator<NTY> {
    range: NanoRange,
    count: u64,
    min: Option<Vec<NTY>>,
    max: Option<Vec<NTY>>,
    sumc: u64,
    sum: Option<Vec<f32>>,
}

impl<NTY> MinMaxAvgDim1BinsAggregator<NTY> {
    pub fn new(range: NanoRange, _x_bin_count: usize, do_time_weight: bool) -> Self {
        if do_time_weight {
            err::todo();
        }
        Self {
            range,
            count: 0,
            // TODO get rid of Option
            min: err::todoval(),
            max: None,
            sumc: 0,
            sum: None,
        }
    }
}

impl<NTY> TimeBinnableTypeAggregator for MinMaxAvgDim1BinsAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = MinMaxAvgDim1Bins<NTY>;
    type Output = MinMaxAvgDim1Bins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        for i1 in 0..item.ts1s.len() {
            if item.ts2s[i1] <= self.range.beg {
                continue;
            } else if item.ts1s[i1] >= self.range.end {
                continue;
            } else {
                match self.min.as_mut() {
                    None => self.min = item.mins[i1].clone(),
                    Some(min) => match item.mins[i1].as_ref() {
                        None => {}
                        Some(v) => {
                            for (a, b) in min.iter_mut().zip(v.iter()) {
                                if b < a {
                                    *a = b.clone();
                                }
                            }
                        }
                    },
                };
                match self.max.as_mut() {
                    None => self.max = item.maxs[i1].clone(),
                    Some(max) => match item.maxs[i1].as_ref() {
                        None => {}
                        Some(v) => {
                            for (a, b) in max.iter_mut().zip(v.iter()) {
                                if b > a {
                                    *a = b.clone();
                                }
                            }
                        }
                    },
                };
                match self.sum.as_mut() {
                    None => {
                        self.sum = item.avgs[i1].clone();
                    }
                    Some(sum) => match item.avgs[i1].as_ref() {
                        None => {}
                        Some(v) => {
                            for (a, b) in sum.iter_mut().zip(v.iter()) {
                                if (*b).is_nan() {
                                } else {
                                    *a += *b;
                                }
                            }
                            self.sumc += 1;
                        }
                    },
                }
                self.count += item.counts[i1];
            }
        }
    }

    fn result_reset(&mut self, range: NanoRange, _expand: bool) -> Self::Output {
        let avg = if self.sumc == 0 {
            None
        } else {
            let avg = self
                .sum
                .as_ref()
                .unwrap()
                .iter()
                .map(|k| k / self.sumc as f32)
                .collect();
            Some(avg)
        };
        let ret = Self::Output {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            // TODO replace with reset-value instead:
            mins: vec![self.min.clone()],
            maxs: vec![self.max.clone()],
            avgs: vec![avg],
        };
        self.range = range;
        self.count = 0;
        self.min = None;
        self.max = None;
        self.sum = None;
        self.sumc = 0;
        ret
    }
}

#[derive(Serialize)]
pub struct WaveEventsCollectedResult<NTY> {
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
    values: Vec<Vec<NTY>>,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "rangeFinal")]
    range_complete: bool,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "timedOut")]
    timed_out: bool,
}

pub struct WaveEventsCollector<NTY> {
    vals: WaveEvents<NTY>,
    range_complete: bool,
    timed_out: bool,
}

impl<NTY> WaveEventsCollector<NTY> {
    pub fn new(_bin_count_exp: u32) -> Self {
        info!("\n\nWaveEventsCollector\n\n");
        Self {
            vals: WaveEvents::empty(),
            range_complete: false,
            timed_out: false,
        }
    }
}

impl<NTY> WithLen for WaveEventsCollector<NTY> {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

impl<NTY> Collector for WaveEventsCollector<NTY>
where
    NTY: NumOps,
{
    type Input = WaveEvents<NTY>;
    type Output = WaveEventsCollectedResult<NTY>;

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
            values: self.vals.vals,
            range_complete: self.range_complete,
            timed_out: self.timed_out,
        };
        Ok(ret)
    }
}

impl<NTY> Collectable for WaveEvents<NTY>
where
    NTY: NumOps,
{
    type Collector = WaveEventsCollector<NTY>;

    fn new_collector(bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new(bin_count_exp)
    }
}

impl<NTY: NumOps> crate::TimeBinnableDynStub for MinMaxAvgDim1Bins<NTY> {}

impl<NTY: NumOps> TimeBinned for MinMaxAvgDim1Bins<NTY> {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnableDyn {
        self as &dyn TimeBinnableDyn
    }

    fn edges_slice(&self) -> (&[u64], &[u64]) {
        (&self.ts1s[..], &self.ts2s[..])
    }

    fn counts(&self) -> &[u64] {
        &self.counts[..]
    }

    fn avgs(&self) -> Vec<f32> {
        err::todoval()
    }

    fn mins(&self) -> Vec<f32> {
        err::todoval()
    }

    fn maxs(&self) -> Vec<f32> {
        err::todoval()
    }

    fn validate(&self) -> Result<(), String> {
        err::todoval()
    }
}
