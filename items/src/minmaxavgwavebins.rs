use crate::numops::NumOps;
use crate::streams::{Collectable, Collector, ToJsonBytes, ToJsonResult};
use crate::{
    ts_offs_from_abs, Appendable, FilterFittingInside, Fits, FitsInside, IsoDateTime, RangeOverlapInfo, ReadPbv,
    ReadableFromFile, Sitemty, SitemtyFrameType, SubFrId, TimeBinnableType, TimeBinnableTypeAggregator, TimeBins,
    WithLen,
};
use chrono::{TimeZone, Utc};
use err::Error;
use netpod::timeunits::SEC;
use netpod::NanoRange;
use num_traits::Zero;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::marker::PhantomData;
use tokio::fs::File;

#[derive(Serialize, Deserialize)]
pub struct MinMaxAvgWaveBins<NTY> {
    pub ts1s: Vec<u64>,
    pub ts2s: Vec<u64>,
    pub counts: Vec<u64>,
    pub mins: Vec<Option<Vec<NTY>>>,
    pub maxs: Vec<Option<Vec<NTY>>>,
    pub avgs: Vec<Option<Vec<f32>>>,
}

impl<NTY> SitemtyFrameType for MinMaxAvgWaveBins<NTY>
where
    NTY: SubFrId,
{
    const FRAME_TYPE_ID: u32 = crate::MIN_MAX_AVG_WAVE_BINS + NTY::SUB;
}

impl<NTY> fmt::Debug for MinMaxAvgWaveBins<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "MinMaxAvgWaveBins  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  mins {:?}  maxs {:?}  avgs {:?}",
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

impl<NTY> MinMaxAvgWaveBins<NTY> {
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

impl<NTY> FitsInside for MinMaxAvgWaveBins<NTY> {
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

impl<NTY> FilterFittingInside for MinMaxAvgWaveBins<NTY> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> RangeOverlapInfo for MinMaxAvgWaveBins<NTY> {
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

impl<NTY> TimeBins for MinMaxAvgWaveBins<NTY>
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

impl<NTY> WithLen for MinMaxAvgWaveBins<NTY> {
    fn len(&self) -> usize {
        self.ts1s.len()
    }
}

impl<NTY> Appendable for MinMaxAvgWaveBins<NTY>
where
    NTY: NumOps,
{
    fn empty() -> Self {
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
}

impl<NTY> ReadableFromFile for MinMaxAvgWaveBins<NTY>
where
    NTY: NumOps,
{
    // TODO this function is not needed in the trait:
    fn read_from_file(file: File) -> Result<ReadPbv<Self>, Error> {
        Ok(ReadPbv::new(file))
    }

    fn from_buf(buf: &[u8]) -> Result<Self, Error> {
        let dec = serde_cbor::from_slice(&buf)?;
        Ok(dec)
    }
}

impl<NTY> TimeBinnableType for MinMaxAvgWaveBins<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgWaveBins<NTY>;
    type Aggregator = MinMaxAvgWaveBinsAggregator<NTY>;

    fn aggregator(range: NanoRange, x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        Self::Aggregator::new(range, x_bin_count, do_time_weight)
    }
}

impl<NTY> ToJsonResult for Sitemty<MinMaxAvgWaveBins<NTY>>
where
    NTY: NumOps,
{
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        Ok(Box::new(serde_json::Value::String(format!(
            "MinMaxAvgBins/non-json-item"
        ))))
    }
}

pub struct MinMaxAvgWaveBinsCollected<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> MinMaxAvgWaveBinsCollected<NTY> {
    pub fn new() -> Self {
        Self { _m1: PhantomData }
    }
}

#[derive(Serialize)]
pub struct MinMaxAvgWaveBinsCollectedResult<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: Vec<u64>,
    counts: Vec<u64>,
    mins: Vec<Option<Vec<NTY>>>,
    maxs: Vec<Option<Vec<NTY>>>,
    avgs: Vec<Option<Vec<f32>>>,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "finalisedRange")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "Zero::is_zero", rename = "missingBins")]
    missing_bins: u32,
    #[serde(skip_serializing_if = "Option::is_none", rename = "continueAt")]
    continue_at: Option<IsoDateTime>,
}

pub struct MinMaxAvgWaveBinsCollector<NTY> {
    bin_count_exp: u32,
    timed_out: bool,
    range_complete: bool,
    vals: MinMaxAvgWaveBins<NTY>,
    _m1: PhantomData<NTY>,
}

impl<NTY> MinMaxAvgWaveBinsCollector<NTY> {
    pub fn new(bin_count_exp: u32) -> Self {
        Self {
            bin_count_exp,
            timed_out: false,
            range_complete: false,
            vals: MinMaxAvgWaveBins::<NTY>::empty(),
            _m1: PhantomData,
        }
    }
}

impl<NTY> WithLen for MinMaxAvgWaveBinsCollector<NTY>
where
    NTY: NumOps + Serialize,
{
    fn len(&self) -> usize {
        self.vals.ts1s.len()
    }
}

impl<NTY> Collector for MinMaxAvgWaveBinsCollector<NTY>
where
    NTY: NumOps + Serialize,
{
    type Input = MinMaxAvgWaveBins<NTY>;
    type Output = MinMaxAvgWaveBinsCollectedResult<NTY>;

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
        let t_bin_count = self.vals.counts.len();
        // TODO could save the copy:
        let mut ts_all = self.vals.ts1s.clone();
        if self.vals.ts2s.len() > 0 {
            ts_all.push(*self.vals.ts2s.last().unwrap());
        }
        let continue_at = if self.vals.ts1s.len() < self.bin_count_exp as usize {
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
        let ret = MinMaxAvgWaveBinsCollectedResult {
            ts_anchor_sec: tst.0,
            ts_off_ms: tst.1,
            ts_off_ns: tst.2,
            counts: self.vals.counts,
            mins: self.vals.mins,
            maxs: self.vals.maxs,
            avgs: self.vals.avgs,
            finalised_range: self.range_complete,
            missing_bins: self.bin_count_exp - t_bin_count as u32,
            continue_at,
        };
        Ok(ret)
    }
}

impl<NTY> Collectable for MinMaxAvgWaveBins<NTY>
where
    NTY: NumOps + Serialize,
{
    type Collector = MinMaxAvgWaveBinsCollector<NTY>;

    fn new_collector(bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new(bin_count_exp)
    }
}

pub struct MinMaxAvgWaveBinsAggregator<NTY> {
    range: NanoRange,
    count: u64,
    min: Vec<NTY>,
    max: Vec<NTY>,
    sum: Vec<f32>,
    sumc: u64,
}

impl<NTY> MinMaxAvgWaveBinsAggregator<NTY>
where
    NTY: NumOps,
{
    pub fn new(range: NanoRange, x_bin_count: usize, do_time_weight: bool) -> Self {
        if do_time_weight {
            err::todo();
        }
        Self {
            range,
            count: 0,
            min: vec![NTY::max_or_nan(); x_bin_count],
            max: vec![NTY::min_or_nan(); x_bin_count],
            sum: vec![0f32; x_bin_count],
            sumc: 0,
        }
    }
}

impl<NTY> TimeBinnableTypeAggregator for MinMaxAvgWaveBinsAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = MinMaxAvgWaveBins<NTY>;
    type Output = MinMaxAvgWaveBins<NTY>;

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
                // the input can contain bins where no events did fall into.
                match &item.mins[i1] {
                    None => {}
                    Some(inp) => {
                        for (a, b) in self.min.iter_mut().zip(inp.iter()) {
                            if *b < *a || a.is_nan() {
                                *a = *b;
                            }
                        }
                    }
                }
                match &item.maxs[i1] {
                    None => {}
                    Some(inp) => {
                        for (a, b) in self.max.iter_mut().zip(inp.iter()) {
                            if *b > *a || a.is_nan() {
                                *a = *b;
                            }
                        }
                    }
                }
                match &item.avgs[i1] {
                    None => {}
                    Some(inp) => {
                        for (a, b) in self.sum.iter_mut().zip(inp.iter()) {
                            *a += *b;
                        }
                    }
                }
                self.sumc += 1;
                self.count += item.counts[i1];
            }
        }
    }

    fn result_reset(&mut self, range: NanoRange, _expand: bool) -> Self::Output {
        let ret;
        if self.sumc == 0 {
            ret = Self::Output {
                ts1s: vec![self.range.beg],
                ts2s: vec![self.range.end],
                counts: vec![self.count],
                mins: vec![None],
                maxs: vec![None],
                avgs: vec![None],
            };
        } else {
            let avg = self.sum.iter().map(|j| *j / self.sumc as f32).collect();
            ret = Self::Output {
                ts1s: vec![self.range.beg],
                ts2s: vec![self.range.end],
                counts: vec![self.count],
                // TODO replace with reset-value instead:
                mins: vec![Some(self.min.clone())],
                maxs: vec![Some(self.max.clone())],
                avgs: vec![Some(avg)],
            };
        }
        self.range = range;
        self.count = 0;
        self.min = vec![NTY::max_or_nan(); self.min.len()];
        self.max = vec![NTY::min_or_nan(); self.min.len()];
        self.sum = vec![0f32; self.min.len()];
        self.sumc = 0;
        ret
    }
}
