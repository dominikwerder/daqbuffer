use crate::minmaxavgwavebins::MinMaxAvgWaveBins;
use crate::numops::NumOps;
use crate::streams::{Collectable, Collector};
use crate::{
    Appendable, FilterFittingInside, Fits, FitsInside, PushableIndex, RangeOverlapInfo, ReadPbv, ReadableFromFile,
    SitemtyFrameType, SubFrId, TimeBinnableType, TimeBinnableTypeAggregator, WithLen, WithTimestamps,
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
    const FRAME_TYPE_ID: u32 = 0x900 + NTY::SUB;
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
    fn empty() -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.tss.extend_from_slice(&src.tss);
        self.mins.extend_from_slice(&src.mins);
        self.maxs.extend_from_slice(&src.maxs);
        self.avgs.extend_from_slice(&src.avgs);
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

    fn aggregator(range: NanoRange, bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        Self::Aggregator::new(range, bin_count, do_time_weight)
    }
}

pub struct XBinnedWaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    range: NanoRange,
    count: u64,
    min: Vec<NTY>,
    max: Vec<NTY>,
    sum: Vec<f32>,
    sumc: u64,
}

impl<NTY> XBinnedWaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    pub fn new(range: NanoRange, bin_count: usize, do_time_weight: bool) -> Self {
        if bin_count == 0 {
            panic!("bin_count == 0");
        }
        Self {
            range,
            count: 0,
            min: vec![NTY::max_or_nan(); bin_count],
            max: vec![NTY::min_or_nan(); bin_count],
            sum: vec![0f32; bin_count],
            sumc: 0,
        }
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
        error!("time-weighted binning not available");
        err::todo();
        //info!("XBinnedWaveEventsAggregator  ingest  item {:?}", item);
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            if ts < self.range.beg {
                continue;
            } else if ts >= self.range.end {
                continue;
            } else {
                for (i2, &v) in item.mins[i1].iter().enumerate() {
                    if v < self.min[i2] || self.min[i2].is_nan() {
                        self.min[i2] = v;
                    }
                }
                for (i2, &v) in item.maxs[i1].iter().enumerate() {
                    if v > self.max[i2] || self.max[i2].is_nan() {
                        self.max[i2] = v;
                    }
                }
                for (i2, &v) in item.avgs[i1].iter().enumerate() {
                    if v.is_nan() {
                    } else {
                        self.sum[i2] += v;
                    }
                }
                self.sumc += 1;
                self.count += 1;
            }
        }
    }

    fn result(self) -> Self::Output {
        if self.sumc == 0 {
            Self::Output {
                ts1s: vec![self.range.beg],
                ts2s: vec![self.range.end],
                counts: vec![self.count],
                mins: vec![None],
                maxs: vec![None],
                avgs: vec![None],
            }
        } else {
            let avg = self.sum.iter().map(|k| *k / self.sumc as f32).collect();
            let ret = Self::Output {
                ts1s: vec![self.range.beg],
                ts2s: vec![self.range.end],
                counts: vec![self.count],
                mins: vec![Some(self.min)],
                maxs: vec![Some(self.max)],
                avgs: vec![Some(avg)],
            };
            if ret.ts1s[0] < 1300 {
                info!("XBinnedWaveEventsAggregator  result  {:?}", ret);
            }
            ret
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
