use crate::minmaxavgdim1bins::MinMaxAvgDim1Bins;
use crate::numops::NumOps;
use crate::xbinnedscalarevents::XBinnedScalarEvents;
use crate::xbinnedwaveevents::XBinnedWaveEvents;
use crate::{
    Appendable, ByteEstimate, Clearable, EventAppendable, EventsNodeProcessor, FilterFittingInside, Fits, FitsInside,
    PushableIndex, RangeOverlapInfo, ReadPbv, ReadableFromFile, SitemtyFrameType, SubFrId, TimeBinnableType,
    TimeBinnableTypeAggregator, WithLen, WithTimestamps,
};
use err::Error;
use netpod::log::*;
use netpod::{x_bin_count, AggKind, NanoRange, Shape};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use tokio::fs::File;

#[derive(Debug, Serialize, Deserialize)]
pub struct WaveEvents<NTY> {
    pub tss: Vec<u64>,
    pub vals: Vec<Vec<NTY>>,
}

impl<NTY> WaveEvents<NTY> {
    pub fn shape(&self) -> Result<Shape, Error> {
        if let Some(k) = self.vals.first() {
            let ret = Shape::Wave(k.len() as u32);
            Ok(ret)
        } else {
            Err(Error::with_msg_no_trace("WaveEvents is empty, can not determine Shape"))
        }
    }
}

impl<NTY> SitemtyFrameType for WaveEvents<NTY>
where
    NTY: SubFrId,
{
    const FRAME_TYPE_ID: u32 = crate::WAVE_EVENTS_FRAME_TYPE_ID + NTY::SUB;
}

impl<NTY> WaveEvents<NTY> {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            vals: vec![],
        }
    }
}

impl<NTY> WithLen for WaveEvents<NTY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<NTY> WithTimestamps for WaveEvents<NTY> {
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

impl<NTY> ByteEstimate for WaveEvents<NTY> {
    fn byte_estimate(&self) -> u64 {
        if self.tss.len() == 0 {
            0
        } else {
            // TODO improve via a const fn on NTY
            self.tss.len() as u64 * 8 * self.vals[0].len() as u64
        }
    }
}

impl<NTY> RangeOverlapInfo for WaveEvents<NTY> {
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

impl<NTY> FitsInside for WaveEvents<NTY> {
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

impl<NTY> FilterFittingInside for WaveEvents<NTY> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> PushableIndex for WaveEvents<NTY>
where
    NTY: NumOps,
{
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push(src.tss[ix]);
        // TODO trait should allow to move from source.
        self.vals.push(src.vals[ix].clone());
    }
}

impl<NTY> Appendable for WaveEvents<NTY>
where
    NTY: NumOps,
{
    fn empty_like_self(&self) -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.tss.extend_from_slice(&src.tss);
        self.vals.extend_from_slice(&src.vals);
    }
}

impl<NTY> Clearable for WaveEvents<NTY> {
    fn clear(&mut self) {
        self.tss.clear();
        self.vals.clear();
    }
}

impl<NTY> ReadableFromFile for WaveEvents<NTY>
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

impl<NTY> TimeBinnableType for WaveEvents<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgDim1Bins<NTY>;
    type Aggregator = WaveEventsAggregator<NTY>;

    fn aggregator(range: NanoRange, bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        Self::Aggregator::new(range, bin_count, do_time_weight)
    }
}

pub struct WaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    range: NanoRange,
    count: u64,
    min: Option<Vec<NTY>>,
    max: Option<Vec<NTY>>,
    sumc: u64,
    sum: Option<Vec<f32>>,
}

impl<NTY> WaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    pub fn new(range: NanoRange, _x_bin_count: usize, do_time_weight: bool) -> Self {
        if do_time_weight {
            err::todo();
        }
        Self {
            range,
            count: 0,
            // TODO create the right number of bins right here:
            min: err::todoval(),
            max: None,
            sumc: 0,
            sum: None,
        }
    }
}

impl<NTY> TimeBinnableTypeAggregator for WaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = WaveEvents<NTY>;
    type Output = MinMaxAvgDim1Bins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        error!("time-weighted binning not available");
        err::todo();
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            if ts < self.range.beg {
                continue;
            } else if ts >= self.range.end {
                continue;
            } else {
                match &mut self.min {
                    None => self.min = Some(item.vals[i1].clone()),
                    Some(min) => {
                        for (a, b) in min.iter_mut().zip(item.vals[i1].iter()) {
                            if b < a {
                                *a = *b;
                            }
                        }
                    }
                };
                match &mut self.max {
                    None => self.max = Some(item.vals[i1].clone()),
                    Some(max) => {
                        for (a, b) in max.iter_mut().zip(item.vals[i1].iter()) {
                            if b < a {
                                *a = *b;
                            }
                        }
                    }
                };
                match self.sum.as_mut() {
                    None => {
                        self.sum = Some(item.vals[i1].iter().map(|k| k.as_()).collect());
                    }
                    Some(sum) => {
                        for (a, b) in sum.iter_mut().zip(item.vals[i1].iter()) {
                            let vf = b.as_();
                            if vf.is_nan() {
                            } else {
                                *a += vf;
                            }
                        }
                    }
                }
                self.sumc += 1;
                self.count += 1;
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
                .map(|item| item / self.sumc as f32)
                .collect();
            Some(avg)
        };
        let ret = Self::Output {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            // TODO replace with reset-value instead.
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

impl<NTY> EventAppendable for WaveEvents<NTY>
where
    NTY: NumOps,
{
    type Value = Vec<NTY>;

    fn append_event(ret: Option<Self>, ts: u64, value: Self::Value) -> Self {
        let mut ret = if let Some(ret) = ret { ret } else { Self::empty() };
        ret.tss.push(ts);
        ret.vals.push(value);
        ret
    }
}

pub struct WaveXBinner<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for WaveXBinner<NTY>
where
    NTY: NumOps,
{
    type Input = WaveEvents<NTY>;
    type Output = XBinnedScalarEvents<NTY>;

    fn create(_shape: Shape, _agg_kind: AggKind) -> Self {
        Self { _m1: PhantomData }
    }

    fn process(&self, inp: Self::Input) -> Self::Output {
        let nev = inp.tss.len();
        let mut ret = Self::Output {
            tss: inp.tss,
            mins: Vec::with_capacity(nev),
            maxs: Vec::with_capacity(nev),
            avgs: Vec::with_capacity(nev),
        };
        for i1 in 0..nev {
            let mut min = NTY::max_or_nan();
            let mut max = NTY::min_or_nan();
            let mut sum = 0f32;
            let mut sumc = 0;
            let vals = &inp.vals[i1];
            for &v in vals {
                if v < min || min.is_nan() {
                    min = v;
                }
                if v > max || max.is_nan() {
                    max = v;
                }
                let vf = v.as_();
                if vf.is_nan() {
                } else {
                    sum += vf;
                    sumc += 1;
                }
            }
            ret.mins.push(min);
            ret.maxs.push(max);
            if sumc == 0 {
                ret.avgs.push(f32::NAN);
            } else {
                ret.avgs.push(sum / sumc as f32);
            }
        }
        ret
    }
}

pub struct WaveNBinner<NTY> {
    shape_bin_count: usize,
    x_bin_count: usize,
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for WaveNBinner<NTY>
where
    NTY: NumOps,
{
    type Input = WaveEvents<NTY>;
    type Output = XBinnedWaveEvents<NTY>;

    fn create(shape: Shape, agg_kind: AggKind) -> Self {
        info!("WaveNBinner::create");
        // TODO get rid of panic potential
        let shape_bin_count = if let Shape::Wave(n) = shape { n } else { panic!() } as usize;
        let x_bin_count = x_bin_count(&shape, &agg_kind);
        info!("shape_bin_count {}  x_bin_count {}", shape_bin_count, x_bin_count);
        Self {
            shape_bin_count,
            x_bin_count,
            _m1: PhantomData,
        }
    }

    fn process(&self, inp: Self::Input) -> Self::Output {
        let nev = inp.tss.len();
        let mut ret = Self::Output {
            // TODO get rid of this clone:
            tss: inp.tss.clone(),
            mins: Vec::with_capacity(nev),
            maxs: Vec::with_capacity(nev),
            avgs: Vec::with_capacity(nev),
        };
        for i1 in 0..nev {
            let mut min = vec![NTY::max_or_nan(); self.x_bin_count];
            let mut max = vec![NTY::min_or_nan(); self.x_bin_count];
            let mut sum = vec![0f32; self.x_bin_count];
            let mut sumc = vec![0u64; self.x_bin_count];
            for (i2, &v) in inp.vals[i1].iter().enumerate() {
                let i3 = i2 * self.x_bin_count / self.shape_bin_count;
                if v < min[i3] || min[i3].is_nan() {
                    min[i3] = v;
                }
                if v > max[i3] || max[i3].is_nan() {
                    max[i3] = v;
                }
                if v.is_nan() {
                } else {
                    sum[i3] += v.as_();
                    sumc[i3] += 1;
                }
            }
            // TODO
            if false && inp.tss[0] < 1300 {
                info!("WaveNBinner  process  push min  {:?}", min);
            }
            ret.mins.push(min);
            ret.maxs.push(max);
            let avg = sum
                .into_iter()
                .zip(sumc.into_iter())
                .map(|(j, k)| if k > 0 { j / k as f32 } else { f32::NAN })
                .collect();
            ret.avgs.push(avg);
        }
        ret
    }
}

pub struct WavePlainProc<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for WavePlainProc<NTY>
where
    NTY: NumOps,
{
    type Input = WaveEvents<NTY>;
    type Output = WaveEvents<NTY>;

    fn create(_shape: Shape, _agg_kind: AggKind) -> Self {
        Self { _m1: PhantomData }
    }

    fn process(&self, inp: Self::Input) -> Self::Output {
        if false {
            let n = if inp.vals.len() > 0 { inp.vals[0].len() } else { 0 };
            let n = if n > 5 { 5 } else { n };
            WaveEvents {
                tss: inp.tss,
                vals: inp.vals.iter().map(|k| k[..n].to_vec()).collect(),
            }
        } else {
            WaveEvents {
                tss: inp.tss,
                vals: inp.vals,
            }
        }
    }
}
