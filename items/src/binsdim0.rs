use crate::numops::NumOps;
use crate::streams::{Collectable, Collector, ToJsonBytes, ToJsonResult};
use crate::{
    ts_offs_from_abs, Appendable, FilterFittingInside, Fits, FitsInside, FrameTypeStatic, IsoDateTime, NewEmpty,
    RangeOverlapInfo, ReadPbv, ReadableFromFile, Sitemty, SitemtyFrameType, SubFrId, TimeBinnableDyn, TimeBinnableType,
    TimeBinnableTypeAggregator, TimeBinned, TimeBinnerDyn, TimeBins, WithLen,
};
use chrono::{TimeZone, Utc};
use err::Error;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{NanoRange, Shape};
use num_traits::Zero;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::marker::PhantomData;
use tokio::fs::File;

#[derive(Clone, Serialize, Deserialize)]
pub struct MinMaxAvgDim0Bins<NTY> {
    pub ts1s: Vec<u64>,
    pub ts2s: Vec<u64>,
    pub counts: Vec<u64>,
    pub mins: Vec<NTY>,
    pub maxs: Vec<NTY>,
    pub avgs: Vec<f32>,
}

impl<NTY> FrameTypeStatic for MinMaxAvgDim0Bins<NTY>
where
    NTY: SubFrId,
{
    const FRAME_TYPE_ID: u32 = crate::MIN_MAX_AVG_DIM_0_BINS_FRAME_TYPE_ID + NTY::SUB;

    fn from_error(_: err::Error) -> Self {
        // TODO remove usage of this
        panic!()
    }
}

impl<NTY> SitemtyFrameType for MinMaxAvgDim0Bins<NTY>
where
    NTY: SubFrId,
{
    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeStatic>::FRAME_TYPE_ID
    }
}

impl<NTY> fmt::Debug for MinMaxAvgDim0Bins<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "MinMaxAvgDim0Bins  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  mins {:?}  maxs {:?}  avgs {:?}",
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

impl<NTY> MinMaxAvgDim0Bins<NTY> {
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

impl<NTY> FitsInside for MinMaxAvgDim0Bins<NTY> {
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

impl<NTY> FilterFittingInside for MinMaxAvgDim0Bins<NTY> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> RangeOverlapInfo for MinMaxAvgDim0Bins<NTY> {
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

impl<NTY> TimeBins for MinMaxAvgDim0Bins<NTY>
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

impl<NTY> WithLen for MinMaxAvgDim0Bins<NTY> {
    fn len(&self) -> usize {
        self.ts1s.len()
    }
}

impl<NTY> NewEmpty for MinMaxAvgDim0Bins<NTY> {
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

impl<NTY> Appendable for MinMaxAvgDim0Bins<NTY>
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
        self.mins.push(NTY::zero());
        self.maxs.push(NTY::zero());
        self.avgs.push(0.);
    }
}

impl<NTY> ReadableFromFile for MinMaxAvgDim0Bins<NTY>
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

impl<NTY> TimeBinnableType for MinMaxAvgDim0Bins<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgDim0Bins<NTY>;
    type Aggregator = MinMaxAvgDim0BinsAggregator<NTY>;

    fn aggregator(range: NanoRange, x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        debug!(
            "TimeBinnableType for XBinnedScalarEvents  aggregator()  range {:?}  x_bin_count {}  do_time_weight {}",
            range, x_bin_count, do_time_weight
        );
        Self::Aggregator::new(range, do_time_weight)
    }
}

impl<NTY> ToJsonResult for Sitemty<MinMaxAvgDim0Bins<NTY>>
where
    NTY: NumOps,
{
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        Ok(Box::new(serde_json::Value::String(format!(
            "MinMaxAvgBins/non-json-item"
        ))))
    }
}

pub struct MinMaxAvgBinsCollected<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> MinMaxAvgBinsCollected<NTY> {
    pub fn new() -> Self {
        Self { _m1: PhantomData }
    }
}

#[derive(Serialize)]
pub struct MinMaxAvgBinsCollectedResult<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: Vec<u64>,
    counts: Vec<u64>,
    mins: Vec<NTY>,
    maxs: Vec<NTY>,
    avgs: Vec<f32>,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "finalisedRange")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "Zero::is_zero", rename = "missingBins")]
    missing_bins: u32,
    #[serde(skip_serializing_if = "Option::is_none", rename = "continueAt")]
    continue_at: Option<IsoDateTime>,
}

pub struct MinMaxAvgBinsCollector<NTY> {
    bin_count_exp: u32,
    timed_out: bool,
    range_complete: bool,
    vals: MinMaxAvgDim0Bins<NTY>,
    _m1: PhantomData<NTY>,
}

impl<NTY> MinMaxAvgBinsCollector<NTY> {
    pub fn new(bin_count_exp: u32) -> Self {
        Self {
            bin_count_exp,
            timed_out: false,
            range_complete: false,
            vals: MinMaxAvgDim0Bins::<NTY>::empty(),
            _m1: PhantomData,
        }
    }
}

impl<NTY> WithLen for MinMaxAvgBinsCollector<NTY>
where
    NTY: NumOps + Serialize,
{
    fn len(&self) -> usize {
        self.vals.ts1s.len()
    }
}

impl<NTY> Collector for MinMaxAvgBinsCollector<NTY>
where
    NTY: NumOps + Serialize,
{
    type Input = MinMaxAvgDim0Bins<NTY>;
    type Output = MinMaxAvgBinsCollectedResult<NTY>;

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
        let ret = MinMaxAvgBinsCollectedResult::<NTY> {
            ts_anchor_sec: tst.0,
            ts_off_ms: tst.1,
            ts_off_ns: tst.2,
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

impl<NTY> Collectable for MinMaxAvgDim0Bins<NTY>
where
    NTY: NumOps + Serialize,
{
    type Collector = MinMaxAvgBinsCollector<NTY>;

    fn new_collector(bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new(bin_count_exp)
    }
}

pub struct MinMaxAvgDim0BinsAggregator<NTY> {
    range: NanoRange,
    count: u64,
    min: NTY,
    max: NTY,
    sumc: u64,
    sum: f32,
}

impl<NTY: NumOps> MinMaxAvgDim0BinsAggregator<NTY> {
    pub fn new(range: NanoRange, _do_time_weight: bool) -> Self {
        Self {
            range,
            count: 0,
            min: NTY::zero(),
            max: NTY::zero(),
            sumc: 0,
            sum: 0f32,
        }
    }
}

impl<NTY> TimeBinnableTypeAggregator for MinMaxAvgDim0BinsAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = MinMaxAvgDim0Bins<NTY>;
    type Output = MinMaxAvgDim0Bins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        for i1 in 0..item.ts1s.len() {
            if item.counts[i1] == 0 {
            } else if item.ts2s[i1] <= self.range.beg {
            } else if item.ts1s[i1] >= self.range.end {
            } else {
                if self.count == 0 {
                    self.min = item.mins[i1].clone();
                    self.max = item.maxs[i1].clone();
                } else {
                    if item.mins[i1] < self.min {
                        self.min = item.mins[i1].clone();
                    }
                    if item.maxs[i1] > self.max {
                        self.max = item.maxs[i1].clone();
                    }
                }
                self.count += item.counts[i1];
                self.sum += item.avgs[i1];
                self.sumc += 1;
            }
        }
    }

    fn result_reset(&mut self, range: NanoRange, _expand: bool) -> Self::Output {
        let avg = if self.sumc == 0 {
            0f32
        } else {
            self.sum / self.sumc as f32
        };
        let ret = Self::Output {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![self.min.clone()],
            maxs: vec![self.max.clone()],
            avgs: vec![avg],
        };
        self.count = 0;
        self.min = NTY::zero();
        self.max = NTY::zero();
        self.range = range;
        self.sum = 0f32;
        self.sumc = 0;
        ret
    }
}

impl<NTY: NumOps + 'static> TimeBinnableDyn for MinMaxAvgDim0Bins<NTY> {
    fn time_binner_new(&self, edges: Vec<u64>, do_time_weight: bool) -> Box<dyn TimeBinnerDyn> {
        eprintln!("MinMaxAvgDim0Bins time_binner_new");
        info!("MinMaxAvgDim0Bins time_binner_new");
        let ret = MinMaxAvgDim0BinsTimeBinner::<NTY>::new(edges.into(), do_time_weight);
        Box::new(ret)
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

pub struct MinMaxAvgDim0BinsTimeBinner<NTY: NumOps> {
    edges: VecDeque<u64>,
    do_time_weight: bool,
    range: NanoRange,
    agg: Option<MinMaxAvgDim0BinsAggregator<NTY>>,
    ready: Option<<MinMaxAvgDim0BinsAggregator<NTY> as TimeBinnableTypeAggregator>::Output>,
}

impl<NTY: NumOps> MinMaxAvgDim0BinsTimeBinner<NTY> {
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
        eprintln!("cycle");
        // TODO where to take expand from? Is it still required after all?
        let expand = true;
        let have_next_bin = self.edges.len() >= 3;
        let range_next = if have_next_bin {
            NanoRange {
                beg: self.edges[1],
                end: self.edges[2],
            }
        } else {
            // Using a dummy for this case.
            NanoRange { beg: 1, end: 2 }
        };
        if let Some(agg) = self.agg.as_mut() {
            eprintln!("cycle: use existing agg: {:?}", agg.range);
            let mut h = agg.result_reset(range_next.clone(), expand);
            match self.ready.as_mut() {
                Some(fin) => {
                    fin.append(&mut h);
                }
                None => {
                    self.ready = Some(h);
                }
            }
        } else if have_next_bin {
            eprintln!("cycle: append a zero bin");
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
        } else {
            eprintln!("cycle: no more next bin");
        }
        self.range = range_next;
        self.edges.pop_front();
        if !have_next_bin {
            self.agg = None;
        }
    }
}

impl<NTY: NumOps + 'static> TimeBinnerDyn for MinMaxAvgDim0BinsTimeBinner<NTY> {
    fn cycle(&mut self) {
        Self::cycle(self)
    }

    fn ingest(&mut self, item: &dyn TimeBinnableDyn) {
        const SELF: &str = "MinMaxAvgDim0BinsTimeBinner";
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
            while item.starts_after(self.range.clone()) {
                self.cycle();
                if self.edges.len() < 2 {
                    warn!("TimeBinnerDyn for {SELF}  no more bin in edges B");
                    return;
                }
            }
            if item.ends_before(self.range.clone()) {
                return;
            } else {
                if self.edges.len() < 2 {
                    warn!("TimeBinnerDyn for {SELF}  edge list exhausted");
                    return;
                } else {
                    if self.agg.is_none() {
                        self.agg = Some(MinMaxAvgDim0BinsAggregator::new(
                            self.range.clone(),
                            self.do_time_weight,
                        ));
                    }
                    let agg = self.agg.as_mut().unwrap();
                    if let Some(item) =
                        item.as_any()
                            .downcast_ref::<<MinMaxAvgDim0BinsAggregator<NTY> as TimeBinnableTypeAggregator>::Input>()
                    {
                        agg.ingest(item);
                    } else {
                        let tyid_item = std::any::Any::type_id(item.as_any());
                        error!("not correct item type  {:?}", tyid_item);
                    };
                    if item.ends_after(self.range.clone()) {
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

impl<NTY: NumOps> TimeBinned for MinMaxAvgDim0Bins<NTY> {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnableDyn {
        self as &dyn TimeBinnableDyn
    }

    fn edges_slice(&self) -> (&[u64], &[u64]) {
        (&self.ts1s[..], &self.ts2s[..])
    }

    fn counts(&self) -> &[u64] {
        &self.counts[..]
    }

    fn mins(&self) -> Vec<f32> {
        self.mins.iter().map(|x| x.clone().as_prim_f32()).collect()
    }

    fn maxs(&self) -> Vec<f32> {
        self.maxs.iter().map(|x| x.clone().as_prim_f32()).collect()
    }

    fn avgs(&self) -> Vec<f32> {
        self.avgs.clone()
    }
}
