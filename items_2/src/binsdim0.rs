use crate::{ts_offs_from_abs, ts_offs_from_abs_with_anchor};
use crate::{IsoDateTime, RangeOverlapInfo};
use crate::{TimeBinnable, TimeBinnableType, TimeBinnableTypeAggregator, TimeBinner};
use chrono::{TimeZone, Utc};
use err::Error;
use items_0::collect_s::{Collectable, CollectableType, CollectorType, ToJsonResult};
use items_0::scalar_ops::ScalarOps;
use items_0::TimeBinned;
use items_0::TimeBins;
use items_0::WithLen;
use items_0::{AppendEmptyBin, AsAnyRef};
use items_0::{AsAnyMut, Empty};
use netpod::timeunits::SEC;
use netpod::NanoRange;
use netpod::{log::*, BinnedRange};
use num_traits::Zero;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::VecDeque;
use std::{fmt, mem};

#[allow(unused)]
macro_rules! trace4 {
    ($($arg:tt)*) => ();
    ($($arg:tt)*) => (eprintln!($($arg)*));
}

// TODO make members private
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct BinsDim0<NTY> {
    pub ts1s: VecDeque<u64>,
    pub ts2s: VecDeque<u64>,
    pub counts: VecDeque<u64>,
    pub mins: VecDeque<NTY>,
    pub maxs: VecDeque<NTY>,
    pub avgs: VecDeque<f32>,
}

impl<NTY> fmt::Debug for BinsDim0<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let self_name = std::any::type_name::<Self>();
        if true {
            write!(
                fmt,
                "{self_name}  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  mins {:?}  maxs {:?}  avgs {:?}",
                self.ts1s.len(),
                self.ts1s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
                self.ts2s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
                self.counts,
                self.mins,
                self.maxs,
                self.avgs,
            )
        } else {
            write!(
                fmt,
                "{self_name}  count {}  edges {:?} .. {:?}  counts {:?} .. {:?}  avgs {:?} .. {:?}",
                self.ts1s.len(),
                self.ts1s.front().map(|k| k / SEC),
                self.ts2s.back().map(|k| k / SEC),
                self.counts.front(),
                self.counts.back(),
                self.avgs.front(),
                self.avgs.back(),
            )
        }
    }
}

impl<NTY: ScalarOps> BinsDim0<NTY> {
    pub fn push(&mut self, ts1: u64, ts2: u64, count: u64, min: NTY, max: NTY, avg: f32) {
        self.ts1s.push_back(ts1);
        self.ts2s.push_back(ts2);
        self.counts.push_back(count);
        self.mins.push_back(min);
        self.maxs.push_back(max);
        self.avgs.push_back(avg);
    }

    pub fn append_zero(&mut self, beg: u64, end: u64) {
        self.ts1s.push_back(beg);
        self.ts2s.push_back(end);
        self.counts.push_back(0);
        self.mins.push_back(NTY::zero_b());
        self.maxs.push_back(NTY::zero_b());
        self.avgs.push_back(0.);
    }

    pub fn append_all_from(&mut self, src: &mut Self) {
        self.ts1s.extend(src.ts1s.drain(..));
        self.ts2s.extend(src.ts2s.drain(..));
        self.counts.extend(src.counts.drain(..));
        self.mins.extend(src.mins.drain(..));
        self.maxs.extend(src.maxs.drain(..));
        self.avgs.extend(src.avgs.drain(..));
    }

    pub fn equal_slack(&self, other: &Self) -> bool {
        for (&a, &b) in self.ts1s.iter().zip(other.ts1s.iter()) {
            if a != b {
                return false;
            }
        }
        for (&a, &b) in self.ts2s.iter().zip(other.ts2s.iter()) {
            if a != b {
                return false;
            }
        }
        for (a, b) in self.mins.iter().zip(other.mins.iter()) {
            if !a.equal_slack(b) {
                return false;
            }
        }
        for (a, b) in self.maxs.iter().zip(other.maxs.iter()) {
            if !a.equal_slack(b) {
                return false;
            }
        }
        for (a, b) in self.avgs.iter().zip(other.avgs.iter()) {
            if !a.equal_slack(b) {
                return false;
            }
        }
        true
    }
}

impl<NTY> AsAnyRef for BinsDim0<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<NTY> AsAnyMut for BinsDim0<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<NTY> Empty for BinsDim0<NTY> {
    fn empty() -> Self {
        Self {
            ts1s: VecDeque::new(),
            ts2s: VecDeque::new(),
            counts: VecDeque::new(),
            mins: VecDeque::new(),
            maxs: VecDeque::new(),
            avgs: VecDeque::new(),
        }
    }
}

impl<NTY> WithLen for BinsDim0<NTY> {
    fn len(&self) -> usize {
        self.ts1s.len()
    }
}

impl<NTY> RangeOverlapInfo for BinsDim0<NTY> {
    fn ends_before(&self, range: NanoRange) -> bool {
        if let Some(&max) = self.ts2s.back() {
            max <= range.beg
        } else {
            true
        }
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        if let Some(&max) = self.ts2s.back() {
            max > range.end
        } else {
            true
        }
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        if let Some(&min) = self.ts1s.front() {
            min >= range.end
        } else {
            true
        }
    }
}

impl<NTY: ScalarOps> AppendEmptyBin for BinsDim0<NTY> {
    fn append_empty_bin(&mut self, ts1: u64, ts2: u64) {
        self.ts1s.push_back(ts1);
        self.ts2s.push_back(ts2);
        self.counts.push_back(0);
        self.mins.push_back(NTY::zero_b());
        self.maxs.push_back(NTY::zero_b());
        self.avgs.push_back(0.);
    }
}

impl<NTY: ScalarOps> TimeBins for BinsDim0<NTY> {
    fn ts_min(&self) -> Option<u64> {
        self.ts1s.front().map(Clone::clone)
    }

    fn ts_max(&self) -> Option<u64> {
        self.ts2s.back().map(Clone::clone)
    }

    fn ts_min_max(&self) -> Option<(u64, u64)> {
        if let (Some(min), Some(max)) = (self.ts1s.front().map(Clone::clone), self.ts2s.back().map(Clone::clone)) {
            Some((min, max))
        } else {
            None
        }
    }
}

impl<NTY: ScalarOps> TimeBinnableType for BinsDim0<NTY> {
    type Output = BinsDim0<NTY>;
    type Aggregator = BinsDim0Aggregator<NTY>;

    fn aggregator(range: NanoRange, x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        let self_name = std::any::type_name::<Self>();
        debug!(
            "TimeBinnableType for {self_name}  aggregator()  range {:?}  x_bin_count {}  do_time_weight {}",
            range, x_bin_count, do_time_weight
        );
        Self::Aggregator::new(range, do_time_weight)
    }
}

// TODO rename to BinsDim0CollectorOutput
#[derive(Debug, Serialize, Deserialize)]
pub struct BinsDim0CollectedResult<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "ts1Ms")]
    ts1_off_ms: VecDeque<u64>,
    #[serde(rename = "ts2Ms")]
    ts2_off_ms: VecDeque<u64>,
    #[serde(rename = "ts1Ns")]
    ts1_off_ns: VecDeque<u64>,
    #[serde(rename = "ts2Ns")]
    ts2_off_ns: VecDeque<u64>,
    #[serde(rename = "counts")]
    counts: VecDeque<u64>,
    #[serde(rename = "mins")]
    mins: VecDeque<NTY>,
    #[serde(rename = "maxs")]
    maxs: VecDeque<NTY>,
    #[serde(rename = "avgs")]
    avgs: VecDeque<f32>,
    #[serde(rename = "rangeFinal", default, skip_serializing_if = "crate::bool_is_false")]
    range_final: bool,
    #[serde(rename = "timedOut", default, skip_serializing_if = "crate::bool_is_false")]
    timed_out: bool,
    #[serde(rename = "missingBins", default, skip_serializing_if = "Zero::is_zero")]
    missing_bins: u32,
    #[serde(rename = "continueAt", default, skip_serializing_if = "Option::is_none")]
    continue_at: Option<IsoDateTime>,
    #[serde(rename = "finishedAt", default, skip_serializing_if = "Option::is_none")]
    finished_at: Option<IsoDateTime>,
}

impl<NTY> AsAnyRef for BinsDim0CollectedResult<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<NTY> AsAnyMut for BinsDim0CollectedResult<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<NTY: ScalarOps> items_0::collect_c::Collected for BinsDim0CollectedResult<NTY> {}

impl<NTY> BinsDim0CollectedResult<NTY> {
    pub fn len(&self) -> usize {
        self.ts1_off_ms.len()
    }

    pub fn ts_anchor_sec(&self) -> u64 {
        self.ts_anchor_sec
    }

    pub fn ts1_off_ms(&self) -> &VecDeque<u64> {
        &self.ts1_off_ms
    }

    pub fn ts2_off_ms(&self) -> &VecDeque<u64> {
        &self.ts2_off_ms
    }

    pub fn counts(&self) -> &VecDeque<u64> {
        &self.counts
    }

    pub fn range_final(&self) -> bool {
        self.range_final
    }

    pub fn missing_bins(&self) -> u32 {
        self.missing_bins
    }

    pub fn continue_at(&self) -> Option<IsoDateTime> {
        self.continue_at.clone()
    }

    pub fn mins(&self) -> &VecDeque<NTY> {
        &self.mins
    }

    pub fn maxs(&self) -> &VecDeque<NTY> {
        &self.maxs
    }

    pub fn avgs(&self) -> &VecDeque<f32> {
        &self.avgs
    }
}

impl<NTY: ScalarOps> ToJsonResult for BinsDim0CollectedResult<NTY> {
    fn to_json_result(&self) -> Result<Box<dyn items_0::collect_s::ToJsonBytes>, Error> {
        let k = serde_json::to_value(self)?;
        Ok(Box::new(k))
    }
}

#[derive(Debug)]
pub struct BinsDim0Collector<NTY> {
    timed_out: bool,
    range_final: bool,
    vals: BinsDim0<NTY>,
}

impl<NTY> BinsDim0Collector<NTY> {
    pub fn new() -> Self {
        Self {
            timed_out: false,
            range_final: false,
            vals: BinsDim0::<NTY>::empty(),
        }
    }
}

impl<NTY> WithLen for BinsDim0Collector<NTY> {
    fn len(&self) -> usize {
        self.vals.ts1s.len()
    }
}

impl<NTY: ScalarOps> CollectorType for BinsDim0Collector<NTY> {
    type Input = BinsDim0<NTY>;
    type Output = BinsDim0CollectedResult<NTY>;

    fn ingest(&mut self, src: &mut Self::Input) {
        trace!("\n\n-----------  BinsDim0Collector ingest\n{:?}\n\n", src);
        // TODO could be optimized by non-contiguous container.
        self.vals.ts1s.append(&mut src.ts1s);
        self.vals.ts2s.append(&mut src.ts2s);
        self.vals.counts.append(&mut src.counts);
        self.vals.mins.append(&mut src.mins);
        self.vals.maxs.append(&mut src.maxs);
        self.vals.avgs.append(&mut src.avgs);
    }

    fn set_range_complete(&mut self) {
        self.range_final = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(&mut self, _range: Option<NanoRange>, binrange: Option<BinnedRange>) -> Result<Self::Output, Error> {
        let bin_count_exp = if let Some(r) = &binrange {
            r.bin_count() as u32
        } else {
            eprintln!("no binrange given");
            0
        };
        let bin_count = self.vals.ts1s.len() as u32;
        eprintln!(
            "--------------  MAKE MISSING BINS  bin_count_exp {}  bin_count {}",
            bin_count_exp, bin_count
        );
        let (missing_bins, continue_at, finished_at) = if bin_count < bin_count_exp {
            match self.vals.ts2s.back() {
                Some(&k) => {
                    let missing_bins = bin_count_exp - bin_count;
                    let continue_at = IsoDateTime(Utc.timestamp_nanos(k as i64));
                    let u = k + (k - self.vals.ts1s.back().unwrap()) * missing_bins as u64;
                    let finished_at = IsoDateTime(Utc.timestamp_nanos(u as i64));
                    (missing_bins, Some(continue_at), Some(finished_at))
                }
                None => {
                    warn!("can not determine continue-at parameters");
                    (0, None, None)
                }
            }
        } else {
            (0, None, None)
        };
        if self.vals.ts1s.as_slices().1.len() != 0 {
            panic!();
        }
        if self.vals.ts2s.as_slices().1.len() != 0 {
            panic!();
        }
        let tst1 = ts_offs_from_abs(self.vals.ts1s.as_slices().0);
        let tst2 = ts_offs_from_abs_with_anchor(tst1.0, self.vals.ts2s.as_slices().0);
        let counts = mem::replace(&mut self.vals.counts, VecDeque::new());
        let mins = mem::replace(&mut self.vals.mins, VecDeque::new());
        let maxs = mem::replace(&mut self.vals.maxs, VecDeque::new());
        let avgs = mem::replace(&mut self.vals.avgs, VecDeque::new());
        let ret = BinsDim0CollectedResult::<NTY> {
            ts_anchor_sec: tst1.0,
            ts1_off_ms: tst1.1,
            ts1_off_ns: tst1.2,
            ts2_off_ms: tst2.0,
            ts2_off_ns: tst2.1,
            counts,
            mins,
            maxs,
            avgs,
            range_final: self.range_final,
            timed_out: self.timed_out,
            missing_bins,
            continue_at,
            finished_at,
        };
        Ok(ret)
    }
}

impl<NTY: ScalarOps> CollectableType for BinsDim0<NTY> {
    type Collector = BinsDim0Collector<NTY>;

    fn new_collector() -> Self::Collector {
        Self::Collector::new()
    }
}

impl<NTY> items_0::collect_c::Collector for BinsDim0Collector<NTY>
where
    NTY: ScalarOps,
{
    fn len(&self) -> usize {
        self.vals.len()
    }

    fn ingest(&mut self, item: &mut dyn items_0::collect_c::Collectable) {
        trace4!("\n\n••••••••••••••••••••••••••••\nINGEST\n{:?}\n\n", item);
        {
            {
                //let tyid = item.type_id();
                //let tyid = item.as_any_mut().type_id();
                //let tyid = format!("{:?}", item.type_id().to_owned());
                trace4!("ty  0: {:40?}", (item.as_any_mut() as &dyn Any).type_id());
            }
            trace4!("ty  1: {:40?}", std::any::TypeId::of::<BinsDim0<NTY>>());
            trace4!("ty  2: {:40?}", std::any::TypeId::of::<BinsDim0<i32>>());
            trace4!("ty  3: {:40?}", std::any::TypeId::of::<Box<BinsDim0<i32>>>());
            trace4!(
                "ty  4: {:?}",
                std::any::TypeId::of::<Box<dyn items_0::collect_c::Collectable>>()
            );
            trace4!(
                "ty  5: {:?}",
                std::any::TypeId::of::<&mut dyn items_0::collect_c::Collectable>()
            );
            trace4!("ty  6: {:?}", std::any::TypeId::of::<Box<dyn items_0::TimeBinned>>());
        }
        if let Some(item) = item.as_any_mut().downcast_mut::<BinsDim0<NTY>>() {
            trace4!("ingest plain");
            CollectorType::ingest(self, item)
        } else if let Some(item) = item.as_any_mut().downcast_mut::<Box<BinsDim0<NTY>>>() {
            trace4!("ingest boxed");
            CollectorType::ingest(self, item)
        } else if let Some(item) = item.as_any_mut().downcast_mut::<Box<dyn items_0::TimeBinned>>() {
            trace4!("ingest boxed dyn TimeBinned");
            if let Some(item) = item.as_any_mut().downcast_mut::<BinsDim0<NTY>>() {
                trace4!("ingest boxed dyn TimeBinned  match");
                CollectorType::ingest(self, item)
            } else {
                warn!("BinsDim0Collector::ingest unexpected inner item");
                trace!("BinsDim0Collector::ingest unexpected inner item {:?}", item);
            }
        } else {
            warn!("BinsDim0Collector::ingest unexpected item");
            trace!("BinsDim0Collector::ingest unexpected item {:?}", item);
        }
    }

    fn set_range_complete(&mut self) {
        CollectorType::set_range_complete(self)
    }

    fn set_timed_out(&mut self) {
        CollectorType::set_timed_out(self)
    }

    fn result(
        &mut self,
        range: Option<NanoRange>,
        binrange: Option<BinnedRange>,
    ) -> Result<Box<dyn items_0::collect_c::Collected>, Error> {
        match CollectorType::result(self, range, binrange) {
            Ok(res) => Ok(Box::new(res)),
            Err(e) => Err(e.into()),
        }
    }
}

pub struct BinsDim0Aggregator<NTY> {
    range: NanoRange,
    count: u64,
    min: NTY,
    max: NTY,
    // Carry over to next bin:
    avg: f32,
    sumc: u64,
    sum: f32,
}

impl<NTY: ScalarOps> BinsDim0Aggregator<NTY> {
    pub fn new(range: NanoRange, _do_time_weight: bool) -> Self {
        Self {
            range,
            count: 0,
            min: NTY::zero_b(),
            max: NTY::zero_b(),
            avg: 0.,
            sumc: 0,
            sum: 0f32,
        }
    }
}

impl<NTY: ScalarOps> TimeBinnableTypeAggregator for BinsDim0Aggregator<NTY> {
    type Input = BinsDim0<NTY>;
    type Output = BinsDim0<NTY>;

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
                    if self.min > item.mins[i1] {
                        self.min = item.mins[i1].clone();
                    }
                    if self.max < item.maxs[i1] {
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
        if self.sumc > 0 {
            self.avg = self.sum / self.sumc as f32;
        }
        let ret = Self::Output {
            ts1s: [self.range.beg].into(),
            ts2s: [self.range.end].into(),
            counts: [self.count].into(),
            mins: [self.min.clone()].into(),
            maxs: [self.max.clone()].into(),
            avgs: [self.avg].into(),
        };
        self.range = range;
        self.count = 0;
        self.sum = 0f32;
        self.sumc = 0;
        ret
    }
}

impl<NTY: ScalarOps> TimeBinnable for BinsDim0<NTY> {
    fn time_binner_new(&self, edges: Vec<u64>, do_time_weight: bool) -> Box<dyn TimeBinner> {
        let ret = BinsDim0TimeBinner::<NTY>::new(edges.into(), do_time_weight);
        Box::new(ret)
    }

    fn to_box_to_json_result(&self) -> Box<dyn ToJsonResult> {
        let k = serde_json::to_value(self).unwrap();
        Box::new(k) as _
    }
}

pub struct BinsDim0TimeBinner<NTY: ScalarOps> {
    edges: VecDeque<u64>,
    do_time_weight: bool,
    agg: Option<BinsDim0Aggregator<NTY>>,
    ready: Option<<BinsDim0Aggregator<NTY> as TimeBinnableTypeAggregator>::Output>,
}

impl<NTY: ScalarOps> BinsDim0TimeBinner<NTY> {
    fn new(edges: VecDeque<u64>, do_time_weight: bool) -> Self {
        Self {
            edges,
            do_time_weight,
            agg: None,
            ready: None,
        }
    }

    fn next_bin_range(&mut self) -> Option<NanoRange> {
        if self.edges.len() >= 2 {
            let ret = NanoRange {
                beg: self.edges[0],
                end: self.edges[1],
            };
            self.edges.pop_front();
            Some(ret)
        } else {
            None
        }
    }
}

impl<NTY: ScalarOps> TimeBinner for BinsDim0TimeBinner<NTY> {
    fn ingest(&mut self, item: &dyn TimeBinnable) {
        let self_name = std::any::type_name::<Self>();
        if item.len() == 0 {
            // Return already here, RangeOverlapInfo would not give much sense.
            return;
        }
        if self.edges.len() < 2 {
            warn!("TimeBinnerDyn for {self_name}  no more bin in edges A\n{:?}\n\n", item);
            return;
        }
        // TODO optimize by remembering at which event array index we have arrived.
        // That needs modified interfaces which can take and yield the start and latest index.
        loop {
            while item.starts_after(NanoRange {
                beg: 0,
                end: self.edges[1],
            }) {
                self.cycle();
                if self.edges.len() < 2 {
                    warn!("TimeBinnerDyn for {self_name}  no more bin in edges B\n{:?}\n\n", item);
                    return;
                }
            }
            if item.ends_before(NanoRange {
                beg: self.edges[0],
                end: u64::MAX,
            }) {
                return;
            } else {
                if self.edges.len() < 2 {
                    warn!("TimeBinnerDyn for {self_name}  edge list exhausted");
                    return;
                } else {
                    let agg = if let Some(agg) = self.agg.as_mut() {
                        agg
                    } else {
                        self.agg = Some(BinsDim0Aggregator::new(
                            // We know here that we have enough edges for another bin.
                            // and `next_bin_range` will pop the first edge.
                            self.next_bin_range().unwrap(),
                            self.do_time_weight,
                        ));
                        self.agg.as_mut().unwrap()
                    };
                    if let Some(item) = item
                        .as_any_ref()
                        // TODO make statically sure that we attempt to cast to the correct type here:
                        .downcast_ref::<<BinsDim0Aggregator<NTY> as TimeBinnableTypeAggregator>::Input>()
                    {
                        agg.ingest(item);
                    } else {
                        let tyid_item = std::any::Any::type_id(item.as_any_ref());
                        error!("not correct item type  {:?}", tyid_item);
                    };
                    if item.ends_after(agg.range().clone()) {
                        self.cycle();
                        if self.edges.len() < 2 {
                            warn!("TimeBinnerDyn for {self_name}  no more bin in edges C\n{:?}\n\n", item);
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

    fn bins_ready(&mut self) -> Option<Box<dyn items_0::TimeBinned>> {
        match self.ready.take() {
            Some(k) => Some(Box::new(k)),
            None => None,
        }
    }

    // TODO there is too much common code between implementors:
    fn push_in_progress(&mut self, push_empty: bool) {
        // TODO expand should be derived from AggKind. Is it still required after all?
        let expand = true;
        if let Some(agg) = self.agg.as_mut() {
            let dummy_range = NanoRange { beg: 4, end: 5 };
            let mut bins = agg.result_reset(dummy_range, expand);
            self.agg = None;
            assert_eq!(bins.len(), 1);
            if push_empty || bins.counts[0] != 0 {
                match self.ready.as_mut() {
                    Some(ready) => {
                        ready.append_all_from(&mut bins);
                    }
                    None => {
                        self.ready = Some(bins);
                    }
                }
            }
        }
    }

    // TODO there is too much common code between implementors:
    fn cycle(&mut self) {
        let n = self.bins_ready_count();
        self.push_in_progress(true);
        if self.bins_ready_count() == n {
            if let Some(range) = self.next_bin_range() {
                let mut bins = BinsDim0::<NTY>::empty();
                bins.append_zero(range.beg, range.end);
                match self.ready.as_mut() {
                    Some(ready) => {
                        ready.append_all_from(&mut bins);
                    }
                    None => {
                        self.ready = Some(bins);
                    }
                }
                if self.bins_ready_count() <= n {
                    error!("failed to push a zero bin");
                }
            } else {
                warn!("cycle: no in-progress bin pushed, but also no more bin to add as zero-bin");
            }
        }
    }

    fn set_range_complete(&mut self) {}

    fn empty(&self) -> Box<dyn items_0::TimeBinned> {
        let ret = <BinsDim0Aggregator<NTY> as TimeBinnableTypeAggregator>::Output::empty();
        Box::new(ret)
    }
}

impl<NTY: ScalarOps> TimeBinned for BinsDim0<NTY> {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnable {
        self as &dyn TimeBinnable
    }

    fn edges_slice(&self) -> (&[u64], &[u64]) {
        if self.ts1s.as_slices().1.len() != 0 {
            panic!();
        }
        if self.ts2s.as_slices().1.len() != 0 {
            panic!();
        }
        (&self.ts1s.as_slices().0, &self.ts2s.as_slices().0)
    }

    fn counts(&self) -> &[u64] {
        // TODO check for contiguous
        self.counts.as_slices().0
    }

    // TODO is Vec needed?
    fn mins(&self) -> Vec<f32> {
        self.mins.iter().map(|x| x.clone().as_prim_f32_b()).collect()
    }

    // TODO is Vec needed?
    fn maxs(&self) -> Vec<f32> {
        self.maxs.iter().map(|x| x.clone().as_prim_f32_b()).collect()
    }

    // TODO is Vec needed?
    fn avgs(&self) -> Vec<f32> {
        self.avgs.iter().map(Clone::clone).collect()
    }

    fn validate(&self) -> Result<(), String> {
        use std::fmt::Write;
        let mut msg = String::new();
        if self.ts1s.len() != self.ts2s.len() {
            write!(&mut msg, "ts1s ≠ ts2s\n").unwrap();
        }
        for (i, ((count, min), max)) in self.counts.iter().zip(&self.mins).zip(&self.maxs).enumerate() {
            if min.as_prim_f32_b() < 1. && *count != 0 {
                write!(&mut msg, "i {}  count {}  min {:?}  max {:?}\n", i, count, min, max).unwrap();
            }
        }
        if msg.is_empty() {
            Ok(())
        } else {
            Err(msg)
        }
    }

    fn as_collectable_mut(&mut self) -> &mut dyn Collectable {
        self
    }
}

impl<NTY> items_0::collect_c::Collectable for BinsDim0<NTY>
where
    NTY: ScalarOps,
{
    fn new_collector(&self) -> Box<dyn items_0::collect_c::Collector> {
        Box::new(BinsDim0Collector::<NTY>::new())
    }
}
