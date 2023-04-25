use crate::binsxbindim0::BinsXbinDim0;
use crate::IsoDateTime;
use crate::RangeOverlapInfo;
use crate::TimeBinnableType;
use crate::TimeBinnableTypeAggregator;
use err::Error;
use items_0::collect_s::Collectable;
use items_0::collect_s::CollectableType;
use items_0::collect_s::Collected;
use items_0::collect_s::CollectorType;
use items_0::collect_s::ToJsonBytes;
use items_0::collect_s::ToJsonResult;
use items_0::container::ByteEstimate;
use items_0::overlap::HasTimestampDeque;
use items_0::overlap::RangeOverlapCmp;
use items_0::scalar_ops::ScalarOps;
use items_0::timebin::TimeBinnable;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Empty;
use items_0::Events;
use items_0::EventsNonObj;
use items_0::MergeError;
use items_0::TypeName;
use items_0::WithLen;
use netpod::is_false;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::BinnedRangeEnum;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::mem;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsXbinDim0<NTY> {
    pub tss: VecDeque<u64>,
    pub pulses: VecDeque<u64>,
    pub mins: VecDeque<NTY>,
    pub maxs: VecDeque<NTY>,
    pub avgs: VecDeque<f32>,
    // TODO maybe add variance?
}

impl<NTY> EventsXbinDim0<NTY> {
    #[inline(always)]
    pub fn push(&mut self, ts: u64, pulse: u64, min: NTY, max: NTY, avg: f32) {
        self.tss.push_back(ts);
        self.pulses.push_back(pulse);
        self.mins.push_back(min);
        self.maxs.push_back(max);
        self.avgs.push_back(avg);
    }

    #[inline(always)]
    pub fn push_front(&mut self, ts: u64, pulse: u64, min: NTY, max: NTY, avg: f32) {
        self.tss.push_front(ts);
        self.pulses.push_front(pulse);
        self.mins.push_front(min);
        self.maxs.push_front(max);
        self.avgs.push_front(avg);
    }

    pub fn serde_id() -> &'static str {
        "EventsXbinDim0"
    }
}

impl<STY> TypeName for EventsXbinDim0<STY> {
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl<NTY> fmt::Debug for EventsXbinDim0<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("EventsXbinDim0")
            .field("tss", &self.tss)
            .field("pulses", &self.pulses)
            .field("mins", &self.mins)
            .field("maxs", &self.maxs)
            .field("avgs", &self.avgs)
            .finish()
    }
}

impl<NTY> ByteEstimate for EventsXbinDim0<NTY> {
    fn byte_estimate(&self) -> u64 {
        todo!("byte_estimate")
    }
}

impl<NTY> Empty for EventsXbinDim0<NTY> {
    fn empty() -> Self {
        Self {
            tss: VecDeque::new(),
            pulses: VecDeque::new(),
            mins: VecDeque::new(),
            maxs: VecDeque::new(),
            avgs: VecDeque::new(),
        }
    }
}

impl<NTY> AsAnyRef for EventsXbinDim0<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<NTY> AsAnyMut for EventsXbinDim0<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<NTY> WithLen for EventsXbinDim0<NTY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<STY: ScalarOps> HasTimestampDeque for EventsXbinDim0<STY> {
    fn timestamp_min(&self) -> Option<u64> {
        self.tss.front().map(|x| *x)
    }

    fn timestamp_max(&self) -> Option<u64> {
        self.tss.back().map(|x| *x)
    }

    fn pulse_min(&self) -> Option<u64> {
        self.pulses.front().map(|x| *x)
    }

    fn pulse_max(&self) -> Option<u64> {
        self.pulses.back().map(|x| *x)
    }
}

items_0::impl_range_overlap_info_events!(EventsXbinDim0);

impl<STY: ScalarOps> EventsNonObj for EventsXbinDim0<STY> {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>) {
        info!(
            "EventsXbinDim0::into_tss_pulses  len {}  len {}",
            self.tss.len(),
            self.pulses.len()
        );
        (self.tss, self.pulses)
    }
}

impl<STY: ScalarOps> Events for EventsXbinDim0<STY> {
    fn as_time_binnable_mut(&mut self) -> &mut dyn TimeBinnable {
        self as &mut dyn TimeBinnable
    }

    fn verify(&self) -> bool {
        let mut good = true;
        let mut ts_max = 0;
        for ts in &self.tss {
            let ts = *ts;
            if ts < ts_max {
                good = false;
                error!("unordered event data  ts {}  ts_max {}", ts, ts_max);
            }
            ts_max = ts_max.max(ts);
        }
        good
    }

    fn output_info(&self) {
        if false {
            info!("output_info  len {}", self.tss.len());
            if self.tss.len() == 1 {
                info!(
                    "  only:  ts {}  pulse {}  value {:?}",
                    self.tss[0], self.pulses[0], self.avgs[0]
                );
            } else if self.tss.len() > 1 {
                info!(
                    "  first: ts {}  pulse {}  value {:?}",
                    self.tss[0], self.pulses[0], self.avgs[0]
                );
                let n = self.tss.len() - 1;
                info!(
                    "  last:  ts {}  pulse {}  value {:?}",
                    self.tss[n], self.pulses[n], self.avgs[n]
                );
            }
        }
    }

    fn as_collectable_mut(&mut self) -> &mut dyn Collectable {
        self
    }

    fn as_collectable_with_default_ref(&self) -> &dyn Collectable {
        self
    }

    fn as_collectable_with_default_mut(&mut self) -> &mut dyn Collectable {
        self
    }

    fn take_new_events_until_ts(&mut self, ts_end: u64) -> Box<dyn Events> {
        // TODO improve the search
        let n1 = self.tss.iter().take_while(|&&x| x <= ts_end).count();
        let tss = self.tss.drain(..n1).collect();
        let pulses = self.pulses.drain(..n1).collect();
        let mins = self.mins.drain(..n1).collect();
        let maxs = self.maxs.drain(..n1).collect();
        let avgs = self.avgs.drain(..n1).collect();
        let ret = Self {
            tss,
            pulses,
            mins,
            maxs,
            avgs,
        };
        Box::new(ret)
    }

    fn new_empty_evs(&self) -> Box<dyn Events> {
        Box::new(Self::empty())
    }

    fn drain_into_evs(&mut self, dst: &mut Box<dyn Events>, range: (usize, usize)) -> Result<(), MergeError> {
        // TODO as_any and as_any_mut are declared on unrelated traits. Simplify.
        if let Some(dst) = dst.as_mut().as_any_mut().downcast_mut::<Self>() {
            // TODO make it harder to forget new members when the struct may get modified in the future
            let r = range.0..range.1;
            dst.tss.extend(self.tss.drain(r.clone()));
            dst.pulses.extend(self.pulses.drain(r.clone()));
            dst.mins.extend(self.mins.drain(r.clone()));
            dst.maxs.extend(self.maxs.drain(r.clone()));
            dst.avgs.extend(self.avgs.drain(r.clone()));
            Ok(())
        } else {
            error!("downcast to {} FAILED", self.type_name());
            Err(MergeError::NotCompatible)
        }
    }

    fn find_lowest_index_gt_evs(&self, ts: u64) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate() {
            if m > ts {
                return Some(i);
            }
        }
        None
    }

    fn find_lowest_index_ge_evs(&self, ts: u64) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate() {
            if m >= ts {
                return Some(i);
            }
        }
        None
    }

    fn find_highest_index_lt_evs(&self, ts: u64) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate().rev() {
            if m < ts {
                return Some(i);
            }
        }
        None
    }

    fn ts_min(&self) -> Option<u64> {
        self.tss.front().map(|&x| x)
    }

    fn ts_max(&self) -> Option<u64> {
        self.tss.back().map(|&x| x)
    }

    fn partial_eq_dyn(&self, other: &dyn Events) -> bool {
        if let Some(other) = other.as_any_ref().downcast_ref::<Self>() {
            self == other
        } else {
            false
        }
    }

    fn serde_id(&self) -> &'static str {
        Self::serde_id()
    }

    fn nty_id(&self) -> u32 {
        STY::SUB
    }

    fn clone_dyn(&self) -> Box<dyn Events> {
        Box::new(self.clone())
    }

    fn tss(&self) -> &VecDeque<u64> {
        &self.tss
    }

    fn pulses(&self) -> &VecDeque<u64> {
        &self.pulses
    }

    fn frame_type_id(&self) -> u32 {
        error!("TODO frame_type_id should not be called");
        // TODO make more nice
        panic!()
    }

    fn to_min_max_avg(&mut self) -> Box<dyn Events> {
        let dst = Self {
            tss: mem::replace(&mut self.tss, Default::default()),
            pulses: mem::replace(&mut self.pulses, Default::default()),
            mins: mem::replace(&mut self.mins, Default::default()),
            maxs: mem::replace(&mut self.maxs, Default::default()),
            avgs: mem::replace(&mut self.avgs, Default::default()),
        };
        Box::new(dst)
    }
}

impl<NTY> TimeBinnableType for EventsXbinDim0<NTY>
where
    NTY: ScalarOps,
{
    type Output = BinsXbinDim0<NTY>;
    type Aggregator = EventsXbinDim0Aggregator<NTY>;

    fn aggregator(range: SeriesRange, x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        let name = any::type_name::<Self>();
        debug!(
            "TimeBinnableType for {}  aggregator()  range {:?}  x_bin_count {}  do_time_weight {}",
            name, range, x_bin_count, do_time_weight
        );
        Self::Aggregator::new(range, do_time_weight)
    }
}

impl<NTY> TimeBinnable for EventsXbinDim0<NTY>
where
    NTY: ScalarOps,
{
    fn time_binner_new(
        &self,
        binrange: BinnedRangeEnum,
        do_time_weight: bool,
    ) -> Box<dyn items_0::timebin::TimeBinner> {
        todo!()
    }

    fn to_box_to_json_result(&self) -> Box<dyn ToJsonResult> {
        todo!()
    }
}

pub struct EventsXbinDim0Aggregator<NTY>
where
    NTY: ScalarOps,
{
    range: SeriesRange,
    count: u64,
    min: NTY,
    max: NTY,
    sumc: u64,
    sum: f32,
    int_ts: u64,
    last_ts: u64,
    last_avg: Option<f32>,
    last_min: Option<NTY>,
    last_max: Option<NTY>,
    do_time_weight: bool,
}

impl<NTY> EventsXbinDim0Aggregator<NTY>
where
    NTY: ScalarOps,
{
    pub fn new(range: SeriesRange, do_time_weight: bool) -> Self {
        Self {
            int_ts: todo!(),
            range,
            count: 0,
            min: NTY::zero_b(),
            max: NTY::zero_b(),
            sumc: 0,
            sum: 0f32,
            last_ts: 0,
            last_avg: None,
            last_min: None,
            last_max: None,
            do_time_weight,
        }
    }

    fn apply_min_max(&mut self, min: NTY, max: NTY) {
        if self.count == 0 {
            self.min = min;
            self.max = max;
        } else {
            if min < self.min {
                self.min = min;
            }
            if max > self.max {
                self.max = max;
            }
        }
    }

    fn apply_event_unweight(&mut self, avg: f32, min: NTY, max: NTY) {
        //debug!("apply_event_unweight");
        self.apply_min_max(min, max);
        let vf = avg;
        if vf.is_nan() {
        } else {
            self.sum += vf;
            self.sumc += 1;
        }
    }

    fn apply_event_time_weight(&mut self, ts: u64) {
        //debug!("apply_event_time_weight");
        /*if let (Some(avg), Some(min), Some(max)) = (self.last_avg, &self.last_min, &self.last_max) {
            let min2 = min.clone();
            let max2 = max.clone();
            self.apply_min_max(min2, max2);
            let w = (ts - self.int_ts) as f32 / self.range.delta() as f32;
            if avg.is_nan() {
            } else {
                self.sum += avg * w;
            }
            self.sumc += 1;
            self.int_ts = ts;
        }*/
        todo!()
    }

    fn ingest_unweight(&mut self, item: &EventsXbinDim0<NTY>) {
        /*for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let avg = item.avgs[i1];
            let min = item.mins[i1].clone();
            let max = item.maxs[i1].clone();
            if ts < self.range.beg {
            } else if ts >= self.range.end {
            } else {
                self.apply_event_unweight(avg, min, max);
                self.count += 1;
            }
        }*/
        todo!()
    }

    fn ingest_time_weight(&mut self, item: &EventsXbinDim0<NTY>) {
        /*for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let avg = item.avgs[i1];
            let min = item.mins[i1].clone();
            let max = item.maxs[i1].clone();
            if ts < self.int_ts {
                self.last_ts = ts;
                self.last_avg = Some(avg);
                self.last_min = Some(min);
                self.last_max = Some(max);
            } else if ts >= self.range.end {
                return;
            } else {
                self.apply_event_time_weight(ts);
                self.count += 1;
                self.last_ts = ts;
                self.last_avg = Some(avg);
                self.last_min = Some(min);
                self.last_max = Some(max);
            }
        }*/
        todo!()
    }

    fn result_reset_unweight(&mut self, range: SeriesRange, _expand: bool) -> BinsXbinDim0<NTY> {
        /*let avg = if self.sumc == 0 {
            0f32
        } else {
            self.sum / self.sumc as f32
        };
        let ret = BinsXbinDim0::from_content(
            [self.range.beg].into(),
            [self.range.end].into(),
            [self.count].into(),
            [self.min.clone()].into(),
            [self.max.clone()].into(),
            [avg].into(),
        );
        self.int_ts = range.beg;
        self.range = range;
        self.count = 0;
        self.min = NTY::zero_b();
        self.max = NTY::zero_b();
        self.sum = 0f32;
        self.sumc = 0;
        ret*/
        todo!()
    }

    fn result_reset_time_weight(&mut self, range: SeriesRange, expand: bool) -> BinsXbinDim0<NTY> {
        // TODO check callsite for correct expand status.
        /*if true || expand {
            self.apply_event_time_weight(self.range.end);
        }
        let avg = {
            let sc = self.range.delta() as f32 * 1e-9;
            self.sum / sc
        };
        let ret = BinsXbinDim0::from_content(
            [self.range.beg].into(),
            [self.range.end].into(),
            [self.count].into(),
            [self.min.clone()].into(),
            [self.max.clone()].into(),
            [avg].into(),
        );
        self.int_ts = range.beg;
        self.range = range;
        self.count = 0;
        self.min = NTY::zero_b();
        self.max = NTY::zero_b();
        self.sum = 0f32;
        self.sumc = 0;
        ret*/
        todo!()
    }
}

impl<NTY> TimeBinnableTypeAggregator for EventsXbinDim0Aggregator<NTY>
where
    NTY: ScalarOps,
{
    type Input = EventsXbinDim0<NTY>;
    type Output = BinsXbinDim0<NTY>;

    fn range(&self) -> &SeriesRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        debug!("ingest");
        if self.do_time_weight {
            self.ingest_time_weight(item)
        } else {
            self.ingest_unweight(item)
        }
    }

    fn result_reset(&mut self, range: SeriesRange, expand: bool) -> Self::Output {
        if self.do_time_weight {
            self.result_reset_time_weight(range, expand)
        } else {
            self.result_reset_unweight(range, expand)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsXbinDim0CollectorOutput<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: VecDeque<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: VecDeque<u64>,
    #[serde(rename = "pulseAnchor")]
    pulse_anchor: u64,
    #[serde(rename = "pulseOff")]
    pulse_off: VecDeque<u64>,
    #[serde(rename = "mins")]
    mins: VecDeque<NTY>,
    #[serde(rename = "maxs")]
    maxs: VecDeque<NTY>,
    #[serde(rename = "avgs")]
    avgs: VecDeque<f32>,
    #[serde(rename = "rangeFinal", default, skip_serializing_if = "is_false")]
    range_final: bool,
    #[serde(rename = "timedOut", default, skip_serializing_if = "is_false")]
    timed_out: bool,
    #[serde(rename = "continueAt", default, skip_serializing_if = "Option::is_none")]
    continue_at: Option<IsoDateTime>,
}

impl<NTY> AsAnyRef for EventsXbinDim0CollectorOutput<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<NTY> AsAnyMut for EventsXbinDim0CollectorOutput<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<NTY: ScalarOps> WithLen for EventsXbinDim0CollectorOutput<NTY> {
    fn len(&self) -> usize {
        self.mins.len()
    }
}

impl<NTY> ToJsonResult for EventsXbinDim0CollectorOutput<NTY>
where
    NTY: ScalarOps,
{
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        let k = serde_json::to_value(self)?;
        Ok(Box::new(k))
    }
}

impl<NTY> Collected for EventsXbinDim0CollectorOutput<NTY> where NTY: ScalarOps {}

#[derive(Debug)]
pub struct EventsXbinDim0Collector<NTY> {
    vals: EventsXbinDim0<NTY>,
    range_final: bool,
    timed_out: bool,
}

impl<NTY> EventsXbinDim0Collector<NTY> {
    pub fn new() -> Self {
        Self {
            range_final: false,
            timed_out: false,
            vals: EventsXbinDim0::empty(),
        }
    }
}

impl<NTY> WithLen for EventsXbinDim0Collector<NTY> {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

impl<NTY> CollectorType for EventsXbinDim0Collector<NTY>
where
    NTY: ScalarOps,
{
    type Input = EventsXbinDim0<NTY>;
    type Output = EventsXbinDim0CollectorOutput<NTY>;

    fn ingest(&mut self, src: &mut Self::Input) {
        self.vals.tss.append(&mut src.tss);
        self.vals.pulses.append(&mut src.pulses);
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

    fn result(
        &mut self,
        range: Option<SeriesRange>,
        _binrange: Option<BinnedRangeEnum>,
    ) -> Result<Self::Output, Error> {
        /*use std::mem::replace;
        let continue_at = if self.timed_out {
            if let Some(ts) = self.vals.tss.back() {
                Some(IsoDateTime::from_u64(*ts + netpod::timeunits::MS))
            } else {
                if let Some(range) = &range {
                    Some(IsoDateTime::from_u64(range.beg + netpod::timeunits::SEC))
                } else {
                    warn!("can not determine continue-at parameters");
                    None
                }
            }
        } else {
            None
        };
        let mins = replace(&mut self.vals.mins, VecDeque::new());
        let maxs = replace(&mut self.vals.maxs, VecDeque::new());
        let avgs = replace(&mut self.vals.avgs, VecDeque::new());
        self.vals.tss.make_contiguous();
        self.vals.pulses.make_contiguous();
        let tst = crate::ts_offs_from_abs(self.vals.tss.as_slices().0);
        let (pulse_anchor, pulse_off) = crate::pulse_offs_from_abs(&self.vals.pulses.as_slices().0);
        let ret = Self::Output {
            ts_anchor_sec: tst.0,
            ts_off_ms: tst.1,
            ts_off_ns: tst.2,
            pulse_anchor,
            pulse_off,
            mins,
            maxs,
            avgs,
            range_final: self.range_final,
            timed_out: self.timed_out,
            continue_at,
        };
        Ok(ret)*/
        todo!()
    }
}

impl<NTY> CollectableType for EventsXbinDim0<NTY>
where
    NTY: ScalarOps,
{
    type Collector = EventsXbinDim0Collector<NTY>;

    fn new_collector() -> Self::Collector {
        Self::Collector::new()
    }
}
