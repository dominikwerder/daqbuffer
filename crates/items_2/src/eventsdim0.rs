use crate::binsdim0::BinsDim0;
use crate::framable::FrameType;
use crate::framable::FrameTypeStatic;
use crate::timebin::ChooseIndicesForTimeBin;
use crate::timebin::ChooseIndicesForTimeBinEvents;
use crate::timebin::TimeAggregatorCommonV0Func;
use crate::timebin::TimeAggregatorCommonV0Trait;
use crate::timebin::TimeBinnerCommonV0Func;
use crate::timebin::TimeBinnerCommonV0Trait;
use crate::IsoDateTime;
use crate::RangeOverlapInfo;
use crate::TimeBinnableType;
use crate::TimeBinnableTypeAggregator;
use err::Error;
use items_0::collect_s::Collectable;
use items_0::collect_s::Collected;
use items_0::collect_s::Collector;
use items_0::collect_s::CollectorType;
use items_0::collect_s::ToJsonBytes;
use items_0::collect_s::ToJsonResult;
use items_0::container::ByteEstimate;
use items_0::framable::FrameTypeInnerStatic;
use items_0::overlap::HasTimestampDeque;
use items_0::scalar_ops::ScalarOps;
use items_0::test::f32_iter_cmp_near;
use items_0::timebin::TimeBinnable;
use items_0::timebin::TimeBinned;
use items_0::timebin::TimeBinner;
use items_0::AppendAllFrom;
use items_0::AppendEmptyBin;
use items_0::Appendable;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Empty;
use items_0::Events;
use items_0::EventsNonObj;
use items_0::HasNonemptyFirstBin;
use items_0::MergeError;
use items_0::Resettable;
use items_0::TypeName;
use items_0::WithLen;
use netpod::is_false;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::MS;
use netpod::timeunits::SEC;
use netpod::BinnedRange;
use netpod::BinnedRangeEnum;
use netpod::TsNano;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::mem;

#[allow(unused)]
macro_rules! trace_ingest {
    ($($arg:tt)*) => {};
    ($($arg:tt)*) => { trace!($($arg)*); };
}

#[allow(unused)]
macro_rules! trace_ingest_item {
    ($($arg:tt)*) => {};
    ($($arg:tt)*) => { trace!($($arg)*); };
}

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => {};
    ($($arg:tt)*) => { trace!($($arg)*); };
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsDim0<STY> {
    pub tss: VecDeque<u64>,
    pub pulses: VecDeque<u64>,
    pub values: VecDeque<STY>,
}

impl<STY> EventsDim0<STY> {
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn push_front(&mut self, ts: u64, pulse: u64, value: STY) {
        self.tss.push_front(ts);
        self.pulses.push_front(pulse);
        self.values.push_front(value);
    }

    pub fn serde_id() -> &'static str {
        "EventsDim0"
    }

    pub fn tss(&self) -> &VecDeque<u64> {
        &self.tss
    }
}

impl<STY> AsAnyRef for EventsDim0<STY>
where
    STY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<STY> AsAnyMut for EventsDim0<STY>
where
    STY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY> Empty for EventsDim0<STY> {
    fn empty() -> Self {
        Self {
            tss: VecDeque::new(),
            pulses: VecDeque::new(),
            values: VecDeque::new(),
        }
    }
}

impl<STY> fmt::Debug for EventsDim0<STY>
where
    STY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        if false {
            write!(
                fmt,
                "{} {{ count {}  ts {:?}  vals {:?} }}",
                self.type_name(),
                self.tss.len(),
                self.tss.iter().map(|x| x / SEC).collect::<Vec<_>>(),
                self.values,
            )
        } else {
            write!(
                fmt,
                "{} {{ count {}  ts {:?} .. {:?}  vals {:?} .. {:?} }}",
                self.type_name(),
                self.tss.len(),
                self.tss.front().map(|x| x / SEC),
                self.tss.back().map(|x| x / SEC),
                self.values.front(),
                self.values.back(),
            )
        }
    }
}

impl<STY> WithLen for EventsDim0<STY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<STY> ByteEstimate for EventsDim0<STY> {
    fn byte_estimate(&self) -> u64 {
        let stylen = mem::size_of::<STY>();
        (self.len() * (8 + 8 + stylen)) as u64
    }
}

impl<STY> Resettable for EventsDim0<STY> {
    fn reset(&mut self) {
        self.tss.clear();
        self.pulses.clear();
        self.values.clear();
    }
}

impl<STY: ScalarOps> HasTimestampDeque for EventsDim0<STY> {
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

items_0::impl_range_overlap_info_events!(EventsDim0);

impl<STY> ChooseIndicesForTimeBin for EventsDim0<STY> {
    fn choose_indices_unweight(&self, beg: u64, end: u64) -> (Option<usize>, usize, usize) {
        ChooseIndicesForTimeBinEvents::choose_unweight(beg, end, &self.tss)
    }

    fn choose_indices_timeweight(&self, beg: u64, end: u64) -> (Option<usize>, usize, usize) {
        ChooseIndicesForTimeBinEvents::choose_timeweight(beg, end, &self.tss)
    }
}

impl<STY> TimeBinnableType for EventsDim0<STY>
where
    STY: ScalarOps,
{
    type Output = BinsDim0<STY>;
    type Aggregator = EventsDim0Aggregator<STY>;

    fn aggregator(range: SeriesRange, x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        let self_name = any::type_name::<Self>();
        debug!(
            "TimeBinnableType for {self_name}  aggregator()  range {:?}  x_bin_count {}  do_time_weight {}",
            range, x_bin_count, do_time_weight
        );
        Self::Aggregator::new(range, do_time_weight)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsDim0ChunkOutput<STY> {
    tss: VecDeque<u64>,
    pulses: VecDeque<u64>,
    values: VecDeque<STY>,
    scalar_type: String,
}

impl<STY: ScalarOps> EventsDim0ChunkOutput<STY> {}

#[derive(Debug)]
pub struct EventsDim0Collector<STY> {
    vals: EventsDim0<STY>,
    range_final: bool,
    timed_out: bool,
}

impl<STY> EventsDim0Collector<STY> {
    pub fn self_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            vals: EventsDim0::empty(),
            range_final: false,
            timed_out: false,
        }
    }
}

impl<STY> WithLen for EventsDim0Collector<STY> {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsDim0CollectorOutput<STY> {
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
    #[serde(rename = "values")]
    values: VecDeque<STY>,
    #[serde(rename = "rangeFinal", default, skip_serializing_if = "is_false")]
    range_final: bool,
    #[serde(rename = "timedOut", default, skip_serializing_if = "is_false")]
    timed_out: bool,
    #[serde(rename = "continueAt", default, skip_serializing_if = "Option::is_none")]
    continue_at: Option<IsoDateTime>,
}

impl<STY: ScalarOps> EventsDim0CollectorOutput<STY> {
    pub fn ts_anchor_sec(&self) -> u64 {
        self.ts_anchor_sec
    }

    pub fn ts_off_ms(&self) -> &VecDeque<u64> {
        &self.ts_off_ms
    }

    pub fn pulse_anchor(&self) -> u64 {
        self.pulse_anchor
    }

    pub fn pulse_off(&self) -> &VecDeque<u64> {
        &self.pulse_off
    }

    /// Note: only used for unit tests.
    pub fn values_to_f32(&self) -> VecDeque<f32> {
        self.values.iter().map(|x| x.as_prim_f32_b()).collect()
    }

    pub fn range_final(&self) -> bool {
        self.range_final
    }

    pub fn timed_out(&self) -> bool {
        self.timed_out
    }

    pub fn is_valid(&self) -> bool {
        if self.ts_off_ms.len() != self.ts_off_ns.len() {
            false
        } else if self.ts_off_ms.len() != self.pulse_off.len() {
            false
        } else if self.ts_off_ms.len() != self.values.len() {
            false
        } else {
            true
        }
    }

    pub fn info_str(&self) -> String {
        use fmt::Write;
        let mut out = String::new();
        write!(
            out,
            "ts_off_ms {}  ts_off_ns {}  pulse_off {}  values {}",
            self.ts_off_ms.len(),
            self.ts_off_ns.len(),
            self.pulse_off.len(),
            self.values.len(),
        )
        .unwrap();
        out
    }
}

impl<STY> AsAnyRef for EventsDim0CollectorOutput<STY>
where
    STY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<STY> AsAnyMut for EventsDim0CollectorOutput<STY>
where
    STY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY: ScalarOps> WithLen for EventsDim0CollectorOutput<STY> {
    fn len(&self) -> usize {
        self.values.len()
    }
}

impl<STY: ScalarOps> ToJsonResult for EventsDim0CollectorOutput<STY> {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        let k = serde_json::to_value(self)?;
        Ok(Box::new(k))
    }
}

impl<STY: ScalarOps> Collected for EventsDim0CollectorOutput<STY> {}

impl<STY: ScalarOps> CollectorType for EventsDim0Collector<STY> {
    type Input = EventsDim0<STY>;
    type Output = EventsDim0CollectorOutput<STY>;

    fn ingest(&mut self, src: &mut Self::Input) {
        self.vals.tss.append(&mut src.tss);
        self.vals.pulses.append(&mut src.pulses);
        self.vals.values.append(&mut src.values);
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
        // If we timed out, we want to hint the client from where to continue.
        // This is tricky: currently, client can not request a left-exclusive range.
        // We currently give the timestamp of the last event plus a small delta.
        // The amount of the delta must take into account what kind of timestamp precision the client
        // can parse and handle.
        let vals = &mut self.vals;
        let continue_at = if self.timed_out {
            if let Some(ts) = vals.tss.back() {
                Some(IsoDateTime::from_u64(*ts + MS))
            } else {
                if let Some(range) = &range {
                    match range {
                        SeriesRange::TimeRange(x) => Some(IsoDateTime::from_u64(x.beg + SEC)),
                        SeriesRange::PulseRange(x) => {
                            error!("TODO emit create continueAt for pulse range");
                            None
                        }
                    }
                } else {
                    warn!("can not determine continue-at parameters");
                    None
                }
            }
        } else {
            None
        };
        let tss_sl = vals.tss.make_contiguous();
        let pulses_sl = vals.pulses.make_contiguous();
        let (ts_anchor_sec, ts_off_ms, ts_off_ns) = crate::ts_offs_from_abs(tss_sl);
        let (pulse_anchor, pulse_off) = crate::pulse_offs_from_abs(pulses_sl);
        let values = mem::replace(&mut vals.values, VecDeque::new());
        if ts_off_ms.len() != ts_off_ns.len() {
            return Err(Error::with_msg_no_trace("collected len mismatch"));
        }
        if ts_off_ms.len() != pulse_off.len() {
            return Err(Error::with_msg_no_trace("collected len mismatch"));
        }
        if ts_off_ms.len() != values.len() {
            return Err(Error::with_msg_no_trace("collected len mismatch"));
        }
        let ret = Self::Output {
            ts_anchor_sec,
            ts_off_ms,
            ts_off_ns,
            pulse_anchor,
            pulse_off,
            values,
            range_final: self.range_final,
            timed_out: self.timed_out,
            continue_at,
        };
        if !ret.is_valid() {
            error!("invalid:\n{}", ret.info_str());
        }
        Ok(ret)
    }
}

impl<STY: ScalarOps> items_0::collect_s::CollectableType for EventsDim0<STY> {
    type Collector = EventsDim0Collector<STY>;

    fn new_collector() -> Self::Collector {
        Self::Collector::new()
    }
}

#[derive(Debug)]
pub struct EventsDim0Aggregator<STY> {
    range: SeriesRange,
    count: u64,
    minmax: Option<(STY, STY)>,
    sumc: u64,
    sum: f32,
    int_ts: u64,
    last_ts: u64,
    last_val: Option<STY>,
    do_time_weight: bool,
    events_ignored_count: u64,
    items_seen: usize,
}

impl<STY> Drop for EventsDim0Aggregator<STY> {
    fn drop(&mut self) {
        // TODO collect as stats for the request context:
        trace!("count {}  ignored {}", self.count, self.events_ignored_count);
    }
}

impl<STY: ScalarOps> TimeAggregatorCommonV0Trait for EventsDim0Aggregator<STY> {
    type Input = <Self as TimeBinnableTypeAggregator>::Input;
    type Output = <Self as TimeBinnableTypeAggregator>::Output;

    fn type_name() -> &'static str {
        Self::type_name()
    }

    fn common_range_current(&self) -> &SeriesRange {
        &self.range
    }

    fn common_ingest_unweight_range(&mut self, item: &Self::Input, r: std::ops::Range<usize>) {
        for (&ts, val) in item.tss.range(r.clone()).zip(item.values.range(r)) {
            self.apply_event_unweight(val.clone());
            self.count += 1;
            self.last_ts = ts;
            self.last_val = Some(val.clone());
        }
    }

    fn common_ingest_one_before(&mut self, item: &Self::Input, j: usize) {
        //trace_ingest!("{self_name} ingest  {:6}  {:20}  {:10?}  BEFORE", i1, ts, val);
        self.last_ts = item.tss[j];
        self.last_val = Some(item.values[j].clone());
    }

    fn common_ingest_range(&mut self, item: &Self::Input, r: std::ops::Range<usize>) {
        let beg = self.range.beg_u64();
        for (&ts, val) in item.tss.range(r.clone()).zip(item.values.range(r)) {
            if ts > beg {
                self.apply_event_time_weight(ts);
            }
            self.count += 1;
            self.last_ts = ts;
            self.last_val = Some(val.clone());
        }
    }
}

impl<STY: ScalarOps> EventsDim0Aggregator<STY> {
    fn type_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new(range: SeriesRange, do_time_weight: bool) -> Self {
        let int_ts = range.beg_u64();
        Self {
            range,
            count: 0,
            minmax: None,
            sumc: 0,
            sum: 0.,
            int_ts,
            last_ts: 0,
            last_val: None,
            do_time_weight,
            events_ignored_count: 0,
            items_seen: 0,
        }
    }

    // TODO reduce clone.. optimize via more traits to factor the trade-offs?
    fn apply_min_max(&mut self, val: STY) {
        trace_ingest!(
            "apply_min_max  val {:?}  last_val {:?}  count {}  sumc {:?}  minmax {:?}",
            val,
            self.last_val,
            self.count,
            self.sumc,
            self.minmax,
        );
        if let Some((min, max)) = self.minmax.as_mut() {
            if *min > val {
                *min = val.clone();
            }
            if *max < val {
                *max = val.clone();
            }
        } else {
            self.minmax = Some((val.clone(), val.clone()));
        }
    }

    fn apply_event_unweight(&mut self, val: STY) {
        error!("TODO check again result_reset_unweight");
        err::todo();
        let vf = val.as_prim_f32_b();
        self.apply_min_max(val);
        if vf.is_nan() {
        } else {
            self.sum += vf;
            self.sumc += 1;
        }
    }

    fn apply_event_time_weight(&mut self, px: u64) {
        if let Some(v) = &self.last_val {
            trace_ingest!("apply_event_time_weight with v {v:?}");
            let vf = v.as_prim_f32_b();
            let v2 = v.clone();
            self.apply_min_max(v2);
            self.sumc += 1;
            let w = (px - self.int_ts) as f32 * 1e-9;
            if false {
                trace!(
                    "int_ts {:10}  px {:8}  w {:8.1}  vf {:8.1}  sum {:8.1}",
                    self.int_ts / MS,
                    px / MS,
                    w,
                    vf,
                    self.sum
                );
            }
            if vf.is_nan() {
            } else {
                self.sum += vf * w;
            }
            self.int_ts = px;
        } else {
            debug!("apply_event_time_weight NO VALUE");
        }
    }

    fn ingest_unweight(&mut self, item: &<Self as TimeBinnableTypeAggregator>::Input) {
        TimeAggregatorCommonV0Func::ingest_time_weight(self, item)
    }

    fn ingest_time_weight(&mut self, item: &<Self as TimeBinnableTypeAggregator>::Input) {
        TimeAggregatorCommonV0Func::ingest_time_weight(self, item)
    }

    fn reset_values(&mut self, range: SeriesRange) {
        self.int_ts = range.beg_u64();
        trace!("ON RESET SET int_ts {:10}", self.int_ts);
        self.range = range;
        self.count = 0;
        self.sum = 0.;
        self.sumc = 0;
        self.minmax = None;
        self.items_seen = 0;
    }

    fn result_reset_unweight(&mut self, range: SeriesRange) -> BinsDim0<STY> {
        let (min, max) = if let Some((min, max)) = self.minmax.take() {
            (min, max)
        } else {
            (STY::zero_b(), STY::zero_b())
        };
        let avg = if self.sumc > 0 {
            self.sum / self.sumc as f32
        } else {
            STY::zero_b().as_prim_f32_b()
        };
        let ret = if self.range.is_time() {
            BinsDim0 {
                ts1s: [self.range.beg_u64()].into(),
                ts2s: [self.range.end_u64()].into(),
                counts: [self.count].into(),
                mins: [min].into(),
                maxs: [max].into(),
                avgs: [avg].into(),
                dim0kind: Some(self.range.dim0kind()),
            }
        } else {
            error!("TODO result_reset_unweight");
            err::todoval()
        };
        self.reset_values(range);
        ret
    }

    fn result_reset_time_weight(&mut self, range: SeriesRange) -> BinsDim0<STY> {
        // TODO check callsite for correct expand status.
        debug!(
            "result_reset_time_weight calls apply_event_time_weight  range {:?}  items_seen {}  count {}",
            self.range, self.items_seen, self.count
        );
        let range_beg = self.range.beg_u64();
        let range_end = self.range.end_u64();
        if self.range.is_time() {
            self.apply_event_time_weight(range_end);
        } else {
            error!("TODO result_reset_time_weight");
            err::todoval()
        }
        let (min, max) = if let Some((min, max)) = self.minmax.take() {
            (min, max)
        } else {
            (STY::zero_b(), STY::zero_b())
        };
        let avg = if self.sumc > 0 {
            self.sum / (self.range.delta_u64() as f32 * 1e-9)
        } else {
            if let Some(v) = self.last_val.as_ref() {
                v.as_prim_f32_b()
            } else {
                STY::zero_b().as_prim_f32_b()
            }
        };
        let ret = if self.range.is_time() {
            BinsDim0 {
                ts1s: [range_beg].into(),
                ts2s: [range_end].into(),
                counts: [self.count].into(),
                mins: [min].into(),
                maxs: [max].into(),
                avgs: [avg].into(),
                dim0kind: Some(self.range.dim0kind()),
            }
        } else {
            error!("TODO result_reset_time_weight");
            err::todoval()
        };
        self.reset_values(range);
        ret
    }
}

impl<STY: ScalarOps> TimeBinnableTypeAggregator for EventsDim0Aggregator<STY> {
    type Input = EventsDim0<STY>;
    type Output = BinsDim0<STY>;

    fn range(&self) -> &SeriesRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        if true {
            trace_ingest!("{} ingest {} events", Self::type_name(), item.len());
        }
        if false {
            for (i, &ts) in item.tss.iter().enumerate() {
                trace_ingest!("{} ingest  {:6}  {:20}", Self::type_name(), i, ts);
            }
        }
        if self.do_time_weight {
            self.ingest_time_weight(item)
        } else {
            self.ingest_unweight(item)
        }
    }

    fn result_reset(&mut self, range: SeriesRange) -> Self::Output {
        trace!("result_reset  {:?}", range);
        if self.do_time_weight {
            self.result_reset_time_weight(range)
        } else {
            self.result_reset_unweight(range)
        }
    }
}

impl<STY: ScalarOps> TimeBinnable for EventsDim0<STY> {
    fn time_binner_new(&self, binrange: BinnedRangeEnum, do_time_weight: bool) -> Box<dyn TimeBinner> {
        // TODO get rid of unwrap
        let ret = EventsDim0TimeBinner::<STY>::new(binrange, do_time_weight).unwrap();
        Box::new(ret)
    }

    fn to_box_to_json_result(&self) -> Box<dyn items_0::collect_s::ToJsonResult> {
        let k = serde_json::to_value(self).unwrap();
        Box::new(k) as _
    }
}

impl<STY> TypeName for EventsDim0<STY> {
    fn type_name(&self) -> String {
        let self_name = any::type_name::<Self>();
        format!("{self_name}")
    }
}

impl<STY: ScalarOps> EventsNonObj for EventsDim0<STY> {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>) {
        trace!(
            "{}::into_tss_pulses  len {}  len {}",
            Self::type_name(),
            self.tss.len(),
            self.pulses.len()
        );
        (self.tss, self.pulses)
    }
}

impl<STY: ScalarOps> Events for EventsDim0<STY> {
    fn as_time_binnable_ref(&self) -> &dyn TimeBinnable {
        self
    }

    fn as_time_binnable_mut(&mut self) -> &mut dyn TimeBinnable {
        self
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
                    self.tss[0], self.pulses[0], self.values[0]
                );
            } else if self.tss.len() > 1 {
                info!(
                    "  first: ts {}  pulse {}  value {:?}",
                    self.tss[0], self.pulses[0], self.values[0]
                );
                let n = self.tss.len() - 1;
                info!(
                    "  last:  ts {}  pulse {}  value {:?}",
                    self.tss[n], self.pulses[n], self.values[n]
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
        let values = self.values.drain(..n1).collect();
        let ret = Self { tss, pulses, values };
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
            dst.values.extend(self.values.drain(r.clone()));
            Ok(())
        } else {
            error!("downcast to EventsDim0 FAILED");
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
            values: mem::replace(&mut self.values, Default::default()),
        };
        Box::new(dst)
    }

    fn to_cbor_vec_u8(&self) -> Vec<u8> {
        let ret = EventsDim0ChunkOutput {
            // TODO use &mut to swap the content
            tss: self.tss.clone(),
            pulses: self.pulses.clone(),
            values: self.values.clone(),
            scalar_type: STY::scalar_type_name().into(),
        };
        let mut buf = Vec::new();
        ciborium::into_writer(&ret, &mut buf).unwrap();
        buf
    }
}

#[derive(Debug)]
pub struct EventsDim0TimeBinner<STY: ScalarOps> {
    binrange: BinnedRangeEnum,
    rix: usize,
    rng: Option<SeriesRange>,
    agg: EventsDim0Aggregator<STY>,
    ready: Option<<EventsDim0Aggregator<STY> as TimeBinnableTypeAggregator>::Output>,
    range_final: bool,
}

impl<STY: ScalarOps> EventsDim0TimeBinner<STY> {
    fn type_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new(binrange: BinnedRangeEnum, do_time_weight: bool) -> Result<Self, Error> {
        trace!("{}::new  binrange {binrange:?}", Self::type_name());
        let rng = binrange
            .range_at(0)
            .ok_or_else(|| Error::with_msg_no_trace("empty binrange"))?;
        let agg = EventsDim0Aggregator::new(rng, do_time_weight);
        let ret = Self {
            binrange,
            rix: 0,
            rng: Some(agg.range().clone()),
            agg,
            ready: None,
            range_final: false,
        };
        Ok(ret)
    }

    fn next_bin_range(&mut self) -> Option<SeriesRange> {
        self.rix += 1;
        if let Some(rng) = self.binrange.range_at(self.rix) {
            trace!("{}  next_bin_range {:?}", Self::type_name(), rng);
            Some(rng)
        } else {
            trace!("{}  next_bin_range None", Self::type_name());
            None
        }
    }
}

impl<STY: ScalarOps> TimeBinnerCommonV0Trait for EventsDim0TimeBinner<STY> {
    type Input = <EventsDim0Aggregator<STY> as TimeBinnableTypeAggregator>::Input;
    type Output = <EventsDim0Aggregator<STY> as TimeBinnableTypeAggregator>::Output;

    fn type_name() -> &'static str {
        Self::type_name()
    }

    fn common_bins_ready_count(&self) -> usize {
        match &self.ready {
            Some(k) => k.len(),
            None => 0,
        }
    }

    fn common_range_current(&self) -> &SeriesRange {
        self.agg.range()
    }

    fn common_has_more_range(&self) -> bool {
        self.rng.is_some()
    }

    fn common_next_bin_range(&mut self) -> Option<SeriesRange> {
        self.next_bin_range()
    }

    fn common_set_current_range(&mut self, range: Option<SeriesRange>) {
        self.rng = range;
    }

    fn common_take_or_append_all_from(&mut self, item: Self::Output) {
        let mut item = item;
        match self.ready.as_mut() {
            Some(ready) => {
                ready.append_all_from(&mut item);
            }
            None => {
                self.ready = Some(item);
            }
        }
    }

    fn common_result_reset(&mut self, range: Option<SeriesRange>) -> Self::Output {
        self.agg.result_reset(range.unwrap_or_else(|| {
            SeriesRange::TimeRange(netpod::range::evrange::NanoRange {
                beg: u64::MAX,
                end: u64::MAX,
            })
        }))
    }

    fn common_agg_ingest(&mut self, item: &mut Self::Input) {
        self.agg.ingest(item)
    }
}

impl<STY: ScalarOps> TimeBinner for EventsDim0TimeBinner<STY> {
    fn bins_ready_count(&self) -> usize {
        TimeBinnerCommonV0Trait::common_bins_ready_count(self)
    }

    fn bins_ready(&mut self) -> Option<Box<dyn TimeBinned>> {
        match self.ready.take() {
            Some(k) => Some(Box::new(k)),
            None => None,
        }
    }

    fn ingest(&mut self, item: &mut dyn TimeBinnable) {
        TimeBinnerCommonV0Func::ingest(self, item)
    }

    fn push_in_progress(&mut self, push_empty: bool) {
        TimeBinnerCommonV0Func::push_in_progress(self, push_empty)
    }

    fn cycle(&mut self) {
        TimeBinnerCommonV0Func::cycle(self)
    }

    fn set_range_complete(&mut self) {
        self.range_final = true;
    }

    fn empty(&self) -> Box<dyn TimeBinned> {
        let ret = <EventsDim0Aggregator<STY> as TimeBinnableTypeAggregator>::Output::empty();
        Box::new(ret)
    }

    fn append_empty_until_end(&mut self) {
        // nothing to do for events
    }
}

impl<STY> Appendable<STY> for EventsDim0<STY>
where
    STY: ScalarOps,
{
    fn push(&mut self, ts: u64, pulse: u64, value: STY) {
        self.tss.push_back(ts);
        self.pulses.push_back(pulse);
        self.values.push_back(value);
    }
}

#[cfg(test)]
mod test_frame {
    use super::*;
    use crate::channelevents::ChannelEvents;
    use crate::framable::Framable;
    use crate::framable::INMEM_FRAME_ENCID;
    use crate::frame::decode_frame;
    use crate::inmem::InMemoryFrame;
    use items_0::streamitem::RangeCompletableItem;
    use items_0::streamitem::Sitemty;
    use items_0::streamitem::StreamItem;

    #[test]
    fn events_serialize() {
        taskrun::tracing_init_testing().unwrap();
        let mut events = EventsDim0::empty();
        events.push(123, 234, 55f32);
        let events = events;
        let events: Box<dyn Events> = Box::new(events);
        let item = ChannelEvents::Events(events);
        let item = Ok::<_, Error>(StreamItem::DataItem(RangeCompletableItem::Data(item)));
        let mut buf = item.make_frame().unwrap();
        let s = String::from_utf8_lossy(&buf[20..buf.len() - 4]);
        eprintln!("[[{s}]]");
        let buflen = buf.len();
        let frame = InMemoryFrame {
            encid: INMEM_FRAME_ENCID,
            tyid: 0x2500,
            len: (buflen - 24) as _,
            buf: buf.split_off(20).split_to(buflen - 20 - 4).freeze(),
        };
        let item: Sitemty<ChannelEvents> = decode_frame(&frame).unwrap();
        let item = if let Ok(x) = item { x } else { panic!() };
        let item = if let StreamItem::DataItem(x) = item {
            x
        } else {
            panic!()
        };
        let item = if let RangeCompletableItem::Data(x) = item {
            x
        } else {
            panic!()
        };
        let mut item = if let ChannelEvents::Events(x) = item {
            x
        } else {
            panic!()
        };
        let item = if let Some(item) = item.as_any_mut().downcast_mut::<EventsDim0<f32>>() {
            item
        } else {
            panic!()
        };
        assert_eq!(item.tss(), &[123]);
        #[cfg(DISABLED)]
        {
            eprintln!("NOW WE SEE: {:?}", item);
            // type_name_of_val alloc::boxed::Box<dyn items_0::Events>
            eprintln!("0 {:22?}", item.as_any_mut().type_id());
            eprintln!("A {:22?}", std::any::TypeId::of::<Box<dyn items_0::Events>>());
            eprintln!("B {:22?}", std::any::TypeId::of::<dyn items_0::Events>());
            eprintln!("C {:22?}", std::any::TypeId::of::<&dyn items_0::Events>());
            eprintln!("D {:22?}", std::any::TypeId::of::<&mut dyn items_0::Events>());
            eprintln!("E {:22?}", std::any::TypeId::of::<&mut Box<dyn items_0::Events>>());
            eprintln!("F {:22?}", std::any::TypeId::of::<Box<EventsDim0<f32>>>());
            eprintln!("G {:22?}", std::any::TypeId::of::<&EventsDim0<f32>>());
            eprintln!("H {:22?}", std::any::TypeId::of::<&mut EventsDim0<f32>>());
            eprintln!("I {:22?}", std::any::TypeId::of::<Box<Box<EventsDim0<f32>>>>());
            //let item = item.as_mut();
            //eprintln!("1 {:22?}", item.type_id());
            /*
            let item = if let Some(item) =
                items_0::collect_s::Collectable::as_any_mut(item).downcast_ref::<Box<EventsDim0<f32>>>()
            {
                item
            } else {
                panic!()
            };
            */
            //eprintln!("Final value: {item:?}");
        }
    }
}

#[cfg(test)]
mod test_serde_opt {
    use super::*;

    #[derive(Serialize)]
    struct A {
        a: Option<String>,
        #[serde(default)]
        b: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        c: Option<String>,
    }

    #[test]
    fn test_a() {
        let s = serde_json::to_string(&A {
            a: None,
            b: None,
            c: None,
        })
        .unwrap();
        assert_eq!(s, r#"{"a":null,"b":null}"#);
    }
}

#[test]
fn overlap_info_00() {
    let mut ev1 = EventsDim0::empty();
    ev1.push(MS * 1200, 3, 1.2f32);
    ev1.push(MS * 3200, 3, 3.2f32);
    let range = SeriesRange::TimeRange(NanoRange {
        beg: MS * 1000,
        end: MS * 2000,
    });
    assert_eq!(ev1.ends_after(&range), true);
}

#[test]
fn overlap_info_01() {
    let mut ev1 = EventsDim0::empty();
    ev1.push(MS * 1200, 3, 1.2f32);
    ev1.push(MS * 1400, 3, 3.2f32);
    let range = SeriesRange::TimeRange(NanoRange {
        beg: MS * 1000,
        end: MS * 2000,
    });
    assert_eq!(ev1.ends_after(&range), false);
}

#[test]
fn binner_00() {
    let mut ev1 = EventsDim0::empty();
    ev1.push(MS * 1200, 3, 1.2f32);
    ev1.push(MS * 3200, 3, 3.2f32);
    let binrange = BinnedRangeEnum::from_custom(TsNano(SEC), 0, 10);
    let mut binner = ev1.time_binner_new(binrange, true);
    binner.ingest(ev1.as_time_binnable_mut());
    eprintln!("{:?}", binner);
    // TODO add actual asserts
}

#[test]
fn binner_01() {
    let mut ev1 = EventsDim0::empty();
    ev1.push(MS * 1200, 3, 1.2f32);
    ev1.push(MS * 1300, 3, 1.3);
    ev1.push(MS * 2100, 3, 2.1);
    ev1.push(MS * 2300, 3, 2.3);
    let binrange = BinnedRangeEnum::from_custom(TsNano(SEC), 0, 10);
    let mut binner = ev1.time_binner_new(binrange, true);
    binner.ingest(ev1.as_time_binnable_mut());
    eprintln!("{:?}", binner);
    // TODO add actual asserts
}

/*
TODO adapt and enable
#[test]
fn bin_binned_01() {
    use binsdim0::MinMaxAvgDim0Bins;
    let edges = vec![SEC * 1000, SEC * 1010, SEC * 1020, SEC * 1030];
    let inp0 = <MinMaxAvgDim0Bins<u32> as NewEmpty>::empty(Shape::Scalar);
    let mut time_binner = inp0.time_binner_new(edges, true);
    let inp1 = MinMaxAvgDim0Bins::<u32> {
        ts1s: vec![SEC * 1000, SEC * 1010],
        ts2s: vec![SEC * 1010, SEC * 1020],
        counts: vec![1, 1],
        mins: vec![3, 4],
        maxs: vec![10, 9],
        avgs: vec![7., 6.],
    };
    assert_eq!(time_binner.bins_ready_count(), 0);
    time_binner.ingest(&inp1);
    assert_eq!(time_binner.bins_ready_count(), 1);
    time_binner.push_in_progress(false);
    assert_eq!(time_binner.bins_ready_count(), 2);
    // From here on, pushing any more should not change the bin count:
    time_binner.push_in_progress(false);
    assert_eq!(time_binner.bins_ready_count(), 2);
    // On the other hand, cycling should add one more zero-bin:
    time_binner.cycle();
    assert_eq!(time_binner.bins_ready_count(), 3);
    time_binner.cycle();
    assert_eq!(time_binner.bins_ready_count(), 3);
    let bins = time_binner.bins_ready().expect("bins should be ready");
    eprintln!("bins: {:?}", bins);
    assert_eq!(time_binner.bins_ready_count(), 0);
    assert_eq!(bins.counts(), &[1, 1, 0]);
    // TODO use proper float-compare logic:
    assert_eq!(bins.mins(), &[3., 4., 0.]);
    assert_eq!(bins.maxs(), &[10., 9., 0.]);
    assert_eq!(bins.avgs(), &[7., 6., 0.]);
}

#[test]
fn bin_binned_02() {
    use binsdim0::MinMaxAvgDim0Bins;
    let edges = vec![SEC * 1000, SEC * 1020];
    let inp0 = <MinMaxAvgDim0Bins<u32> as NewEmpty>::empty(Shape::Scalar);
    let mut time_binner = inp0.time_binner_new(edges, true);
    let inp1 = MinMaxAvgDim0Bins::<u32> {
        ts1s: vec![SEC * 1000, SEC * 1010],
        ts2s: vec![SEC * 1010, SEC * 1020],
        counts: vec![1, 1],
        mins: vec![3, 4],
        maxs: vec![10, 9],
        avgs: vec![7., 6.],
    };
    assert_eq!(time_binner.bins_ready_count(), 0);
    time_binner.ingest(&inp1);
    assert_eq!(time_binner.bins_ready_count(), 0);
    time_binner.cycle();
    assert_eq!(time_binner.bins_ready_count(), 1);
    time_binner.cycle();
    //assert_eq!(time_binner.bins_ready_count(), 2);
    let bins = time_binner.bins_ready().expect("bins should be ready");
    eprintln!("bins: {:?}", bins);
    assert_eq!(time_binner.bins_ready_count(), 0);
    assert_eq!(bins.counts(), &[2]);
    assert_eq!(bins.mins(), &[3.]);
    assert_eq!(bins.maxs(), &[10.]);
    assert_eq!(bins.avgs(), &[13. / 2.]);
}
*/

#[test]
fn events_timebin_ingest_continuous_00() {
    let binrange = BinnedRangeEnum::Time(BinnedRange {
        bin_len: TsNano(SEC * 2),
        bin_off: 9,
        bin_cnt: 20,
    });
    let do_time_weight = true;
    let mut bins = EventsDim0::<u32>::empty();
    bins.push(SEC * 20, 1, 20);
    bins.push(SEC * 23, 2, 23);
    let mut binner = bins.as_time_binnable_ref().time_binner_new(binrange, do_time_weight);
    binner.ingest(&mut bins);
    //binner.push_in_progress(true);
    let ready = binner.bins_ready();
    let got = ready.unwrap();
    let got: &BinsDim0<u32> = got.as_any_ref().downcast_ref().unwrap();
    let mut exp = BinsDim0::empty();
    exp.push(SEC * 18, SEC * 20, 0, 0, 0, 0.);
    exp.push(SEC * 20, SEC * 22, 1, 20, 20, 20.);
    assert!(f32_iter_cmp_near(got.avgs.clone(), exp.avgs.clone(), 0.0001, 0.0001));
}
