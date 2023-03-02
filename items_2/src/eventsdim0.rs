use crate::binsdim0::BinsDim0;
use crate::IsoDateTime;
use crate::RangeOverlapInfo;
use crate::TimeBinnable;
use crate::TimeBinnableType;
use crate::TimeBinnableTypeAggregator;
use crate::TimeBinner;
use err::Error;
use items_0::Appendable;
use items_0::scalar_ops::ScalarOps;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Empty;
use items_0::Events;
use items_0::EventsNonObj;
use items_0::WithLen;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::BinnedRange;
use netpod::NanoRange;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::mem;

#[allow(unused)]
macro_rules! trace2 {
    (EN$($arg:tt)*) => ();
    ($($arg:tt)*) => (trace!($($arg)*));
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsDim0<NTY> {
    pub tss: VecDeque<u64>,
    pub pulses: VecDeque<u64>,
    pub values: VecDeque<NTY>,
}

impl<NTY> EventsDim0<NTY> {
    #[inline(always)]
    pub fn push(&mut self, ts: u64, pulse: u64, value: NTY) {
        self.tss.push_back(ts);
        self.pulses.push_back(pulse);
        self.values.push_back(value);
    }

    #[inline(always)]
    pub fn push_front(&mut self, ts: u64, pulse: u64, value: NTY) {
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

impl<NTY> AsAnyRef for EventsDim0<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<NTY> AsAnyMut for EventsDim0<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<NTY> Empty for EventsDim0<NTY> {
    fn empty() -> Self {
        Self {
            tss: VecDeque::new(),
            pulses: VecDeque::new(),
            values: VecDeque::new(),
        }
    }
}

impl<NTY> fmt::Debug for EventsDim0<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        if false {
            write!(
                fmt,
                "EventsDim0 {{ count {}  ts {:?}  vals {:?} }}",
                self.tss.len(),
                self.tss.iter().map(|x| x / SEC).collect::<Vec<_>>(),
                self.values,
            )
        } else {
            write!(
                fmt,
                "EventsDim0 {{ count {}  ts {:?} .. {:?}  vals {:?} .. {:?} }}",
                self.tss.len(),
                self.tss.front().map(|x| x / SEC),
                self.tss.back().map(|x| x / SEC),
                self.values.front(),
                self.values.back(),
            )
        }
    }
}

impl<NTY> WithLen for EventsDim0<NTY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<NTY: ScalarOps> RangeOverlapInfo for EventsDim0<NTY> {
    fn ends_before(&self, range: NanoRange) -> bool {
        if let Some(&max) = self.tss.back() {
            max < range.beg
        } else {
            true
        }
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        if let Some(&max) = self.tss.back() {
            max >= range.end
        } else {
            true
        }
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        if let Some(&min) = self.tss.front() {
            min >= range.end
        } else {
            true
        }
    }
}

impl<NTY> TimeBinnableType for EventsDim0<NTY>
where
    NTY: ScalarOps,
{
    type Output = BinsDim0<NTY>;
    type Aggregator = EventsDim0Aggregator<NTY>;

    fn aggregator(range: NanoRange, x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        let self_name = std::any::type_name::<Self>();
        debug!(
            "TimeBinnableType for {self_name}  aggregator()  range {:?}  x_bin_count {}  do_time_weight {}",
            range, x_bin_count, do_time_weight
        );
        Self::Aggregator::new(range, do_time_weight)
    }
}

#[derive(Debug)]
pub struct EventsDim0Collector<NTY> {
    vals: EventsDim0<NTY>,
    range_final: bool,
    timed_out: bool,
}

impl<NTY> EventsDim0Collector<NTY> {
    pub fn new() -> Self {
        Self {
            vals: EventsDim0::empty(),
            range_final: false,
            timed_out: false,
        }
    }
}

impl<NTY> WithLen for EventsDim0Collector<NTY> {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsDim0CollectorOutput<NTY> {
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
    values: VecDeque<NTY>,
    #[serde(rename = "rangeFinal", default, skip_serializing_if = "crate::bool_is_false")]
    range_final: bool,
    #[serde(rename = "timedOut", default, skip_serializing_if = "crate::bool_is_false")]
    timed_out: bool,
    #[serde(rename = "continueAt", default, skip_serializing_if = "Option::is_none")]
    continue_at: Option<IsoDateTime>,
}

impl<NTY: ScalarOps> EventsDim0CollectorOutput<NTY> {
    pub fn len(&self) -> usize {
        self.values.len()
    }

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

    pub fn range_complete(&self) -> bool {
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

impl<NTY> AsAnyRef for EventsDim0CollectorOutput<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<NTY> AsAnyMut for EventsDim0CollectorOutput<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<NTY: ScalarOps> items_0::collect_s::ToJsonResult for EventsDim0CollectorOutput<NTY> {
    fn to_json_result(&self) -> Result<Box<dyn items_0::collect_s::ToJsonBytes>, Error> {
        let k = serde_json::to_value(self)?;
        Ok(Box::new(k))
    }
}

impl<NTY: ScalarOps> items_0::collect_c::Collected for EventsDim0CollectorOutput<NTY> {}

impl<NTY: ScalarOps> items_0::collect_s::CollectorType for EventsDim0Collector<NTY> {
    type Input = EventsDim0<NTY>;
    type Output = EventsDim0CollectorOutput<NTY>;

    fn ingest(&mut self, src: &mut Self::Input) {
        self.vals.tss.append(&mut src.tss);
        self.vals.pulses.append(&mut src.pulses);
        self.vals.values.append(&mut src.values);
        if self.len() >= 2 {
            let mut print = false;
            let c = self.vals.tss.len();
            if self.vals.tss[c - 2] + 1000000000 <= self.vals.tss[c - 1] {
                print = true;
            }
            let c = self.vals.pulses.len();
            if self.vals.pulses[c - 2] + 1000 <= self.vals.pulses[c - 1] {
                print = true;
            }
            if print {
                error!("gap detected\n{self:?}");
                let bt = std::backtrace::Backtrace::capture();
                error!("{bt}");
            }
        }
    }

    fn set_range_complete(&mut self) {
        self.range_final = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(&mut self, range: Option<NanoRange>, _binrange: Option<BinnedRange>) -> Result<Self::Output, Error> {
        // If we timed out, we want to hint the client from where to continue.
        // This is tricky: currently, client can not request a left-exclusive range.
        // We currently give the timestamp of the last event plus a small delta.
        // The amount of the delta must take into account what kind of timestamp precision the client
        // can parse and handle.
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
        let tss_sl = self.vals.tss.make_contiguous();
        let pulses_sl = self.vals.pulses.make_contiguous();
        let (ts_anchor_sec, ts_off_ms, ts_off_ns) = crate::ts_offs_from_abs(tss_sl);
        let (pulse_anchor, pulse_off) = crate::pulse_offs_from_abs(pulses_sl);
        let values = mem::replace(&mut self.vals.values, VecDeque::new());
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

impl<NTY: ScalarOps> items_0::collect_s::CollectableType for EventsDim0<NTY> {
    type Collector = EventsDim0Collector<NTY>;

    fn new_collector() -> Self::Collector {
        Self::Collector::new()
    }
}

impl<NTY: ScalarOps> items_0::collect_c::Collector for EventsDim0Collector<NTY> {
    fn len(&self) -> usize {
        self.vals.len()
    }

    fn ingest(&mut self, item: &mut dyn items_0::collect_c::Collectable) {
        if let Some(item) = item.as_any_mut().downcast_mut::<EventsDim0<NTY>>() {
            items_0::collect_s::CollectorType::ingest(self, item)
        } else {
            error!("EventsDim0Collector::ingest unexpected item {:?}", item);
        }
    }

    fn set_range_complete(&mut self) {
        items_0::collect_s::CollectorType::set_range_complete(self)
    }

    fn set_timed_out(&mut self) {
        items_0::collect_s::CollectorType::set_timed_out(self)
    }

    fn result(
        &mut self,
        range: Option<NanoRange>,
        binrange: Option<BinnedRange>,
    ) -> Result<Box<dyn items_0::collect_c::Collected>, err::Error> {
        match items_0::collect_s::CollectorType::result(self, range, binrange) {
            Ok(x) => Ok(Box::new(x)),
            Err(e) => Err(e.into()),
        }
    }
}

pub struct EventsDim0Aggregator<NTY> {
    range: NanoRange,
    count: u64,
    min: NTY,
    max: NTY,
    sumc: u64,
    sum: f32,
    int_ts: u64,
    last_seen_ts: u64,
    last_seen_val: Option<NTY>,
    did_min_max: bool,
    do_time_weight: bool,
    events_taken_count: u64,
    events_ignored_count: u64,
}

impl<NTY> Drop for EventsDim0Aggregator<NTY> {
    fn drop(&mut self) {
        // TODO collect as stats for the request context:
        trace!(
            "taken {}  ignored {}",
            self.events_taken_count,
            self.events_ignored_count
        );
    }
}

impl<NTY: ScalarOps> EventsDim0Aggregator<NTY> {
    pub fn new(range: NanoRange, do_time_weight: bool) -> Self {
        let int_ts = range.beg;
        Self {
            range,
            count: 0,
            min: NTY::zero_b(),
            max: NTY::zero_b(),
            sum: 0.,
            sumc: 0,
            int_ts,
            last_seen_ts: 0,
            last_seen_val: None,
            did_min_max: false,
            do_time_weight,
            events_taken_count: 0,
            events_ignored_count: 0,
        }
    }

    // TODO reduce clone.. optimize via more traits to factor the trade-offs?
    fn apply_min_max(&mut self, val: NTY) {
        trace!(
            "apply_min_max  val {:?}  last_val {:?}  count {}  sumc {:?}  min {:?}  max {:?}",
            val,
            self.last_seen_val,
            self.count,
            self.sumc,
            self.min,
            self.max
        );
        if self.did_min_max == false {
            self.did_min_max = true;
            self.min = val.clone();
            self.max = val.clone();
        } else {
            if self.min > val {
                self.min = val.clone();
            }
            if self.max < val {
                self.max = val.clone();
            }
        }
    }

    fn apply_event_unweight(&mut self, val: NTY) {
        trace!("TODO check again result_reset_unweight");
        err::todo();
        let vf = val.as_prim_f32_b();
        self.apply_min_max(val);
        if vf.is_nan() {
        } else {
            self.sum += vf;
            self.sumc += 1;
        }
    }

    fn apply_event_time_weight(&mut self, ts: u64) {
        if let Some(v) = &self.last_seen_val {
            let vf = v.as_prim_f32_b();
            let v2 = v.clone();
            if ts > self.range.beg {
                self.apply_min_max(v2);
            }
            let w = if self.do_time_weight {
                (ts - self.int_ts) as f32 * 1e-9
            } else {
                1.
            };
            if vf.is_nan() {
            } else {
                self.sum += vf * w;
                self.sumc += 1;
            }
            self.int_ts = ts;
        } else {
            debug!(
                "apply_event_time_weight NO VALUE  {}",
                ts as i64 - self.range.beg as i64
            );
        }
    }

    fn ingest_unweight(&mut self, item: &<Self as TimeBinnableTypeAggregator>::Input) {
        trace!("TODO check again result_reset_unweight");
        err::todo();
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let val = item.values[i1].clone();
            if ts < self.range.beg {
                self.events_ignored_count += 1;
            } else if ts >= self.range.end {
                self.events_ignored_count += 1;
                return;
            } else {
                self.apply_event_unweight(val);
                self.count += 1;
                self.events_taken_count += 1;
            }
        }
    }

    fn ingest_time_weight(&mut self, item: &<Self as TimeBinnableTypeAggregator>::Input) {
        let self_name = std::any::type_name::<Self>();
        trace!("{self_name}::ingest_time_weight  item len {}", item.len());
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let val = item.values[i1].clone();
            trace!("{self_name} ingest  {:6}  {:20}  {:10?}", i1, ts, val);
            if ts < self.int_ts {
                if self.last_seen_val.is_none() {
                    info!(
                        "ingest_time_weight event before range, only set last  ts {}  val {:?}",
                        ts, val
                    );
                }
                self.events_ignored_count += 1;
                self.last_seen_ts = ts;
                self.last_seen_val = Some(val);
            } else if ts >= self.range.end {
                self.events_ignored_count += 1;
                return;
            } else {
                if false && self.last_seen_val.is_none() {
                    // TODO no longer needed or?
                    info!(
                        "call apply_min_max without last val, use current instead  {}  {:?}",
                        ts, val
                    );
                    self.apply_min_max(val.clone());
                }
                self.apply_event_time_weight(ts);
                self.count += 1;
                self.last_seen_ts = ts;
                self.last_seen_val = Some(val);
                self.events_taken_count += 1;
            }
        }
    }

    fn result_reset_unweight(&mut self, range: NanoRange, _expand: bool) -> BinsDim0<NTY> {
        trace!("TODO check again result_reset_unweight");
        err::todo();
        let (min, max, avg) = if self.sumc > 0 {
            let avg = self.sum / self.sumc as f32;
            (self.min.clone(), self.max.clone(), avg)
        } else {
            let g = match &self.last_seen_val {
                Some(x) => x.clone(),
                None => NTY::zero_b(),
            };
            (g.clone(), g.clone(), g.as_prim_f32_b())
        };
        let ret = BinsDim0 {
            ts1s: [self.range.beg].into(),
            ts2s: [self.range.end].into(),
            counts: [self.count].into(),
            mins: [min].into(),
            maxs: [max].into(),
            avgs: [avg].into(),
        };
        self.int_ts = range.beg;
        self.range = range;
        self.count = 0;
        self.sum = 0f32;
        self.sumc = 0;
        self.did_min_max = false;
        ret
    }

    fn result_reset_time_weight(&mut self, range: NanoRange, expand: bool) -> BinsDim0<NTY> {
        // TODO check callsite for correct expand status.
        if expand {
            debug!("result_reset_time_weight calls apply_event_time_weight");
            self.apply_event_time_weight(self.range.end);
        } else {
            debug!("result_reset_time_weight NO EXPAND");
        }
        let (min, max, avg) = if self.sumc > 0 {
            let avg = self.sum / (self.range.delta() as f32 * 1e-9);
            (self.min.clone(), self.max.clone(), avg)
        } else {
            let g = match &self.last_seen_val {
                Some(x) => x.clone(),
                None => NTY::zero_b(),
            };
            (g.clone(), g.clone(), g.as_prim_f32_b())
        };
        let ret = BinsDim0 {
            ts1s: [self.range.beg].into(),
            ts2s: [self.range.end].into(),
            counts: [self.count].into(),
            mins: [min].into(),
            maxs: [max].into(),
            avgs: [avg].into(),
        };
        self.int_ts = range.beg;
        self.range = range;
        self.count = 0;
        self.sum = 0.;
        self.sumc = 0;
        self.did_min_max = false;
        self.min = NTY::zero_b();
        self.max = NTY::zero_b();
        ret
    }
}

impl<NTY: ScalarOps> TimeBinnableTypeAggregator for EventsDim0Aggregator<NTY> {
    type Input = EventsDim0<NTY>;
    type Output = BinsDim0<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        if true {
            trace!("{} ingest {} events", std::any::type_name::<Self>(), item.len());
        }
        if false {
            for (i, &ts) in item.tss.iter().enumerate() {
                trace!("{} ingest  {:6}  {:20}", std::any::type_name::<Self>(), i, ts);
            }
        }
        if self.do_time_weight {
            self.ingest_time_weight(item)
        } else {
            self.ingest_unweight(item)
        }
    }

    fn result_reset(&mut self, range: NanoRange, expand: bool) -> Self::Output {
        trace!("result_reset  {}  {}", range.beg, range.end);
        if self.do_time_weight {
            self.result_reset_time_weight(range, expand)
        } else {
            self.result_reset_unweight(range, expand)
        }
    }
}

impl<NTY: ScalarOps> TimeBinnable for EventsDim0<NTY> {
    fn time_binner_new(&self, edges: Vec<u64>, do_time_weight: bool) -> Box<dyn TimeBinner> {
        let ret = EventsDim0TimeBinner::<NTY>::new(edges.into(), do_time_weight).unwrap();
        Box::new(ret)
    }

    fn to_box_to_json_result(&self) -> Box<dyn items_0::collect_s::ToJsonResult> {
        let k = serde_json::to_value(self).unwrap();
        Box::new(k) as _
    }
}

impl<STY> items_0::TypeName for EventsDim0<STY> {
    fn type_name(&self) -> String {
        let sty = std::any::type_name::<STY>();
        format!("EventsDim0<{sty}>")
    }
}

impl<STY: ScalarOps> EventsNonObj for EventsDim0<STY> {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>) {
        info!(
            "EventsDim0::into_tss_pulses  len {}  len {}",
            self.tss.len(),
            self.pulses.len()
        );
        (self.tss, self.pulses)
    }
}

impl<STY: ScalarOps> Events for EventsDim0<STY> {
    fn as_time_binnable(&self) -> &dyn TimeBinnable {
        self as &dyn TimeBinnable
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

    fn as_collectable_mut(&mut self) -> &mut dyn items_0::collect_s::Collectable {
        self
    }

    fn as_collectable_with_default_ref(&self) -> &dyn items_0::collect_c::CollectableWithDefault {
        self
    }

    fn as_collectable_with_default_mut(&mut self) -> &mut dyn items_0::collect_c::CollectableWithDefault {
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

    fn new_empty(&self) -> Box<dyn Events> {
        Box::new(Self::empty())
    }

    fn drain_into(&mut self, dst: &mut Box<dyn Events>, range: (usize, usize)) -> Result<(), items_0::MergeError> {
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
            Err(items_0::MergeError::NotCompatible)
        }
    }

    fn find_lowest_index_gt(&self, ts: u64) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate() {
            if m > ts {
                return Some(i);
            }
        }
        None
    }

    fn find_lowest_index_ge(&self, ts: u64) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate() {
            if m >= ts {
                return Some(i);
            }
        }
        None
    }

    fn find_highest_index_lt(&self, ts: u64) -> Option<usize> {
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
}

pub struct EventsDim0TimeBinner<NTY: ScalarOps> {
    edges: VecDeque<u64>,
    agg: EventsDim0Aggregator<NTY>,
    ready: Option<<EventsDim0Aggregator<NTY> as TimeBinnableTypeAggregator>::Output>,
    range_complete: bool,
}

impl<NTY: ScalarOps> EventsDim0TimeBinner<NTY> {
    fn new(edges: VecDeque<u64>, do_time_weight: bool) -> Result<Self, Error> {
        if edges.len() < 2 {
            return Err(Error::with_msg_no_trace(format!("need at least 2 edges")));
        }
        let self_name = std::any::type_name::<Self>();
        trace!("{self_name}::new  edges {edges:?}");
        let agg = EventsDim0Aggregator::new(
            NanoRange {
                beg: edges[0],
                end: edges[1],
            },
            do_time_weight,
        );
        let ret = Self {
            edges,
            agg,
            ready: None,
            range_complete: false,
        };
        Ok(ret)
    }

    fn next_bin_range(&mut self) -> Option<NanoRange> {
        let self_name = std::any::type_name::<Self>();
        if self.edges.len() >= 3 {
            self.edges.pop_front();
            let ret = NanoRange {
                beg: self.edges[0],
                end: self.edges[1],
            };
            trace!("{self_name}  next_bin_range  {}  {}", ret.beg, ret.end);
            Some(ret)
        } else {
            self.edges.clear();
            trace!("{self_name}  next_bin_range None");
            None
        }
    }
}

impl<NTY: ScalarOps> TimeBinner for EventsDim0TimeBinner<NTY> {
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

    fn ingest(&mut self, item: &dyn TimeBinnable) {
        let self_name = std::any::type_name::<Self>();
        trace2!(
            "TimeBinner for EventsDim0TimeBinner   {:?}\n{:?}\n------------------------------------",
            self.edges.iter().take(2).collect::<Vec<_>>(),
            item
        );
        if item.len() == 0 {
            // Return already here, RangeOverlapInfo would not give much sense.
            return;
        }
        if self.edges.len() < 2 {
            warn!("{self_name}  no more bin in edges A");
            return;
        }
        // TODO optimize by remembering at which event array index we have arrived.
        // That needs modified interfaces which can take and yield the start and latest index.
        loop {
            while item.starts_after(self.agg.range().clone()) {
                trace!("{self_name}  IGNORE ITEM  AND CYCLE  BECAUSE item.starts_after");
                self.cycle();
                if self.edges.len() < 2 {
                    warn!("{self_name}  no more bin in edges B");
                    return;
                }
            }
            if item.ends_before(self.agg.range().clone()) {
                trace!("{self_name}  IGNORE ITEM  BECAUSE ends_before\n-------------      -----------");
                return;
            } else {
                if self.edges.len() < 2 {
                    trace!("{self_name}  edge list exhausted");
                    return;
                } else {
                    if let Some(item) = item
                        .as_any_ref()
                        // TODO make statically sure that we attempt to cast to the correct type here:
                        .downcast_ref::<<EventsDim0Aggregator<NTY> as TimeBinnableTypeAggregator>::Input>()
                    {
                        // TODO collect statistics associated with this request:
                        trace!("{self_name}  FEED THE ITEM...");
                        self.agg.ingest(item);
                        if item.ends_after(self.agg.range().clone()) {
                            trace!("{self_name}  FED ITEM, ENDS AFTER.");
                            self.cycle();
                            if self.edges.len() < 2 {
                                warn!("{self_name}  no more bin in edges C");
                                return;
                            } else {
                                trace!("{self_name}  FED ITEM, CYCLED, CONTINUE.");
                            }
                        } else {
                            trace!("{self_name}  FED ITEM.");
                            break;
                        }
                    } else {
                        panic!("{self_name} not correct item type");
                    };
                }
            }
        }
    }

    fn push_in_progress(&mut self, push_empty: bool) {
        let self_name = std::any::type_name::<Self>();
        trace!("{self_name}::push_in_progress");
        // TODO expand should be derived from AggKind. Is it still required after all?
        // TODO here, the expand means that agg will assume that the current value is kept constant during
        // the rest of the time range.
        if self.edges.len() >= 2 {
            let expand = true;
            let range_next = if let Some(x) = self.next_bin_range() {
                Some(x)
            } else {
                None
            };
            let mut bins = if let Some(range_next) = range_next {
                self.agg.result_reset(range_next, expand)
            } else {
                let range_next = NanoRange {
                    beg: u64::MAX - 1,
                    end: u64::MAX,
                };
                self.agg.result_reset(range_next, expand)
            };
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

    fn cycle(&mut self) {
        let self_name = std::any::type_name::<Self>();
        trace!("{self_name}::cycle");
        // TODO refactor this logic.
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

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn empty(&self) -> Box<dyn items_0::TimeBinned> {
        let ret = <EventsDim0Aggregator<NTY> as TimeBinnableTypeAggregator>::Output::empty();
        Box::new(ret)
    }
}

// TODO remove this struct?
#[derive(Debug)]
pub struct EventsDim0CollectorDyn {}

impl EventsDim0CollectorDyn {
    pub fn new() -> Self {
        Self {}
    }
}

impl items_0::collect_c::CollectorDyn for EventsDim0CollectorDyn {
    fn len(&self) -> usize {
        todo!()
    }

    fn ingest(&mut self, _item: &mut dyn items_0::collect_c::CollectableWithDefault) {
        todo!()
    }

    fn set_range_complete(&mut self) {
        todo!()
    }

    fn set_timed_out(&mut self) {
        todo!()
    }

    fn result(
        &mut self,
        _range: Option<NanoRange>,
        _binrange: Option<BinnedRange>,
    ) -> Result<Box<dyn items_0::collect_c::Collected>, err::Error> {
        todo!()
    }
}

impl<NTY: ScalarOps> items_0::collect_c::CollectorDyn for EventsDim0Collector<NTY> {
    fn len(&self) -> usize {
        WithLen::len(self)
    }

    fn ingest(&mut self, item: &mut dyn items_0::collect_c::CollectableWithDefault) {
        let x = item.as_any_mut();
        if let Some(item) = x.downcast_mut::<EventsDim0<NTY>>() {
            items_0::collect_s::CollectorType::ingest(self, item)
        } else {
            // TODO need possibility to return error
            ()
        }
    }

    fn set_range_complete(&mut self) {
        items_0::collect_s::CollectorType::set_range_complete(self);
    }

    fn set_timed_out(&mut self) {
        items_0::collect_s::CollectorType::set_timed_out(self);
    }

    fn result(
        &mut self,
        range: Option<NanoRange>,
        binrange: Option<BinnedRange>,
    ) -> Result<Box<dyn items_0::collect_c::Collected>, err::Error> {
        items_0::collect_s::CollectorType::result(self, range, binrange)
            .map(|x| Box::new(x) as _)
            .map_err(|e| e.into())
    }
}

impl<NTY: ScalarOps> items_0::collect_c::CollectableWithDefault for EventsDim0<NTY> {
    fn new_collector(&self) -> Box<dyn items_0::collect_c::CollectorDyn> {
        let coll = EventsDim0Collector::<NTY>::new();
        Box::new(coll)
    }
}

impl<NTY: ScalarOps> items_0::collect_c::Collectable for EventsDim0<NTY> {
    fn new_collector(&self) -> Box<dyn items_0::collect_c::Collector> {
        Box::new(EventsDim0Collector::<NTY>::new())
    }
}

impl<STY> Appendable<STY> for EventsDim0<STY>
where
    STY: ScalarOps,
{
    fn push(&mut self, ts: u64, pulse: u64, value: STY) {
        Self::push(self, ts, pulse, value)
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
    fn events_bincode() {
        taskrun::tracing_init().unwrap();
        // core::result::Result<items::StreamItem<items::RangeCompletableItem<items_2::channelevents::ChannelEvents>>, err::Error>
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
