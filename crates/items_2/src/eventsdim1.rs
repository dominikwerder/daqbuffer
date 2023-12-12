use crate::binsdim0::BinsDim0;
use crate::eventsxbindim0::EventsXbinDim0;
use crate::framable::FrameType;
use crate::ts_offs_from_abs_with_anchor;
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
use items_0::scalar_ops::ScalarOps;
use items_0::timebin::TimeBinnable;
use items_0::timebin::TimeBinned;
use items_0::timebin::TimeBinner;
use items_0::Appendable;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Empty;
use items_0::Events;
use items_0::EventsNonObj;
use items_0::MergeError;
use items_0::WithLen;
use netpod::is_false;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::MS;
use netpod::timeunits::SEC;
use netpod::BinnedRangeEnum;
use serde::Deserialize;
use serde::Serialize;
use std::any;
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
pub struct EventsDim1<NTY> {
    pub tss: VecDeque<u64>,
    pub pulses: VecDeque<u64>,
    pub values: VecDeque<Vec<NTY>>,
}

impl<NTY> EventsDim1<NTY> {
    #[inline(always)]
    pub fn push(&mut self, ts: u64, pulse: u64, value: Vec<NTY>) {
        self.tss.push_back(ts);
        self.pulses.push_back(pulse);
        self.values.push_back(value);
    }

    #[inline(always)]
    pub fn push_front(&mut self, ts: u64, pulse: u64, value: Vec<NTY>) {
        self.tss.push_front(ts);
        self.pulses.push_front(pulse);
        self.values.push_front(value);
    }

    pub fn serde_id() -> &'static str {
        "EventsDim1"
    }

    pub fn tss(&self) -> &VecDeque<u64> {
        &self.tss
    }
}

impl<NTY> AsAnyRef for EventsDim1<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<NTY> AsAnyMut for EventsDim1<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<NTY> Empty for EventsDim1<NTY> {
    fn empty() -> Self {
        Self {
            tss: VecDeque::new(),
            pulses: VecDeque::new(),
            values: VecDeque::new(),
        }
    }
}

impl<NTY> fmt::Debug for EventsDim1<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        if false {
            write!(
                fmt,
                "EventsDim1 {{ count {}  ts {:?}  vals {:?} }}",
                self.tss.len(),
                self.tss.iter().map(|x| x / SEC).collect::<Vec<_>>(),
                self.values,
            )
        } else {
            write!(
                fmt,
                "EventsDim1 {{ count {}  ts {:?} .. {:?}  vals {:?} .. {:?} }}",
                self.tss.len(),
                self.tss.front().map(|x| x / SEC),
                self.tss.back().map(|x| x / SEC),
                self.values.front(),
                self.values.back(),
            )
        }
    }
}

impl<NTY> WithLen for EventsDim1<NTY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<STY> ByteEstimate for EventsDim1<STY> {
    fn byte_estimate(&self) -> u64 {
        let stylen = mem::size_of::<STY>();
        let n = self.values.front().map_or(0, Vec::len);
        (self.len() * (8 + 8 + n * stylen)) as u64
    }
}

impl<STY: ScalarOps> HasTimestampDeque for EventsDim1<STY> {
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

items_0::impl_range_overlap_info_events!(EventsDim1);

impl<NTY> TimeBinnableType for EventsDim1<NTY>
where
    NTY: ScalarOps,
{
    // TODO
    type Output = BinsDim0<NTY>;
    type Aggregator = EventsDim1Aggregator<NTY>;

    fn aggregator(range: SeriesRange, x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        let self_name = std::any::type_name::<Self>();
        debug!(
            "TimeBinnableType for {self_name}  aggregator()  range {:?}  x_bin_count {}  do_time_weight {}",
            range, x_bin_count, do_time_weight
        );
        Self::Aggregator::new(range, do_time_weight)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsDim1ChunkOutput<STY> {
    tss: VecDeque<u64>,
    pulses: VecDeque<u64>,
    values: VecDeque<Vec<STY>>,
    scalar_type: String,
}

impl<STY: ScalarOps> EventsDim1ChunkOutput<STY> {}

#[derive(Debug)]
pub struct EventsDim1Collector<STY> {
    vals: EventsDim1<STY>,
    range_final: bool,
    timed_out: bool,
}

impl<STY> EventsDim1Collector<STY> {
    pub fn self_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            vals: EventsDim1::empty(),
            range_final: false,
            timed_out: false,
        }
    }
}

impl<STY> WithLen for EventsDim1Collector<STY> {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsDim1CollectorOutput<STY> {
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
    values: VecDeque<Vec<STY>>,
    #[serde(rename = "rangeFinal", default, skip_serializing_if = "is_false")]
    range_final: bool,
    #[serde(rename = "timedOut", default, skip_serializing_if = "is_false")]
    timed_out: bool,
    #[serde(rename = "continueAt", default, skip_serializing_if = "Option::is_none")]
    continue_at: Option<IsoDateTime>,
}

impl<STY: ScalarOps> EventsDim1CollectorOutput<STY> {
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
    pub fn values_to_f32(&self) -> VecDeque<Vec<f32>> {
        self.values
            .iter()
            .map(|x| x.iter().map(|x| x.as_prim_f32_b()).collect())
            .collect()
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

impl<NTY> AsAnyRef for EventsDim1CollectorOutput<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<NTY> AsAnyMut for EventsDim1CollectorOutput<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<NTY: ScalarOps> WithLen for EventsDim1CollectorOutput<NTY> {
    fn len(&self) -> usize {
        self.values.len()
    }
}

impl<NTY: ScalarOps> ToJsonResult for EventsDim1CollectorOutput<NTY> {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        let k = serde_json::to_value(self)?;
        Ok(Box::new(k))
    }
}

impl<NTY: ScalarOps> Collected for EventsDim1CollectorOutput<NTY> {}

impl<STY: ScalarOps> CollectorType for EventsDim1Collector<STY> {
    type Input = EventsDim1<STY>;
    type Output = EventsDim1CollectorOutput<STY>;

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

    // TODO unify with dim0 case
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

impl<STY: ScalarOps> CollectableType for EventsDim1<STY> {
    type Collector = EventsDim1Collector<STY>;

    fn new_collector() -> Self::Collector {
        Self::Collector::new()
    }
}

#[derive(Debug)]
pub struct EventsDim1Aggregator<NTY> {
    range: SeriesRange,
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

impl<NTY> Drop for EventsDim1Aggregator<NTY> {
    fn drop(&mut self) {
        // TODO collect as stats for the request context:
        trace!(
            "taken {}  ignored {}",
            self.events_taken_count,
            self.events_ignored_count
        );
    }
}

impl<NTY: ScalarOps> EventsDim1Aggregator<NTY> {
    pub fn new(range: SeriesRange, do_time_weight: bool) -> Self {
        /*let int_ts = range.beg;
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
        }*/
        todo!()
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
        /*if let Some(v) = &self.last_seen_val {
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
        }*/
        todo!()
    }

    fn ingest_unweight(&mut self, item: &<Self as TimeBinnableTypeAggregator>::Input) {
        /*trace!("TODO check again result_reset_unweight");
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
                error!("TODO ingest_unweight");
                err::todo();
                //self.apply_event_unweight(val);
                self.count += 1;
                self.events_taken_count += 1;
            }
        }*/
        todo!()
    }

    fn ingest_time_weight(&mut self, item: &<Self as TimeBinnableTypeAggregator>::Input) {
        /*let self_name = std::any::type_name::<Self>();
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
                error!("TODO ingest_time_weight");
                err::todo();
                //self.last_seen_val = Some(val);
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
                    // TODO: self.apply_min_max(val.clone());
                }
                self.apply_event_time_weight(ts);
                self.count += 1;
                self.last_seen_ts = ts;
                error!("TODO ingest_time_weight");
                err::todo();
                //self.last_seen_val = Some(val);
                self.events_taken_count += 1;
            }
        }*/
        todo!()
    }

    fn result_reset_unweight(&mut self, range: SeriesRange, _expand: bool) -> BinsDim0<NTY> {
        /*trace!("TODO check again result_reset_unweight");
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
        ret*/
        todo!()
    }

    fn result_reset_time_weight(&mut self, range: SeriesRange, expand: bool) -> BinsDim0<NTY> {
        // TODO check callsite for correct expand status.
        /*if expand {
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
        ret*/
        todo!()
    }
}

impl<NTY: ScalarOps> TimeBinnableTypeAggregator for EventsDim1Aggregator<NTY> {
    type Input = EventsDim1<NTY>;
    type Output = BinsDim0<NTY>;

    fn range(&self) -> &SeriesRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        if true {
            trace!("{} ingest {} events", any::type_name::<Self>(), item.len());
        }
        if false {
            for (i, &ts) in item.tss.iter().enumerate() {
                trace!("{} ingest  {:6}  {:20}", any::type_name::<Self>(), i, ts);
            }
        }
        if self.do_time_weight {
            self.ingest_time_weight(item)
        } else {
            self.ingest_unweight(item)
        }
    }

    fn result_reset(&mut self, range: SeriesRange) -> Self::Output {
        /*trace!("result_reset  {}  {}", range.beg, range.end);
        if self.do_time_weight {
            self.result_reset_time_weight(range, expand)
        } else {
            self.result_reset_unweight(range, expand)
        }*/
        todo!()
    }
}

impl<NTY: ScalarOps> TimeBinnable for EventsDim1<NTY> {
    fn time_binner_new(&self, binrange: BinnedRangeEnum, do_time_weight: bool) -> Box<dyn TimeBinner> {
        let ret = EventsDim1TimeBinner::<NTY>::new(binrange, do_time_weight).unwrap();
        Box::new(ret)
    }

    fn to_box_to_json_result(&self) -> Box<dyn ToJsonResult> {
        let k = serde_json::to_value(self).unwrap();
        Box::new(k) as _
    }
}

impl<STY> items_0::TypeName for EventsDim1<STY> {
    fn type_name(&self) -> String {
        let sty = std::any::type_name::<STY>();
        format!("EventsDim1<{sty}>")
    }
}

impl<STY: ScalarOps> EventsNonObj for EventsDim1<STY> {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>) {
        info!(
            "EventsDim1::into_tss_pulses  len {}  len {}",
            self.tss.len(),
            self.pulses.len()
        );
        (self.tss, self.pulses)
    }
}

impl<STY: ScalarOps> Events for EventsDim1<STY> {
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
        // TODO make more nice
        panic!()
    }

    fn to_min_max_avg(&mut self) -> Box<dyn Events> {
        let mins = self
            .values
            .iter()
            .map(|x| STY::find_vec_min(x))
            .map(|x| x.unwrap_or_else(|| STY::zero_b()))
            .collect();
        let maxs = self
            .values
            .iter()
            .map(|x| STY::find_vec_max(x))
            .map(|x| x.unwrap_or_else(|| STY::zero_b()))
            .collect();
        let avgs = self
            .values
            .iter()
            .map(|x| STY::avg_vec(x))
            .map(|x| x.unwrap_or_else(|| STY::zero_b()))
            .map(|x| x.as_prim_f32_b())
            .collect();
        let item = EventsXbinDim0 {
            tss: mem::replace(&mut self.tss, VecDeque::new()),
            pulses: mem::replace(&mut self.pulses, VecDeque::new()),
            mins,
            maxs,
            avgs,
        };
        Box::new(item)
    }

    fn to_cbor_vec_u8(&self) -> Vec<u8> {
        let ret = EventsDim1ChunkOutput {
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
pub struct EventsDim1TimeBinner<NTY: ScalarOps> {
    edges: VecDeque<u64>,
    agg: EventsDim1Aggregator<NTY>,
    ready: Option<<EventsDim1Aggregator<NTY> as TimeBinnableTypeAggregator>::Output>,
    range_complete: bool,
}

impl<NTY: ScalarOps> EventsDim1TimeBinner<NTY> {
    fn new(binrange: BinnedRangeEnum, do_time_weight: bool) -> Result<Self, Error> {
        /*if edges.len() < 2 {
            return Err(Error::with_msg_no_trace(format!("need at least 2 edges")));
        }
        let self_name = std::any::type_name::<Self>();
        trace!("{self_name}::new  edges {edges:?}");
        let agg = EventsDim1Aggregator::new(
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
        Ok(ret)*/
        todo!()
    }

    fn next_bin_range(&mut self) -> Option<SeriesRange> {
        /*let self_name = std::any::type_name::<Self>();
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
        }*/
        todo!()
    }
}

impl<NTY: ScalarOps> TimeBinner for EventsDim1TimeBinner<NTY> {
    fn bins_ready_count(&self) -> usize {
        match &self.ready {
            Some(k) => k.len(),
            None => 0,
        }
    }

    fn bins_ready(&mut self) -> Option<Box<dyn TimeBinned>> {
        match self.ready.take() {
            Some(k) => Some(Box::new(k)),
            None => None,
        }
    }

    fn ingest(&mut self, item: &mut dyn TimeBinnable) {
        /*let self_name = std::any::type_name::<Self>();
        trace2!(
            "TimeBinner for EventsDim1TimeBinner   {:?}\n{:?}\n------------------------------------",
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
                        .downcast_ref::<<EventsDim1Aggregator<NTY> as TimeBinnableTypeAggregator>::Input>()
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
        }*/
        todo!()
    }

    fn push_in_progress(&mut self, push_empty: bool) {
        /*let self_name = std::any::type_name::<Self>();
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
        }*/
        todo!()
    }

    fn cycle(&mut self) {
        /*let self_name = std::any::type_name::<Self>();
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
        }*/
        todo!()
    }

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn empty(&self) -> Box<dyn TimeBinned> {
        let ret = <EventsDim1Aggregator<NTY> as TimeBinnableTypeAggregator>::Output::empty();
        Box::new(ret)
    }

    fn append_empty_until_end(&mut self) {
        // nothing to do for events
    }
}

impl<STY> Appendable<Vec<STY>> for EventsDim1<STY>
where
    STY: ScalarOps,
{
    fn push(&mut self, ts: u64, pulse: u64, value: Vec<STY>) {
        Self::push(self, ts, pulse, value)
    }
}
