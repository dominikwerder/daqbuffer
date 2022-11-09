use crate::binsdim0::BinsDim0;
use crate::streams::{CollectableType, CollectorType, ToJsonResult};
use crate::{pulse_offs_from_abs, ts_offs_from_abs, RangeOverlapInfo};
use crate::{Empty, Events, ScalarOps, WithLen};
use crate::{TimeBinnable, TimeBinnableType, TimeBinnableTypeAggregator, TimeBinner};
use err::Error;
use netpod::log::*;
use netpod::NanoRange;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::VecDeque;
use std::{fmt, mem};

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
        write!(
            fmt,
            "count {}  ts {:?} .. {:?}  vals {:?} .. {:?}",
            self.tss.len(),
            self.tss.front(),
            self.tss.back(),
            self.values.front(),
            self.values.back(),
        )
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

impl<NTY: ScalarOps> TimeBinnableType for EventsDim0<NTY> {
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

pub struct EventsDim0Collector<NTY> {
    vals: EventsDim0<NTY>,
    range_complete: bool,
    timed_out: bool,
    #[allow(unused)]
    bin_count_exp: u32,
}

impl<NTY> EventsDim0Collector<NTY> {
    pub fn new(bin_count_exp: u32) -> Self {
        Self {
            vals: EventsDim0::empty(),
            range_complete: false,
            timed_out: false,
            bin_count_exp,
        }
    }
}

impl<NTY> WithLen for EventsDim0Collector<NTY> {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

#[derive(Debug, Serialize)]
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
    values: VecDeque<NTY>,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "finalisedRange")]
    range_complete: bool,
    #[serde(skip_serializing_if = "crate::bool_is_false", rename = "timedOut")]
    timed_out: bool,
}

impl<NTY: ScalarOps> ToJsonResult for EventsDim0CollectorOutput<NTY> {
    fn to_json_result(&self) -> Result<Box<dyn crate::streams::ToJsonBytes>, Error> {
        let k = serde_json::to_value(self)?;
        Ok(Box::new(k))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<NTY: ScalarOps> CollectorType for EventsDim0Collector<NTY> {
    type Input = EventsDim0<NTY>;
    type Output = EventsDim0CollectorOutput<NTY>;

    fn ingest(&mut self, src: &mut Self::Input) {
        // TODO could be optimized by non-contiguous container.
        self.vals.tss.append(&mut src.tss);
        self.vals.pulses.append(&mut src.pulses);
        self.vals.values.append(&mut src.values);
    }

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(&mut self) -> Result<Self::Output, Error> {
        // TODO require contiguous slices
        let tst = ts_offs_from_abs(&self.vals.tss.as_slices().0);
        let (pulse_anchor, pulse_off) = pulse_offs_from_abs(&self.vals.pulses.as_slices().0);
        let ret = Self::Output {
            ts_anchor_sec: tst.0,
            ts_off_ms: tst.1,
            ts_off_ns: tst.2,
            pulse_anchor,
            pulse_off: pulse_off,
            values: mem::replace(&mut self.vals.values, VecDeque::new()),
            range_complete: self.range_complete,
            timed_out: self.timed_out,
        };
        Ok(ret)
    }
}

impl<NTY: ScalarOps> CollectableType for EventsDim0<NTY> {
    type Collector = EventsDim0Collector<NTY>;

    fn new_collector(bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new(bin_count_exp)
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
            min: NTY::zero(),
            max: NTY::zero(),
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
        let vf = val.as_prim_f32();
        self.apply_min_max(val);
        if vf.is_nan() {
        } else {
            self.sum += vf;
            self.sumc += 1;
        }
    }

    fn apply_event_time_weight(&mut self, ts: u64) {
        if let Some(v) = &self.last_seen_val {
            let vf = v.as_prim_f32();
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
                None => NTY::zero(),
            };
            (g.clone(), g.clone(), g.as_prim_f32())
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
                None => NTY::zero(),
            };
            (g.clone(), g.clone(), g.as_prim_f32())
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
        self.min = NTY::zero();
        self.max = NTY::zero();
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

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn to_box_to_json_result(&self) -> Box<dyn ToJsonResult> {
        let k = serde_json::to_value(self).unwrap();
        Box::new(k) as _
    }
}

impl<NTY: ScalarOps> Events for EventsDim0<NTY> {
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

    fn as_collectable_mut(&mut self) -> &mut dyn crate::streams::Collectable {
        self
    }

    fn take_new_events_until_ts(&mut self, ts_end: u64) -> Box<dyn Events> {
        let n1 = self.tss.iter().take_while(|&&x| x < ts_end).count();
        let tss = self.tss.drain(..n1).collect();
        let pulses = self.pulses.drain(..n1).collect();
        let values = self.values.drain(..n1).collect();
        let ret = Self { tss, pulses, values };
        Box::new(ret)
    }

    fn ts_min(&self) -> Option<u64> {
        self.tss.front().map(|&x| x)
    }

    fn ts_max(&self) -> Option<u64> {
        self.tss.back().map(|&x| x)
    }

    fn partial_eq_dyn(&self, other: &dyn Events) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<Self>() {
            self == other
        } else {
            false
        }
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

    fn bins_ready(&mut self) -> Option<Box<dyn crate::TimeBinned>> {
        match self.ready.take() {
            Some(k) => Some(Box::new(k)),
            None => None,
        }
    }

    fn ingest(&mut self, item: &dyn TimeBinnable) {
        let self_name = std::any::type_name::<Self>();
        if true {
            trace!(
                "TimeBinner for EventsDim0TimeBinner   {:?}\n{:?}\n------------------------------------",
                self.edges.iter().take(2).collect::<Vec<_>>(),
                item
            );
        }
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
                        .as_any()
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
}
