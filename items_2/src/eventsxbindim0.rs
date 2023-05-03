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
use items_0::timebin::TimeBinned;
use items_0::timebin::TimeBinner;
use items_0::timebin::TimeBinnerTy;
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
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
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
macro_rules! trace_ingest {
    ($($arg:tt)*) => {};
    ($($arg:tt)*) => { trace!($($arg)*) };
}

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => {};
    ($($arg:tt)*) => { trace!($($arg)*) };
}

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

impl<STY> fmt::Debug for EventsXbinDim0<STY>
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
                self.avgs,
            )
        } else {
            write!(
                fmt,
                "{} {{ count {}  ts {:?} .. {:?}  vals {:?} .. {:?} }}",
                self.type_name(),
                self.tss.len(),
                self.tss.front().map(|x| x / SEC),
                self.tss.back().map(|x| x / SEC),
                self.avgs.front(),
                self.avgs.back(),
            )
        }
    }
}

impl<STY> ByteEstimate for EventsXbinDim0<STY> {
    fn byte_estimate(&self) -> u64 {
        let stylen = mem::size_of::<STY>();
        (self.len() * (8 + 8 + 2 * stylen + 4)) as u64
    }
}

impl<STY> Empty for EventsXbinDim0<STY> {
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

impl<STY> AsAnyRef for EventsXbinDim0<STY>
where
    STY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<STY> AsAnyMut for EventsXbinDim0<STY>
where
    STY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY> WithLen for EventsXbinDim0<STY> {
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

#[derive(Debug)]
pub struct EventsXbinDim0TimeBinner<STY: ScalarOps> {
    binrange: BinnedRangeEnum,
    rix: usize,
    rng: Option<SeriesRange>,
    agg: EventsXbinDim0Aggregator<STY>,
    ready: Option<<EventsXbinDim0Aggregator<STY> as TimeBinnableTypeAggregator>::Output>,
    range_final: bool,
}

impl<STY: ScalarOps> EventsXbinDim0TimeBinner<STY> {
    fn type_name() -> &'static str {
        any::type_name::<Self>()
    }

    fn new(binrange: BinnedRangeEnum, do_time_weight: bool) -> Result<Self, Error> {
        trace!("{}::new  binrange {:?}", Self::type_name(), binrange);
        let rng = binrange
            .range_at(0)
            .ok_or_else(|| Error::with_msg_no_trace("empty binrange"))?;
        trace!("{}::new  rng {rng:?}", Self::type_name());
        let agg = EventsXbinDim0Aggregator::new(rng, do_time_weight);
        trace!("{}  agg range {:?}", Self::type_name(), agg.range());
        let ret = Self {
            binrange,
            rix: 0,
            rng: Some(agg.range.clone()),
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

impl<STY: ScalarOps> TimeBinner for EventsXbinDim0TimeBinner<STY> {
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
        trace2!(
            "TimeBinner for {} ingest  agg.range {:?}  item {:?}",
            Self::type_name(),
            self.agg.range(),
            item
        );
        if item.len() == 0 {
            // Return already here, RangeOverlapInfo would not give much sense.
            return;
        }
        // TODO optimize by remembering at which event array index we have arrived.
        // That needs modified interfaces which can take and yield the start and latest index.
        loop {
            while item.starts_after(self.agg.range()) {
                trace!(
                    "{}  IGNORE ITEM  AND CYCLE  BECAUSE item.starts_after",
                    Self::type_name()
                );
                self.cycle();
                if self.rng.is_none() {
                    warn!("{}  no more bin in edges B", Self::type_name());
                    return;
                }
            }
            if item.ends_before(self.agg.range()) {
                trace!(
                    "{}  IGNORE ITEM  BECAUSE ends_before  {:?}  {:?}",
                    Self::type_name(),
                    self.agg.range(),
                    item
                );
                return;
            } else {
                if self.rng.is_none() {
                    trace!("{}  no more bin in edges D", Self::type_name());
                    return;
                } else {
                    if let Some(item) = item
                        .as_any_ref()
                        // TODO make statically sure that we attempt to cast to the correct type here:
                        .downcast_ref::<<EventsXbinDim0Aggregator<STY> as TimeBinnableTypeAggregator>::Input>()
                    {
                        // TODO collect statistics associated with this request:
                        trace_ingest!("{}  FEED THE ITEM...", Self::type_name());
                        self.agg.ingest(item);
                        if item.ends_after(self.agg.range()) {
                            trace_ingest!("{}  FED ITEM, ENDS AFTER.", Self::type_name());
                            self.cycle();
                            if self.rng.is_none() {
                                warn!("{}  no more bin in edges C", Self::type_name());
                                return;
                            } else {
                                trace_ingest!("{}  FED ITEM, CYCLED, CONTINUE.", Self::type_name());
                            }
                        } else {
                            trace_ingest!("{}  FED ITEM.", Self::type_name());
                            break;
                        }
                    } else {
                        error!("{}::ingest  unexpected item type", Self::type_name());
                    };
                }
            }
        }
    }

    fn push_in_progress(&mut self, push_empty: bool) {
        trace!("{}::push_in_progress  push_empty {push_empty}", Self::type_name());
        // TODO expand should be derived from AggKind. Is it still required after all?
        // TODO here, the expand means that agg will assume that the current value is kept constant during
        // the rest of the time range.
        if self.rng.is_none() {
        } else {
            let expand = true;
            let range_next = self.next_bin_range();
            trace!("\n+++++\n+++++\n{}  range_next {:?}", Self::type_name(), range_next);
            self.rng = range_next.clone();
            let mut bins = if let Some(range_next) = range_next {
                self.agg.result_reset(range_next, expand)
            } else {
                // Acts as placeholder
                let range_next = NanoRange {
                    beg: u64::MAX - 1,
                    end: u64::MAX,
                };
                self.agg.result_reset(range_next.into(), expand)
            };
            if bins.len() != 1 {
                error!("{}::push_in_progress  bins.len() {}", Self::type_name(), bins.len());
                return;
            } else {
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
    }

    fn cycle(&mut self) {
        trace!("{}::cycle", Self::type_name());
        // TODO refactor this logic.
        let n = self.bins_ready_count();
        self.push_in_progress(true);
        if self.bins_ready_count() == n {
            let range_next = self.next_bin_range();
            self.rng = range_next.clone();
            if let Some(range) = range_next {
                let mut bins = BinsXbinDim0::empty();
                if range.is_time() {
                    bins.append_zero(range.beg_u64(), range.end_u64());
                } else {
                    error!("TODO  {}::cycle  is_pulse", Self::type_name());
                }
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
        self.range_final = true;
    }

    fn empty(&self) -> Box<dyn TimeBinned> {
        let ret = <EventsXbinDim0Aggregator<STY> as TimeBinnableTypeAggregator>::Output::empty();
        Box::new(ret)
    }
}

impl<STY> TimeBinnableType for EventsXbinDim0<STY>
where
    STY: ScalarOps,
{
    type Output = BinsXbinDim0<STY>;
    type Aggregator = EventsXbinDim0Aggregator<STY>;

    fn aggregator(range: SeriesRange, x_bin_count: usize, do_time_weight: bool) -> Self::Aggregator {
        let name = any::type_name::<Self>();
        debug!(
            "TimeBinnableType for {}  aggregator()  range {:?}  x_bin_count {}  do_time_weight {}",
            name, range, x_bin_count, do_time_weight
        );
        Self::Aggregator::new(range, do_time_weight)
    }
}

impl<STY> TimeBinnable for EventsXbinDim0<STY>
where
    STY: ScalarOps,
{
    fn time_binner_new(
        &self,
        binrange: BinnedRangeEnum,
        do_time_weight: bool,
    ) -> Box<dyn items_0::timebin::TimeBinner> {
        let ret = EventsXbinDim0TimeBinner::<STY>::new(binrange, do_time_weight).unwrap();
        Box::new(ret)
    }

    fn to_box_to_json_result(&self) -> Box<dyn ToJsonResult> {
        let k = serde_json::to_value(self).unwrap();
        Box::new(k) as _
    }
}

#[derive(Debug)]
pub struct EventsXbinDim0Aggregator<STY>
where
    STY: ScalarOps,
{
    range: SeriesRange,
    /// Number of events which actually fall in this bin.
    count: u64,
    min: STY,
    max: STY,
    /// Number of times we accumulated to the sum of this bin.
    sumc: u64,
    sum: f32,
    int_ts: u64,
    last_ts: u64,
    last_vals: Option<(STY, STY, f32)>,
    did_min_max: bool,
    do_time_weight: bool,
    events_ignored_count: u64,
}

impl<STY> EventsXbinDim0Aggregator<STY>
where
    STY: ScalarOps,
{
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(range: SeriesRange, do_time_weight: bool) -> Self {
        let int_ts = range.beg_u64();
        Self {
            range,
            did_min_max: false,
            count: 0,
            min: STY::zero_b(),
            max: STY::zero_b(),
            sumc: 0,
            sum: 0f32,
            int_ts,
            last_ts: 0,
            last_vals: None,
            events_ignored_count: 0,
            do_time_weight,
        }
    }

    fn apply_min_max(&mut self, min: &STY, max: &STY) {
        if self.did_min_max != (self.sumc > 0) {
            panic!("logic error apply_min_max  {}  {}", self.did_min_max, self.sumc);
        }
        if self.sumc == 0 {
            self.did_min_max = true;
            self.min = min.clone();
            self.max = max.clone();
        } else {
            if *min < self.min {
                self.min = min.clone();
            }
            if *max > self.max {
                self.max = max.clone();
            }
        }
    }

    fn apply_event_unweight(&mut self, avg: f32, min: STY, max: STY) {
        //debug!("apply_event_unweight");
        self.apply_min_max(&min, &max);
        self.sumc += 1;
        let vf = avg;
        if vf.is_nan() {
        } else {
            self.sum += vf;
        }
    }

    // Only integrate, do not count because it is used even if the event does not fall into current bin.
    fn apply_event_time_weight(&mut self, px: u64) {
        trace_ingest!(
            "apply_event_time_weight  px {}  count {}  sumc {}  events_ignored_count {}",
            px,
            self.count,
            self.sumc,
            self.events_ignored_count
        );
        if let Some((min, max, avg)) = self.last_vals.as_ref() {
            let vf = *avg;
            {
                let min = min.clone();
                let max = max.clone();
                self.apply_min_max(&min, &max);
            }
            self.sumc += 1;
            let w = (px - self.int_ts) as f32 * 1e-9;
            if vf.is_nan() {
            } else {
                self.sum += vf * w;
            }
            self.int_ts = px;
        } else {
            debug!("apply_event_time_weight NO VALUE");
        }
    }

    fn ingest_unweight(&mut self, item: &EventsXbinDim0<STY>) {
        /*for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let avg = item.avgs[i1];
            let min = item.mins[i1].clone();
            let max = item.maxs[i1].clone();
            if ts < self.range.beg {
            } else if ts >= self.range.end {
            } else {
                self.apply_event_unweight(avg, min, max);
            }
        }*/
        todo!()
    }

    fn ingest_time_weight(&mut self, item: &EventsXbinDim0<STY>) {
        trace!(
            "{} ingest_time_weight  range {:?}  last_ts {:?}  int_ts {:?}",
            Self::type_name(),
            self.range,
            self.last_ts,
            self.int_ts
        );
        let range_beg = self.range.beg_u64();
        let range_end = self.range.end_u64();
        for (((&ts, min), max), avg) in item
            .tss
            .iter()
            .zip(item.mins.iter())
            .zip(item.maxs.iter())
            .zip(item.avgs.iter())
        {
            if ts >= range_end {
                self.events_ignored_count += 1;
                // TODO break early when tests pass.
                //break;
            } else if ts >= range_beg {
                self.apply_event_time_weight(ts);
                self.count += 1;
                self.last_ts = ts;
                self.last_vals = Some((min.clone(), max.clone(), avg.clone()));
            } else {
                self.events_ignored_count += 1;
                self.last_ts = ts;
                self.last_vals = Some((min.clone(), max.clone(), avg.clone()));
            }
        }
    }

    fn result_reset_unweight(&mut self, range: SeriesRange) -> BinsXbinDim0<STY> {
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
        self.sum = 0f32;
        self.sumc = 0;
        self.did_min_max = false;
        self.min = NTY::zero_b();
        self.max = NTY::zero_b();
        ret*/
        todo!()
    }

    fn result_reset_time_weight(&mut self, range: SeriesRange) -> BinsXbinDim0<STY> {
        trace!("{} result_reset_time_weight", Self::type_name());
        // TODO check callsite for correct expand status.
        if self.range.is_time() {
            self.apply_event_time_weight(self.range.end_u64());
        } else {
            error!("TODO result_reset_time_weight");
            err::todoval()
        }
        let range_beg = self.range.beg_u64();
        let range_end = self.range.end_u64();
        let (min, max, avg) = if self.sumc > 0 {
            let avg = self.sum / (self.range.delta_u64() as f32 * 1e-9);
            (self.min.clone(), self.max.clone(), avg)
        } else {
            let (min, max, avg) = match &self.last_vals {
                Some((min, max, avg)) => {
                    warn!("\n\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   SHOULD ALWAYS HAVE ACCUMULATED IN THIS CASE");
                    (min.clone(), max.clone(), avg.clone())
                }
                None => (STY::zero_b(), STY::zero_b(), 0.),
            };
            (min, max, avg)
        };
        let ret = BinsXbinDim0::from_content(
            [range_beg].into(),
            [range_end].into(),
            [self.count].into(),
            [min.clone()].into(),
            [max.clone()].into(),
            [avg].into(),
        );
        self.int_ts = range.beg_u64();
        self.range = range;
        self.count = 0;
        self.sumc = 0;
        self.sum = 0.;
        self.did_min_max = false;
        self.min = STY::zero_b();
        self.max = STY::zero_b();
        ret
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
        debug!("{} ingest", Self::type_name());
        if self.do_time_weight {
            self.ingest_time_weight(item)
        } else {
            self.ingest_unweight(item)
        }
    }

    fn result_reset(&mut self, range: SeriesRange, expand: bool) -> Self::Output {
        if self.do_time_weight {
            self.result_reset_time_weight(range)
        } else {
            self.result_reset_unweight(range)
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
