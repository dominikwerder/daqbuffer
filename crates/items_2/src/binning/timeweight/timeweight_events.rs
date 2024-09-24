use super::super::container_events::EventValueType;
use super::___;
use crate::binning::aggregator::AggregatorTimeWeight;
use crate::binning::container_bins::ContainerBins;
use crate::binning::container_events::ContainerEvents;
use crate::binning::container_events::ContainerEventsTakeUpTo;
use crate::binning::container_events::EventSingle;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::DtNano;
use netpod::TsNano;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[allow(unused)]
macro_rules! trace_ { ($($arg:tt)*) => ( if true { eprintln!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_cycle { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_event_next { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest_init_lst { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest_minmax { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest_event { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest_firsts { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest_finish_bin { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest_container { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

const DEBUG_CHECKS: bool = true;

#[derive(Debug, ThisError)]
#[cstm(name = "BinnedEventsTimeweight")]
pub enum Error {
    BadContainer(#[from] super::super::container_events::EventsContainerError),
    Unordered,
    EventAfterRange,
    NoLstAfterFirst,
    EmptyContainerInnerHandler,
    NoLstButMinMax,
    WithLstButEventBeforeRange,
    WithMinMaxButEventBeforeRange,
    NoMinMaxAfterInit,
    ExpectEventWithinRange,
}

type MinMax<EVT> = (EventSingle<EVT>, EventSingle<EVT>);

struct LstRef<'a, EVT>(&'a EventSingle<EVT>);

struct LstMut<'a, EVT>(&'a mut EventSingle<EVT>);

struct InnerB<EVT>
where
    EVT: EventValueType,
{
    cnt: u64,
    active_beg: TsNano,
    active_end: TsNano,
    active_len: DtNano,
    filled_until: TsNano,
    agg: <EVT as EventValueType>::AggregatorTimeWeight,
}

impl<EVT> InnerB<EVT>
where
    EVT: EventValueType,
{
    // NOTE that this is also used during bin-cycle.
    fn ingest_event_with_lst_gt_range_beg_agg(&mut self, ev: EventSingle<EVT>, lst: LstRef<EVT>) {
        trace_ingest_event!("ingest_event_with_lst_gt_range_beg_agg  {:?}", ev);
        if DEBUG_CHECKS {
            if ev.ts <= self.active_beg {
                panic!("should never get here");
            }
            if ev.ts >= self.active_end {
                panic!("should never get here");
            }
        }
        let dt = ev.ts.delta(self.filled_until);
        // TODO can the caller already take the value and replace it afterwards with the current value?
        // This fn could swap the value in lst and directly use it.
        // This would require that any call path does not mess with lst.
        // NOTE that this fn is also used during bin-cycle.
        self.agg.ingest(dt, self.active_len, lst.0.val.clone());
        self.filled_until = ev.ts;
    }

    fn ingest_event_with_lst_gt_range_beg_2(&mut self, ev: EventSingle<EVT>, lst: LstMut<EVT>) -> Result<(), Error> {
        trace_ingest_event!("ingest_event_with_lst_gt_range_beg_2");
        self.ingest_event_with_lst_gt_range_beg_agg(ev.clone(), LstRef(lst.0));
        InnerA::apply_lst_after_event_handled(ev, lst);
        Ok(())
    }

    fn ingest_event_with_lst_gt_range_beg(
        &mut self,
        ev: EventSingle<EVT>,
        lst: LstMut<EVT>,
        minmax: &mut MinMax<EVT>,
    ) -> Result<(), Error> {
        trace_ingest_event!("ingest_event_with_lst_gt_range_beg");
        // TODO if the event is exactly on the current bin first edge, then there is no contribution to the avg yet
        // and I must initialize the min/max with the current event.
        InnerA::apply_min_max(&ev, minmax);
        self.ingest_event_with_lst_gt_range_beg_2(ev.clone(), lst)?;
        Ok(())
    }

    fn ingest_event_with_lst_eq_range_beg(
        &mut self,
        ev: EventSingle<EVT>,
        lst: LstMut<EVT>,
        minmax: &mut MinMax<EVT>,
    ) -> Result<(), Error> {
        trace_ingest_event!("ingest_event_with_lst_eq_range_beg");
        // TODO if the event is exactly on the current bin first edge, then there is no contribution to the avg yet
        // and I must initialize the min/max with the current event.
        InnerA::apply_min_max(&ev, minmax);
        InnerA::apply_lst_after_event_handled(ev, lst);
        Ok(())
    }

    fn ingest_with_lst_gt_range_beg(
        &mut self,
        mut evs: ContainerEventsTakeUpTo<EVT>,
        lst: LstMut<EVT>,
        minmax: &mut MinMax<EVT>,
    ) -> Result<(), Error> {
        trace_ingest_event!("ingest_with_lst_gt_range_beg");
        while let Some(ev) = evs.pop_front() {
            trace_event_next!("ingest_with_lst_ge_range_beg  {:?}", ev);
            if ev.ts <= self.active_beg {
                panic!("should never get here");
            }
            if ev.ts >= self.active_end {
                panic!("should never get here");
            }
            self.ingest_event_with_lst_gt_range_beg(ev.clone(), LstMut(lst.0), minmax)?;
            self.cnt += 1;
        }
        Ok(())
    }

    fn ingest_with_lst_ge_range_beg(
        &mut self,
        mut evs: ContainerEventsTakeUpTo<EVT>,
        lst: LstMut<EVT>,
        minmax: &mut MinMax<EVT>,
    ) -> Result<(), Error> {
        trace_ingest_event!("ingest_with_lst_ge_range_beg");
        while let Some(ev) = evs.pop_front() {
            trace_event_next!("ingest_with_lst_ge_range_beg  {:?}", ev);
            if ev.ts < self.active_beg {
                panic!("should never get here");
            }
            if ev.ts >= self.active_end {
                panic!("should never get here");
            }
            if ev.ts == self.active_beg {
                self.ingest_event_with_lst_eq_range_beg(ev, LstMut(lst.0), minmax)?;
                self.cnt += 1;
            } else {
                self.ingest_event_with_lst_gt_range_beg(ev.clone(), LstMut(lst.0), minmax)?;
                self.cnt += 1;
                trace_ingest_firsts!("ingest_with_lst_ge_range_beg  now calling ingest_with_lst_gt_range_beg");
                return self.ingest_with_lst_gt_range_beg(evs, LstMut(lst.0), minmax);
            }
        }
        Ok(())
    }

    fn ingest_with_lst_minmax(
        &mut self,
        evs: ContainerEventsTakeUpTo<EVT>,
        lst: LstMut<EVT>,
        minmax: &mut MinMax<EVT>,
    ) -> Result<(), Error> {
        trace_ingest_event!("ingest_with_lst_minmax");
        // TODO how to handle the min max? I don't take event data yet out of the container.
        if let Some(ts0) = evs.ts_first() {
            if ts0 < self.active_beg {
                panic!("should never get here");
            } else {
                self.ingest_with_lst_ge_range_beg(evs, lst, minmax)
            }
        } else {
            Ok(())
        }
    }

    // PRECONDITION: filled_until < ts <= active_end
    fn fill_until(&mut self, ts: TsNano, lst: LstRef<EVT>) {
        trace_cycle!("fill_until  ts {:?}", ts);
        let b = self;
        assert!(b.filled_until < ts);
        assert!(ts <= b.active_end);
        b.agg.ingest(ts.delta(b.filled_until), b.active_len, lst.0.val.clone());
        b.filled_until = ts;
    }

    fn fill_remaining_if_space_left(&mut self, lst: LstRef<EVT>) {
        trace_cycle!("fill_remaining_if_space_left");
        let b = self;
        if b.filled_until < b.active_end {
            b.fill_until(b.active_end, lst);
        }
    }
}

struct InnerA<EVT>
where
    EVT: EventValueType,
{
    inner_b: InnerB<EVT>,
    minmax: Option<(EventSingle<EVT>, EventSingle<EVT>)>,
}

impl<EVT> InnerA<EVT>
where
    EVT: EventValueType,
{
    fn apply_min_max(ev: &EventSingle<EVT>, minmax: &mut MinMax<EVT>) {
        if ev.val < minmax.0.val {
            minmax.0 = ev.clone();
        }
        if ev.val > minmax.1.val {
            minmax.1 = ev.clone();
        }
    }

    fn apply_lst_after_event_handled(ev: EventSingle<EVT>, lst: LstMut<EVT>) {
        *lst.0 = ev;
    }

    fn init_minmax(&mut self, ev: &EventSingle<EVT>) {
        trace_ingest_minmax!("init_minmax  {:?}", ev);
        self.minmax = Some((ev.clone(), ev.clone()));
    }

    fn init_minmax_with_lst(&mut self, ev: &EventSingle<EVT>, lst: LstRef<EVT>) {
        trace_ingest_minmax!("init_minmax_with_lst  {:?}  {:?}", ev, lst.0);
        self.minmax = Some((lst.0.clone(), lst.0.clone()));
        Self::apply_min_max(ev, self.minmax.as_mut().unwrap());
    }

    fn ingest_with_lst(&mut self, mut evs: ContainerEventsTakeUpTo<EVT>, lst: LstMut<EVT>) -> Result<(), Error> {
        if let Some(minmax) = self.minmax.as_mut() {
            self.inner_b.ingest_with_lst_minmax(evs, lst, minmax)
        } else {
            if let Some(ev) = evs.pop_front() {
                trace_event_next!("ingest_with_lst  {:?}", ev);
                let beg = self.inner_b.active_beg;
                let end = self.inner_b.active_end;
                if ev.ts < beg {
                    panic!("should never get here");
                } else if ev.ts >= end {
                    panic!("should never get here");
                } else {
                    if ev.ts == beg {
                        self.init_minmax(&ev);
                        InnerA::apply_lst_after_event_handled(ev, lst);
                        Ok(())
                    } else {
                        self.init_minmax_with_lst(&ev, LstRef(lst.0));
                        if let Some(minmax) = self.minmax.as_mut() {
                            if ev.ts == beg {
                                panic!("logic error, is handled before");
                            } else {
                                self.inner_b.ingest_event_with_lst_gt_range_beg_2(ev, LstMut(lst.0))?;
                            }
                            self.inner_b.ingest_with_lst_minmax(evs, lst, minmax)
                        } else {
                            Err(Error::NoMinMaxAfterInit)
                        }
                    }
                }
            } else {
                Ok(())
            }
        }
    }
}

pub struct BinnedEventsTimeweight<EVT>
where
    EVT: EventValueType,
{
    lst: Option<EventSingle<EVT>>,
    range: BinnedRange<TsNano>,
    inner_a: InnerA<EVT>,
    out: ContainerBins<EVT>,
}

impl<EVT> BinnedEventsTimeweight<EVT>
where
    EVT: EventValueType,
{
    pub fn new(range: BinnedRange<TsNano>) -> Self {
        let active_beg = range.nano_beg();
        let active_end = active_beg.add_dt_nano(range.bin_len.to_dt_nano());
        let active_len = active_end.delta(active_beg);
        Self {
            range,
            inner_a: InnerA::<EVT> {
                inner_b: InnerB {
                    cnt: 0,
                    active_beg,
                    active_end,
                    active_len,
                    filled_until: active_beg,
                    agg: <<EVT as EventValueType>::AggregatorTimeWeight as AggregatorTimeWeight<EVT>>::new(),
                },
                minmax: None,
            },
            lst: None,
            out: ContainerBins::new(),
        }
    }

    fn ingest_event_without_lst(&mut self, ev: EventSingle<EVT>) -> Result<(), Error> {
        if ev.ts >= self.inner_a.inner_b.active_end {
            panic!("should never get here");
        } else {
            trace_ingest_init_lst!("ingest_event_without_lst  set lst  {:?}", ev);
            self.lst = Some(ev.clone());
            if ev.ts >= self.inner_a.inner_b.active_beg {
                trace_ingest_minmax!("ingest_event_without_lst");
                self.inner_a.init_minmax(&ev);
                self.inner_a.inner_b.cnt += 1;
            }
            Ok(())
        }
    }

    fn ingest_without_lst(&mut self, mut evs: ContainerEventsTakeUpTo<EVT>) -> Result<(), Error> {
        if let Some(ev) = evs.pop_front() {
            trace_event_next!("ingest_without_lst  {:?}", ev);
            if ev.ts >= self.inner_a.inner_b.active_end {
                panic!("should never get here");
            } else {
                self.ingest_event_without_lst(ev)?;
                if let Some(lst) = self.lst.as_mut() {
                    self.inner_a.ingest_with_lst(evs, LstMut(lst))
                } else {
                    Err(Error::NoLstAfterFirst)
                }
            }
        } else {
            Ok(())
        }
    }

    // Caller asserts that evs is ordered within the current container
    // and with respect to the last container, if any.
    fn ingest_ordered(&mut self, evs: ContainerEventsTakeUpTo<EVT>) -> Result<(), Error> {
        if let Some(lst) = self.lst.as_mut() {
            self.inner_a.ingest_with_lst(evs, LstMut(lst))
        } else {
            if self.inner_a.minmax.is_some() {
                Err(Error::NoLstButMinMax)
            } else {
                self.ingest_without_lst(evs)
            }
        }
    }

    fn cycle_01(&mut self, ts: TsNano) {
        let b = &self.inner_a.inner_b;
        trace_cycle!("cycle_01  {:?}  {:?}", ts, b.active_end);
        assert!(b.active_beg < ts);
        let div = self.range.bin_len.ns();
        if let Some(lst) = self.lst.as_ref() {
            loop {
                let b = &self.inner_a.inner_b;
                if b.filled_until >= ts {
                    break;
                }
                if ts >= b.active_end {
                    self.inner_a.inner_b.fill_remaining_if_space_left(LstRef(lst));
                    let b = &mut self.inner_a.inner_b;
                    let minmax = self.inner_a.minmax.get_or_insert_with(|| {
                        trace_cycle!("cycle_01  minmax not yet set");
                        (lst.clone(), lst.clone())
                    });
                    {
                        // TODO push bin to output.
                        let res = b.agg.result_and_reset_for_new_bin();
                        let cnt = b.cnt;
                        b.cnt = 0;
                        self.out.push_back(
                            b.active_beg,
                            b.active_end,
                            b.cnt,
                            minmax.0.val.clone(),
                            minmax.1.val.clone(),
                            res,
                            lst.val.clone(),
                        );
                    }
                    trace_cycle!("cycle_01  filled up to {:?}  emit and reset", b.active_end);
                    let old_end = b.active_end;
                    let ts1 = TsNano::from_ns(b.active_end.ns() / div * div);
                    assert!(ts1 == old_end);
                    b.active_beg = ts1;
                    b.active_end = ts1.add_dt_nano(b.active_len);
                    b.filled_until = ts1;
                    self.inner_a.minmax = Some((lst.clone(), lst.clone()));
                } else {
                    self.inner_a.inner_b.fill_until(ts, LstRef(lst));
                }
            }
        } else {
            let ts1 = TsNano::from_ns(ts.ns() / div * div);
            let b = &mut self.inner_a.inner_b;
            b.active_beg = ts1;
            b.active_end = ts1.add_dt_nano(b.active_len);
            b.filled_until = ts1;
            b.cnt = 0;
            b.agg.reset_for_new_bin();
            assert!(self.inner_a.minmax.is_none());
            trace_cycle!("cycled direct to  {:?}  {:?}", b.active_beg, b.active_end);
        }
    }

    pub fn ingest(&mut self, mut evs_all: ContainerEvents<EVT>) -> Result<(), Error> {
        // It is this type's task to find and store the one-before event.
        // We then pass it to the aggregation.
        // AggregatorTimeWeight needs a function for that.
        // What about counting the events that actually fall into the range?
        // Maybe that should be done in this type.
        // That way we can pass the values and weights to the aggregation, and count the in-range here.
        // This type must also "close" the current aggregation by passing the "last" and init the next.
        // ALSO: need to keep track of the "lst". Probably best done in this type as well?

        // TODO should rely on external stream adapter for verification to not duplicate things.
        evs_all.verify()?;

        loop {
            // How to handle transition to the next bin?
            // How to handle to not emit bins until at least some partially filled bin is encountered?
            break if let Some(ts) = evs_all.ts_first() {
                let b = &mut self.inner_a.inner_b;
                if ts >= self.range.nano_end() {
                    return Err(Error::EventAfterRange);
                }
                if ts >= b.active_end {
                    trace_cycle!("bin edge boundary {:?}", b.active_end);
                    assert!(b.filled_until < b.active_beg);
                    self.cycle_01(ts);
                }
                let n1 = evs_all.len();
                let len_before = evs_all.len_before(self.inner_a.inner_b.active_end);
                let evs = ContainerEventsTakeUpTo::new(&mut evs_all, len_before);
                if let Some(lst) = self.lst.as_ref() {
                    if ts < lst.ts {
                        return Err(Error::Unordered);
                    } else {
                        self.ingest_ordered(evs)?
                    }
                } else {
                    self.ingest_ordered(evs)?
                };
                trace_ingest_container!("ingest  after still left len  evs {}", evs_all.len());
                let n2 = evs_all.len();
                if n2 != 0 {
                    if n2 == n1 {
                        panic!("no progress");
                    }
                    continue;
                }
            } else {
                ()
            };
        }
        Ok(())
    }

    pub fn range_final(&mut self) -> Result<(), Error> {
        trace_cycle!("range_final");
        self.cycle_01(self.range.nano_end());
        Ok(())
    }

    pub fn output(&mut self) -> ContainerBins<EVT> {
        ::core::mem::replace(&mut self.out, ContainerBins::new())
    }
}

pub struct BinnedEventsTimeweightStream {}

impl Stream for BinnedEventsTimeweightStream {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
