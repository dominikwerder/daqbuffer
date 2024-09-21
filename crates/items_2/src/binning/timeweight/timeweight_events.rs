use super::super::container_events::EventValueType;
use super::___;
use crate::binning::container_events::ContainerEvents;
use crate::binning::container_events::EventSingle;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::TsNano;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[allow(unused)]
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[derive(Debug, ThisError)]
#[cstm(name = "BinnedEventsTimeweight")]
pub enum Error {
    BadContainer(#[from] super::super::container_events::EventsContainerError),
    Unordered,
    AnotherBeforeRange,
    NoLstAfterFirst,
    EmptyContainerInnerHandler,
    NoLstButMinMax,
}

type MinMax<EVT> = (EventSingle<EVT>, EventSingle<EVT>);

struct InnerA<EVT> {
    range: BinnedRange<TsNano>,
    cnt: u64,
    _t1: PhantomData<EVT>,
}

impl<EVT> InnerA<EVT>
where
    EVT: EventValueType,
{
    fn ingest_event_with_lst_in_range(
        &mut self,
        ev: EventSingle<EVT>,
        lst: &mut EventSingle<EVT>,
    ) -> Result<(), Error> {
        // Aggregator:
        // Must handle min, max, avg, var.
        // min and max is actually tricky and can not be done in one go with lst:
        // The current read procedure allows that the event stream contains a one-before event even though the
        // first in-range event is exactly on range-beg. In that case the min/max given by the one-before is
        // irrelevant for the bin.

        // fn apply_event_time_weight(&mut self, px: u64) {
        //     if let Some((_, _, v)) = self.minmaxlst.as_ref() {
        //         trace_ingest!("apply_event_time_weight with v {v:?}");
        //         let vf = v.as_prim_f32_b();
        //         let v2 = v.clone();
        //         self.apply_min_max_lst(v2);
        //         self.sumc += 1;
        //         let w = (px - self.int_ts) as f32 * 1e-9;
        //         if false {
        //             trace!(
        //                 "int_ts {:10}  px {:8}  w {:8.1}  vf {:8.1}  sum {:8.1}",
        //                 self.int_ts / MS,
        //                 px / MS,
        //                 w,
        //                 vf,
        //                 self.sum
        //             );
        //         }
        //         if vf.is_nan() {
        //         } else {
        //             self.sum += vf * w;
        //         }
        //         self.int_ts = px;
        //     } else {
        //         debug_ingest!("apply_event_time_weight NO VALUE");
        //     }
        // }

        todo!()
    }

    fn ingest_with_lst_ge_range_beg(
        &mut self,
        mut evs: ContainerEvents<EVT>,
        lst: &mut EventSingle<EVT>,
    ) -> Result<(), Error> {
        while let Some(ev) = evs.event_next() {
            if true {
                // How to handle transition to the next bin?
                // What does self.range mean, the full requested range of all bins or the current range?
                // Do I maybe need both?
                // How to handle to not emit bins until at least some partially filled bin is encountered?
                // How to implement the bin cycle logic in clean way?
                // How to emit things? Do to ship results to the caller?
                todo!("must check if ev is already after range.");
            }

            if ev.ts >= self.range.nano_end() {
                todo!("make sure that the transition to the next bin (if we want any next bin) works");
                todo!("keep lst. we derive min/max from lst upon the first event in range");

                // TODO where and how do I initialize min/max ?
            }

            // TODO if the event is exactly on the current bin first edge, then there is no contribution to the avg yet
            // and I must initialize the min/max with the current event.
            // If the event is after the current bin first edge, then min/max is initialized from the lst
            // and there is a contribution from the lst to the avg.

            self.ingest_event_with_lst_in_range(ev, lst)?;

            // TODO update the lst (needs clone?)
        }
        Ok(())
    }

    fn ingest_with_lst_nominmax(
        &mut self,
        evs: ContainerEvents<EVT>,
        lst: &mut EventSingle<EVT>,
        minmax: &mut Option<MinMax<EVT>>,
    ) -> Result<(), Error> {
        // TODO how to handle the min max? I don't take event data yet out of the container.
        todo!("minmax handle");
        if let Some(ts0) = evs.ts_first() {
            if ts0 < self.range.nano_beg() {
                Err(Error::AnotherBeforeRange)
            } else {
                self.ingest_with_lst_ge_range_beg(evs, lst)
            }
        } else {
            Err(Error::EmptyContainerInnerHandler)
        }
    }

    fn ingest_with_lst_minmax(
        &mut self,
        evs: ContainerEvents<EVT>,
        lst: &mut EventSingle<EVT>,
        minmax: &mut MinMax<EVT>,
    ) -> Result<(), Error> {
        // TODO how to handle the min max? I don't take event data yet out of the container.
        todo!("minmax handle");
        if let Some(ts0) = evs.ts_first() {
            if ts0 < self.range.nano_beg() {
                Err(Error::AnotherBeforeRange)
            } else {
                self.ingest_with_lst_ge_range_beg(evs, lst)
            }
        } else {
            Err(Error::EmptyContainerInnerHandler)
        }
    }
}

pub struct BinnedEventsTimeweight<EVT>
where
    EVT: EventValueType,
{
    inner_a: InnerA<EVT>,
    lst: Option<EventSingle<EVT>>,
    minmax: Option<(EventSingle<EVT>, EventSingle<EVT>)>,
}

impl<EVT> BinnedEventsTimeweight<EVT>
where
    EVT: EventValueType,
{
    pub fn new(range: BinnedRange<TsNano>) -> Self {
        Self {
            inner_a: InnerA::<EVT> {
                range,
                cnt: 0,
                _t1: PhantomData,
            },
            lst: None,
            minmax: None,
        }
    }

    fn ingest_event_without_lst(&mut self, ev: EventSingle<EVT>) -> Result<(), Error> {
        let range = &self.inner_a.range;
        let beg = range.nano_beg();
        let end = range.nano_end();
        if ev.ts < end {
            if ev.ts >= beg {
                self.minmax = Some((ev.clone(), ev.clone()));
                self.inner_a.cnt += 1;
            }
            self.lst = Some(ev);
        }
        Ok(())
    }

    fn ingest_without_lst(&mut self, mut evs: ContainerEvents<EVT>) -> Result<(), Error> {
        if let Some(ev) = evs.event_next() {
            self.ingest_event_without_lst(ev)?;
        }
        if let Some(lst) = self.lst.as_mut() {
            if let Some(minmax) = self.minmax.as_mut() {
                self.inner_a.ingest_with_lst_minmax(evs, lst, minmax)
            } else {
                self.inner_a.ingest_with_lst_nominmax(evs, lst, &mut self.minmax)
            }
        } else {
            Err(Error::NoLstAfterFirst)
        }
    }

    // Caller asserts that evs is ordered within the current container
    // and with respect to the last container, if any.
    fn ingest_ordered(&mut self, evs: ContainerEvents<EVT>) -> Result<(), Error> {
        if let Some(lst) = self.lst.as_mut() {
            if let Some(minmax) = self.minmax.as_mut() {
                self.inner_a.ingest_with_lst_minmax(evs, lst, minmax)
            } else {
                self.inner_a.ingest_with_lst_nominmax(evs, lst, &mut self.minmax)
            }
        } else {
            if self.minmax.is_some() {
                Err(Error::NoLstButMinMax)
            } else {
                self.ingest_without_lst(evs)
            }
        }
    }

    pub fn ingest(&mut self, evs: ContainerEvents<EVT>) -> Result<(), Error> {
        // It is this type's task to find and store the one-before event.
        // We then pass it to the aggregation.
        // AggregatorTimeWeight needs a function for that.
        // What about counting the events that actually fall into the range?
        // Maybe that should be done in this type.
        // That way we can pass the values and weights to the aggregation, and count the in-range here.
        // This type must also "close" the current aggregation by passing the "last" and init the next.
        // ALSO: need to keep track of the "lst". Probably best done in this type as well?

        // TODO should rely on external stream adapter for verification to not duplicate things.
        evs.verify()?;

        if let Some(ts) = evs.ts_first() {
            if let Some(lst) = self.lst.as_ref() {
                if ts < lst.ts {
                    return Err(Error::Unordered);
                } else {
                    self.ingest_ordered(evs)
                }
            } else {
                self.ingest_ordered(evs)
            }
        } else {
            Ok(())
        }
    }
}

pub struct BinnedEventsTimeweightStream {}

impl Stream for BinnedEventsTimeweightStream {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
