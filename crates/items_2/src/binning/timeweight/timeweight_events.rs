use super::super::container_events::EventValueType;
use super::___;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use netpod::log::*;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[allow(unused)]
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[derive(Debug, ThisError)]
#[cstm(name = "BinnedEventsTimeweight")]
pub enum Error {}

pub struct BinnedEventsTimeweight<EVT>
where
    EVT: EventValueType,
{
    _evt: PhantomData<EVT>,
}

impl<EVT> BinnedEventsTimeweight<EVT>
where
    EVT: EventValueType,
{
    pub fn ingest(&mut self, evs: <EVT as EventValueType>::Container) -> Result<(), Error> {
        // It is this type's task to find and store the one-before event.
        // We then pass it to the aggregation.
        // AggregatorTimeWeight needs a function for that.
        // What about counting the events that actually fall into the range?
        // Maybe that should be done in this type.
        // That way we can pass the values and weights to the aggregation, and count the in-range here.
        // This type must also "close" the current aggregation by passing the "last" and init the next.
        // ALSO: need to keep track of the "lst". Probably best done in this type as well?
        todo!()
    }
}

pub struct BinnedEventsTimeweightStream {}

impl Stream for BinnedEventsTimeweightStream {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
