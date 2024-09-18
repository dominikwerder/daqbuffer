use super::super::container_events::EventValueType;
use super::___;
use futures_util::Stream;
use netpod::log::*;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[allow(unused)]
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

pub struct BinnedEventsTimeweight<EVT>
where
    EVT: EventValueType,
{
    _evt: PhantomData<EVT>,
}

impl<EVT> BinnedEventsTimeweight<EVT> where EVT: EventValueType {}

pub struct BinnedEventsTimeweightStream {}

impl Stream for BinnedEventsTimeweightStream {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
