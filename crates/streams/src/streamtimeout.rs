use futures_util::Stream;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct TimeoutableStream<S> {
    _t1: PhantomData<S>,
}

impl<S> TimeoutableStream<S> {
    fn new() -> Self {
        Self { _t1: PhantomData }
    }
}

impl<S> Stream for TimeoutableStream<S>
where
    S: Stream,
{
    type Item = Option<<S as Stream>::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub trait StreamTimeout<S>: Send {
    fn timeout_intervals(&self, inp: Pin<Box<dyn Stream<Item = S> + Send>>) -> Pin<Box<dyn Stream<Item = S> + Send>>;
}

pub trait StreamTimeout2<S>: Send {
    fn timeout_intervals(&self, inp: S) -> TimeoutableStream<S>;
}
