use futures_util::Stream;
use items_0::Events;
use items_0::WithLen;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct FirstBefore<S> {
    inp: S,
}

impl<S, T, E> Stream for FirstBefore<S>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    T: Events,
{
    type Item = <S as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
