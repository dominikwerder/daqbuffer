use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::TransformProperties;
use items_0::Transformer;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct Enumerate2<T> {
    inp: T,
    cnt: usize,
}

impl<T> Enumerate2<T> {
    pub fn new(inp: T) -> Self
    where
        T: Transformer,
    {
        Self { inp, cnt: 0 }
    }
}

impl<T> Stream for Enumerate2<T>
where
    T: Stream + Unpin,
{
    type Item = (usize, <T as Stream>::Item);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => {
                let i = self.cnt;
                self.cnt += 1;
                Ready(Some((i, item)))
            }
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

impl<T> Transformer for Enumerate2<T> {
    fn query_transform_properties(&self) -> TransformProperties {
        todo!()
    }
}

pub struct Then2<T, F, Fut> {
    inp: T,
    f: F,
    fut: Option<Pin<Box<Fut>>>,
}

impl<T, F, Fut> Then2<T, F, Fut>
where
    T: Stream,
    F: FnMut(<T as Stream>::Item) -> Fut,
    Fut: Future,
{
    pub fn new(inp: T, f: F) -> Self {
        Self { inp, f, fut: None }
    }

    fn prepare_fut(&mut self, item: <T as Stream>::Item) {
        self.fut = Some(Box::pin((self.f)(item)));
    }
}

impl<T, F, Fut> Stream for Then2<T, F, Fut>
where
    T: Stream + Unpin,
    Fut: Future,
{
    type Item = <Fut as Future>::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let Some(fut) = &mut self.fut {
                match fut.poll_unpin(cx) {
                    Ready(item) => {
                        self.fut = None;
                        Ready(Some(item))
                    }
                    Pending => Pending,
                }
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(item)) => {
                        continue;
                    }
                    Ready(None) => Ready(None),
                    Pending => Pending,
                }
            };
        }
    }
}

impl<T, F, Fut> Transformer for Then2<T, F, Fut> {
    fn query_transform_properties(&self) -> TransformProperties {
        todo!()
    }
}

pub trait TransformerExt {
    fn enumerate2(self) -> Enumerate2<Self>
    where
        Self: Transformer + Sized;

    fn then2<F, Fut>(self, f: F) -> Then2<Self, F, Fut>
    where
        Self: Transformer + Stream + Sized,
        F: FnMut(<Self as Stream>::Item) -> Fut,
        Fut: Future;
}

impl<T> TransformerExt for T {
    fn enumerate2(self) -> Enumerate2<Self>
    where
        Self: Transformer + Sized,
    {
        Enumerate2::new(self)
    }

    fn then2<F, Fut>(self, f: F) -> Then2<Self, F, Fut>
    where
        Self: Transformer + Stream + Sized,
        F: FnMut(<Self as Stream>::Item) -> Fut,
        Fut: Future,
    {
        Then2::new(self, f)
    }
}

pub struct VecStream<T> {
    inp: VecDeque<T>,
}

impl<T> VecStream<T> {
    pub fn new(inp: VecDeque<T>) -> Self {
        Self { inp }
    }
}

impl<T> Stream for VecStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if let Some(item) = self.inp.pop_front() {
            Ready(Some(item))
        } else {
            Ready(None)
        }
    }
}

impl<T> Transformer for VecStream<T> {
    fn query_transform_properties(&self) -> TransformProperties {
        todo!()
    }
}
