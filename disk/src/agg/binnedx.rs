use crate::agg::AggregatableXdim1Bin;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use std::task::{Context, Poll};

pub trait IntoBinnedXBins1<I: AggregatableXdim1Bin> {
    type StreamOut;
    fn into_binned_x_bins_1(self) -> Self::StreamOut
    where
        Self: Stream<Item = Result<I, Error>>;
}

impl<T, I: AggregatableXdim1Bin> IntoBinnedXBins1<I> for T
where
    T: Stream<Item = Result<I, Error>> + Unpin,
{
    type StreamOut = IntoBinnedXBins1DefaultStream<T, I>;

    fn into_binned_x_bins_1(self) -> Self::StreamOut {
        IntoBinnedXBins1DefaultStream { inp: self }
    }
}

pub struct IntoBinnedXBins1DefaultStream<S, I>
where
    S: Stream<Item = Result<I, Error>> + Unpin,
    I: AggregatableXdim1Bin,
{
    inp: S,
}

impl<S, I> Stream for IntoBinnedXBins1DefaultStream<S, I>
where
    S: Stream<Item = Result<I, Error>> + Unpin,
    I: AggregatableXdim1Bin,
{
    type Item = Result<I::Output, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => Ready(Some(Ok(k.into_agg()))),
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}
