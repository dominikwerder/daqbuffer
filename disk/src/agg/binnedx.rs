use crate::agg::streams::StreamItem;
use crate::agg::AggregatableXdim1Bin;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use std::task::{Context, Poll};

pub trait IntoBinnedXBins1<I>
where
    I: AggregatableXdim1Bin,
{
    type StreamOut;
    fn into_binned_x_bins_1(self) -> Self::StreamOut
    where
        Self: Stream<Item = Result<StreamItem<I>, Error>>;
}

impl<T, I> IntoBinnedXBins1<I> for T
where
    T: Stream<Item = Result<StreamItem<I>, Error>> + Unpin,
    I: AggregatableXdim1Bin,
{
    type StreamOut = IntoBinnedXBins1DefaultStream<T, I>;

    fn into_binned_x_bins_1(self) -> Self::StreamOut {
        IntoBinnedXBins1DefaultStream { inp: self }
    }
}

pub struct IntoBinnedXBins1DefaultStream<S, I>
where
    S: Stream<Item = Result<StreamItem<I>, Error>> + Unpin,
    I: AggregatableXdim1Bin,
{
    inp: S,
}

impl<S, I> Stream for IntoBinnedXBins1DefaultStream<S, I>
where
    S: Stream<Item = Result<StreamItem<I>, Error>> + Unpin,
    I: AggregatableXdim1Bin,
{
    type Item = Result<StreamItem<I::Output>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => match k {
                StreamItem::Log(item) => Ready(Some(Ok(StreamItem::Log(item)))),
                StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                StreamItem::DataItem(item) => Ready(Some(Ok(StreamItem::DataItem(item.into_agg())))),
            },
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}
