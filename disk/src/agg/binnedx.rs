use crate::agg::streams::StreamItem;
use crate::agg::AggregatableXdim1Bin;
use crate::binned::{RangeCompletableItem, StreamKind};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use std::task::{Context, Poll};

pub trait IntoBinnedXBins1<I, SK>
where
    SK: StreamKind,
    Self: Stream<Item = Result<StreamItem<RangeCompletableItem<I>>, Error>> + Unpin,
    I: AggregatableXdim1Bin<SK>,
{
    type StreamOut;
    fn into_binned_x_bins_1(self) -> Self::StreamOut;
}

impl<S, I, SK> IntoBinnedXBins1<I, SK> for S
where
    SK: StreamKind,
    S: Stream<Item = Result<StreamItem<RangeCompletableItem<I>>, Error>> + Unpin,
    I: AggregatableXdim1Bin<SK>,
{
    type StreamOut = IntoBinnedXBins1DefaultStream<S, I, SK>;

    fn into_binned_x_bins_1(self) -> Self::StreamOut {
        IntoBinnedXBins1DefaultStream {
            inp: self,
            _marker: std::marker::PhantomData::default(),
        }
    }
}

pub struct IntoBinnedXBins1DefaultStream<S, I, SK>
where
    SK: StreamKind,
    S: Stream<Item = Result<StreamItem<RangeCompletableItem<I>>, Error>> + Unpin,
    I: AggregatableXdim1Bin<SK>,
{
    inp: S,
    _marker: std::marker::PhantomData<SK>,
}

impl<S, I, SK> Stream for IntoBinnedXBins1DefaultStream<S, I, SK>
where
    SK: StreamKind,
    S: Stream<Item = Result<StreamItem<RangeCompletableItem<I>>, Error>> + Unpin,
    I: AggregatableXdim1Bin<SK>,
{
    type Item = Result<StreamItem<RangeCompletableItem<I::Output>>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => match k {
                StreamItem::Log(item) => Ready(Some(Ok(StreamItem::Log(item)))),
                StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => {
                        Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                    }
                    RangeCompletableItem::Data(item) => Ready(Some(Ok(StreamItem::DataItem(
                        RangeCompletableItem::Data(item.into_agg()),
                    )))),
                },
            },
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}
