use crate::agg::streams::StreamItem;
use crate::binned::query::{CacheUsage, PreBinnedQuery};
use crate::binned::{RangeCompletableItem, StreamKind};
use crate::cache::pbvfs::PreBinnedScalarValueFetchedStream;
use crate::frame::makeframe::FrameType;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{AggKind, BinnedRange, ByteSize, Channel, NodeConfigCached, PreBinnedPatchIterator};
use std::future::ready;
use std::pin::Pin;
use std::task::{Context, Poll};

// TODO remove after refactor.
pub struct BoxedStream<I> {
    inp: Pin<Box<dyn Stream<Item = I> + Send>>,
}

impl<I> BoxedStream<I> {
    pub fn new<T>(inp: T) -> Result<Self, Error>
    where
        T: Stream<Item = I> + Send + 'static,
    {
        Ok(Self { inp: Box::pin(inp) })
    }
}

impl<I> Stream for BoxedStream<I> {
    type Item = I;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.inp.poll_next_unpin(cx)
    }
}
