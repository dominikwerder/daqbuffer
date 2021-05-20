use crate::agg::binnedt::IntoBinnedT;
use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatchStreamItem;
use crate::agg::streams::{StatsItem, StreamItem};
use crate::binned::scalar::adapter_to_stream_item;
use crate::binned::BinnedScalarStreamItem;
use crate::cache::pbvfs::{PreBinnedItem, PreBinnedValueFetchedStream};
use crate::cache::{CacheUsage, PreBinnedQuery};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{AggKind, BinnedRange, ByteSize, Channel, NodeConfigCached, PreBinnedPatchIterator};
use std::future::ready;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct BinnedScalarStreamFromPreBinnedPatches {
    inp: Pin<Box<dyn Stream<Item = Result<StreamItem<BinnedScalarStreamItem>, Error>> + Send>>,
}

impl BinnedScalarStreamFromPreBinnedPatches {
    pub fn new(
        patch_it: PreBinnedPatchIterator,
        channel: Channel,
        range: BinnedRange,
        agg_kind: AggKind,
        cache_usage: CacheUsage,
        node_config: &NodeConfigCached,
        disk_stats_every: ByteSize,
    ) -> Result<Self, Error> {
        let patches: Vec<_> = patch_it.collect();
        let mut sp = String::new();
        if false {
            // Convert this to a StreamLog message:
            for (i, p) in patches.iter().enumerate() {
                use std::fmt::Write;
                write!(sp, "  • patch {:2}  {:?}\n", i, p)?;
            }
            info!("BinnedStream::new\n{}", sp);
        }
        let inp = futures_util::stream::iter(patches.into_iter())
            .map({
                let node_config = node_config.clone();
                move |patch| {
                    let query = PreBinnedQuery::new(
                        patch,
                        channel.clone(),
                        agg_kind.clone(),
                        cache_usage.clone(),
                        disk_stats_every.clone(),
                    );
                    let s: Pin<Box<dyn Stream<Item = _> + Send>> =
                        match PreBinnedValueFetchedStream::new(&query, &node_config) {
                            Ok(k) => Box::pin(k),
                            Err(e) => {
                                error!("error from PreBinnedValueFetchedStream::new {:?}", e);
                                Box::pin(futures_util::stream::iter(vec![Err(e)]))
                            }
                        };
                    s
                }
            })
            .flatten()
            .filter_map({
                let range = range.clone();
                move |k| {
                    let fit_range = range.full_range();
                    let g = match k {
                        Ok(PreBinnedItem::Batch(k)) => {
                            use super::agg::{Fits, FitsInside};
                            match k.fits_inside(fit_range) {
                                Fits::Inside
                                | Fits::PartlyGreater
                                | Fits::PartlyLower
                                | Fits::PartlyLowerAndGreater => {
                                    Some(Ok(StreamItem::DataItem(BinnedScalarStreamItem::Values(k))))
                                }
                                _ => None,
                            }
                        }
                        Ok(PreBinnedItem::RangeComplete) => {
                            Some(Ok(StreamItem::DataItem(BinnedScalarStreamItem::RangeComplete)))
                        }
                        Ok(PreBinnedItem::EventDataReadStats(item)) => {
                            Some(Ok(StreamItem::Stats(StatsItem::EventDataReadStats(item))))
                        }
                        Ok(PreBinnedItem::Log(item)) => Some(Ok(StreamItem::Log(item))),
                        Err(e) => Some(Err(e)),
                    };
                    ready(g)
                }
            });
        //let inp: Box<dyn Stream<Item = Result<StreamItem<BinnedScalarStreamItem>, Error>> + Send + Unpin> =
        //    Box::new(inp);
        //let inp: &Stream<Item = Result<StreamItem<BinnedScalarStreamItem>, Error>> + Send + Unpin>> = &inp
        //() == inp;
        let inp = IntoBinnedT::into_binned_t(inp, range);
        Ok(Self { inp: Box::pin(inp) })
        //err::todoval()
    }
}

impl Stream for BinnedScalarStreamFromPreBinnedPatches {
    type Item = Result<StreamItem<BinnedScalarStreamItem>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => Ready(Some(item)),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub struct BinnedStream<I> {
    inp: Pin<Box<dyn Stream<Item = I> + Send>>,
}

impl<I> BinnedStream<I> {
    pub fn new(inp: Pin<Box<dyn Stream<Item = I> + Send>>) -> Result<Self, Error> {
        Ok(Self { inp })
    }
}

impl<I> Stream for BinnedStream<I> {
    type Item = I;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.inp.poll_next_unpin(cx)
    }
}
