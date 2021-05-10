use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatchStreamItem;
use crate::cache::pbvfs::{PreBinnedItem, PreBinnedValueFetchedStream};
use crate::cache::{CacheUsage, PreBinnedQuery};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
#[allow(unused_imports)]
use netpod::log::*;
use netpod::{AggKind, BinnedRange, ByteSize, Channel, NodeConfigCached, PreBinnedPatchIterator};
use std::future::ready;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct BinnedStream {
    inp: Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarBinBatchStreamItem, Error>> + Send>>,
}

impl BinnedStream {
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
        for (i, p) in patches.iter().enumerate() {
            use std::fmt::Write;
            write!(sp, "  • patch {:2}  {:?}\n", i, p)?;
        }
        info!("BinnedStream::new\n{}", sp);
        use super::agg::binnedt::IntoBinnedT;
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
                                error!("see error {:?}", e);
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
                                | Fits::PartlyLowerAndGreater => Some(Ok(MinMaxAvgScalarBinBatchStreamItem::Values(k))),
                                _ => None,
                            }
                        }
                        Ok(PreBinnedItem::RangeComplete) => Some(Ok(MinMaxAvgScalarBinBatchStreamItem::RangeComplete)),
                        Ok(PreBinnedItem::EventDataReadStats(stats)) => {
                            //info!("BinnedStream  '''''''''''''''''''   observes stats {:?}", stats);
                            Some(Ok(MinMaxAvgScalarBinBatchStreamItem::EventDataReadStats(stats)))
                        }
                        Ok(PreBinnedItem::Log(item)) => Some(Ok(MinMaxAvgScalarBinBatchStreamItem::Log(item))),
                        Err(e) => Some(Err(e)),
                    };
                    ready(g)
                }
            })
            .into_binned_t(range);
        Ok(Self { inp: Box::pin(inp) })
    }
}

impl Stream for BinnedStream {
    // TODO make this generic over all possible things
    type Item = Result<MinMaxAvgScalarBinBatchStreamItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.inp.poll_next_unpin(cx)
    }
}

pub struct BinnedStreamFromMerged {
    inp: Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarBinBatchStreamItem, Error>> + Send>>,
}

impl BinnedStreamFromMerged {
    pub fn new(
        inp: Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarBinBatchStreamItem, Error>> + Send>>,
    ) -> Result<Self, Error> {
        Ok(Self { inp })
    }
}

impl Stream for BinnedStreamFromMerged {
    // TODO make this generic over all possible things
    type Item = Result<MinMaxAvgScalarBinBatchStreamItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.inp.poll_next_unpin(cx)
    }
}
