use crate::agg::MinMaxAvgScalarBinBatchStreamItem;
use crate::cache::pbvfs::{PreBinnedItem, PreBinnedValueFetchedStream};
use crate::cache::{CacheUsage, PreBinnedQuery};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
#[allow(unused_imports)]
use netpod::log::*;
use netpod::{AggKind, BinnedRange, Channel, NodeConfigCached, PreBinnedPatchIterator};
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
    ) -> Self {
        let patches: Vec<_> = patch_it.collect();
        warn!("BinnedStream::new");
        for p in &patches {
            info!("BinnedStream::new  patch  {:?}", p);
        }
        use super::agg::binnedt::IntoBinnedT;
        let inp = futures_util::stream::iter(patches.into_iter())
            .map({
                let node_config = node_config.clone();
                move |patch| {
                    let query = PreBinnedQuery::new(patch, channel.clone(), agg_kind.clone(), cache_usage.clone());
                    PreBinnedValueFetchedStream::new(&query, &node_config)
                }
            })
            .filter_map(|k| match k {
                Ok(k) => ready(Some(k)),
                Err(e) => {
                    error!("{:?}", e);
                    ready(None)
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
                            Some(Ok(MinMaxAvgScalarBinBatchStreamItem::EventDataReadStats(stats)))
                        }
                        Err(e) => {
                            error!("observe error in stream {:?}", e);
                            Some(Err(e))
                        }
                    };
                    ready(g)
                }
            })
            //.map(|k| k)
            .into_binned_t(range);
        Self { inp: Box::pin(inp) }
    }
}

impl Stream for BinnedStream {
    // TODO make this generic over all possible things
    type Item = Result<MinMaxAvgScalarBinBatchStreamItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.inp.poll_next_unpin(cx)
    }
}
