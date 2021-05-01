use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::cache::pbvfs::{PreBinnedItem, PreBinnedValueFetchedStream};
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
    inp: Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarBinBatch, Error>> + Send>>,
}

impl BinnedStream {
    pub fn new(
        patch_it: PreBinnedPatchIterator,
        channel: Channel,
        range: BinnedRange,
        agg_kind: AggKind,
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
                move |coord| PreBinnedValueFetchedStream::new(coord, channel.clone(), agg_kind.clone(), &node_config)
            })
            .flatten()
            .filter_map({
                let range = range.clone();
                move |k| {
                    let fit_range = range.full_range();
                    let g = match k.0 {
                        Ok(PreBinnedItem::Batch(k)) => {
                            use super::agg::{Fits, FitsInside};
                            match k.fits_inside(fit_range) {
                                Fits::Inside
                                | Fits::PartlyGreater
                                | Fits::PartlyLower
                                | Fits::PartlyLowerAndGreater => Some(Ok(k)),
                                _ => None,
                            }
                        }
                        Err(e) => {
                            error!("observe error in stream {:?}", e);
                            Some(Err(e))
                        }
                    };
                    ready(g)
                }
            })
            .map(|k| k)
            .into_binned_t(range);
        Self { inp: Box::pin(inp) }
    }
}

impl Stream for BinnedStream {
    // TODO make this generic over all possible things
    type Item = Result<MinMaxAvgScalarBinBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.inp.poll_next_unpin(cx)
    }
}
