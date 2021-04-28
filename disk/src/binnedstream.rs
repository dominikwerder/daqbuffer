use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::cache::pbvfs::PreBinnedValueFetchedStream;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
#[allow(unused_imports)]
use netpod::log::*;
use netpod::{AggKind, Channel, NodeConfig, PreBinnedPatchIterator};
use netpod::{NanoRange, RetStreamExt};
use std::future::ready;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct BinnedStream {
    inp: Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarBinBatch, Error>> + Send>>,
}

impl BinnedStream {
    pub fn new(
        patch_it: PreBinnedPatchIterator,
        channel: Channel,
        range: NanoRange,
        agg_kind: AggKind,
        node_config: Arc<NodeConfig>,
    ) -> Self {
        let patches: Vec<_> = patch_it.collect();
        warn!("BinnedStream will open a PreBinnedValueStream");
        for p in &patches {
            info!("BinnedStream  ->  patch  {:?}", p);
        }
        let inp = futures_util::stream::iter(patches.into_iter())
            .map(move |coord| {
                PreBinnedValueFetchedStream::new(coord, channel.clone(), agg_kind.clone(), node_config.clone())
            })
            .flatten()
            .only_first_error()
            .filter_map({
                let range = range.clone();
                move |k: Result<MinMaxAvgScalarBinBatch, Error>| {
                    let g = match k {
                        Ok(k) => {
                            use super::agg::{Fits, FitsInside};
                            match k.fits_inside(range.clone()) {
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
            });
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
