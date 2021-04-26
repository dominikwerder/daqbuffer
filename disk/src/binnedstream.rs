use crate::agg::MinMaxAvgScalarBinBatch;
use crate::cache::pbvfs::PreBinnedValueFetchedStream;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
#[allow(unused_imports)]
use netpod::log::*;
use netpod::RetStreamExt;
use netpod::{AggKind, Channel, NodeConfig, PreBinnedPatchIterator};
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
        agg_kind: AggKind,
        node_config: Arc<NodeConfig>,
    ) -> Self {
        warn!("BinnedStream will open a PreBinnedValueStream");
        let inp = futures_util::stream::iter(patch_it)
            .map(move |coord| {
                PreBinnedValueFetchedStream::new(coord, channel.clone(), agg_kind.clone(), node_config.clone())
            })
            .flatten()
            .only_first_error()
            .map(|k| {
                match k {
                    Ok(ref k) => {
                        trace!("BinnedStream  got good item {:?}", k);
                    }
                    Err(_) => {
                        error!("\n\n-----------------------------------------------------   BinnedStream  got error")
                    }
                }
                k
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
