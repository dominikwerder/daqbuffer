use crate::agg::streams::StreamItem;
use crate::binned::{BinnedStreamKind, RangeCompletableItem};
use crate::cache::pbvfs::PreBinnedScalarValueFetchedStream;
use crate::cache::{CacheUsage, PreBinnedQuery};
use crate::frame::makeframe::FrameType;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{AggKind, BinnedRange, ByteSize, Channel, NodeConfigCached, PreBinnedPatchIterator};
use std::future::ready;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct BinnedScalarStreamFromPreBinnedPatches<SK>
where
    SK: BinnedStreamKind,
{
    inp: Pin<
        Box<
            dyn Stream<Item = Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::TBinnedBins>>, Error>>
                + Send,
        >,
    >,
    _stream_kind: SK,
}

impl<SK> BinnedScalarStreamFromPreBinnedPatches<SK>
where
    SK: BinnedStreamKind,
    Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::TBinnedBins>>, Error>: FrameType,
{
    pub fn new(
        patch_it: PreBinnedPatchIterator,
        channel: Channel,
        range: BinnedRange,
        agg_kind: AggKind,
        cache_usage: CacheUsage,
        node_config: &NodeConfigCached,
        disk_stats_every: ByteSize,
        report_error: bool,
        stream_kind: SK,
    ) -> Result<Self, Error> {
        let patches: Vec<_> = patch_it.collect();
        let mut sp = String::new();
        if false {
            // Convert this to a StreamLog message:
            for (i, p) in patches.iter().enumerate() {
                use std::fmt::Write;
                write!(sp, "  • patch {:2}  {:?}\n", i, p)?;
            }
            info!("Using these pre-binned patches:\n{}", sp);
        }
        let pmax = patches.len();
        let inp = futures_util::stream::iter(patches.into_iter().enumerate())
            .map({
                let node_config = node_config.clone();
                let stream_kind = stream_kind.clone();
                move |(pix, patch)| {
                    let query = PreBinnedQuery::new(
                        patch,
                        channel.clone(),
                        agg_kind.clone(),
                        cache_usage.clone(),
                        disk_stats_every.clone(),
                        report_error,
                    );
                    let ret: Pin<Box<dyn Stream<Item = _> + Send>> =
                        match PreBinnedScalarValueFetchedStream::new(&query, &node_config, &stream_kind) {
                            Ok(stream) => Box::pin(stream.map(move |q| (pix, q))),
                            Err(e) => {
                                error!("error from PreBinnedValueFetchedStream::new {:?}", e);
                                Box::pin(futures_util::stream::iter(vec![(pix, Err(e))]))
                            }
                        };
                    ret
                }
            })
            .flatten()
            .filter_map({
                let range = range.clone();
                move |(pix, k)| {
                    let fit_range = range.full_range();
                    let g = match k {
                        Ok(item) => match item {
                            StreamItem::Log(item) => Some(Ok(StreamItem::Log(item))),
                            StreamItem::Stats(item) => Some(Ok(StreamItem::Stats(item))),
                            StreamItem::DataItem(item) => match item {
                                RangeCompletableItem::RangeComplete => {
                                    if pix + 1 == pmax {
                                        Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)))
                                    } else {
                                        None
                                    }
                                }
                                RangeCompletableItem::Data(item) => {
                                    match crate::binned::FilterFittingInside::filter_fitting_inside(item, fit_range) {
                                        Some(item) => Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))),
                                        None => None,
                                    }
                                }
                            },
                        },
                        Err(e) => Some(Err(e)),
                    };
                    ready(g)
                }
            });
        // TODO activate the T-binning via the bin-to-bin binning trait.
        //err::todo();
        let inp = crate::agg::binnedt2::IntoBinnedT::into_binned_t(inp, range);
        Ok(Self {
            inp: Box::pin(inp),
            _stream_kind: stream_kind,
        })
    }
}

// TODO change name, type is generic now:
// Can I remove the whole type or keep for static check?
impl<SK> Stream for BinnedScalarStreamFromPreBinnedPatches<SK>
where
    SK: BinnedStreamKind,
{
    type Item = Result<StreamItem<RangeCompletableItem<SK::TBinnedBins>>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.inp.poll_next_unpin(cx)
    }
}

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
