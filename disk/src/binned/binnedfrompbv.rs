use crate::agg::binnedt::{TBinnerStream, TimeBinnableType};
use crate::agg::streams::StreamItem;
use crate::binned::query::{CacheUsage, PreBinnedQuery};
use crate::binned::RangeCompletableItem;
use crate::cache::{node_ix_for_patch, HttpBodyAsAsyncRead};
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::{decode_frame, FrameType};
use crate::Sitemty;
use err::Error;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use http::{StatusCode, Uri};
use netpod::log::*;
use netpod::{
    x_bin_count, AggKind, BinnedRange, ByteSize, Channel, NodeConfigCached, PerfOpts, PreBinnedPatchIterator, Shape,
};
use serde::de::DeserializeOwned;
use std::future::ready;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct FetchedPreBinned<TBT> {
    uri: Uri,
    resfut: Option<hyper::client::ResponseFuture>,
    res: Option<InMemoryFrameAsyncReadStream<HttpBodyAsAsyncRead>>,
    errored: bool,
    completed: bool,
    _m1: PhantomData<TBT>,
}

impl<TBT> FetchedPreBinned<TBT> {
    pub fn new(query: &PreBinnedQuery, node_config: &NodeConfigCached) -> Result<Self, Error> {
        let nodeix = node_ix_for_patch(&query.patch(), &query.channel(), &node_config.node_config.cluster);
        let node = &node_config.node_config.cluster.nodes[nodeix as usize];
        let uri: hyper::Uri = format!(
            "http://{}:{}/api/4/prebinned?{}",
            node.host,
            node.port,
            query.make_query_string()
        )
        .parse()?;
        let ret = Self {
            uri,
            resfut: None,
            res: None,
            errored: false,
            completed: false,
            _m1: PhantomData,
        };
        Ok(ret)
    }
}

impl<TBT> Stream for FetchedPreBinned<TBT>
where
    TBT: TimeBinnableType,
    Sitemty<TBT>: FrameType + DeserializeOwned,
{
    type Item = Sitemty<TBT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("poll_next on completed");
            } else if self.errored {
                self.completed = true;
                return Ready(None);
            } else if let Some(res) = self.res.as_mut() {
                match res.poll_next_unpin(cx) {
                    Ready(Some(Ok(item))) => match item {
                        StreamItem::Log(item) => Ready(Some(Ok(StreamItem::Log(item)))),
                        StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                        StreamItem::DataItem(item) => match decode_frame::<Sitemty<TBT>>(&item) {
                            Ok(Ok(item)) => Ready(Some(Ok(item))),
                            Ok(Err(e)) => {
                                self.errored = true;
                                Ready(Some(Err(e)))
                            }
                            Err(e) => {
                                self.errored = true;
                                Ready(Some(Err(e)))
                            }
                        },
                    },
                    Ready(Some(Err(e))) => {
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Ready(None) => {
                        self.completed = true;
                        Ready(None)
                    }
                    Pending => Pending,
                }
            } else if let Some(resfut) = self.resfut.as_mut() {
                match resfut.poll_unpin(cx) {
                    Ready(res) => match res {
                        Ok(res) => {
                            if res.status() == StatusCode::OK {
                                let perf_opts = PerfOpts { inmem_bufcap: 512 };
                                let s1 = HttpBodyAsAsyncRead::new(res);
                                let s2 = InMemoryFrameAsyncReadStream::new(s1, perf_opts.inmem_bufcap);
                                self.res = Some(s2);
                                continue 'outer;
                            } else {
                                error!(
                                    "PreBinnedValueFetchedStream  got non-OK result from sub request: {:?}",
                                    res
                                );
                                let e = Error::with_msg(format!(
                                    "PreBinnedValueFetchedStream  got non-OK result from sub request: {:?}",
                                    res
                                ));
                                self.errored = true;
                                Ready(Some(Err(e)))
                            }
                        }
                        Err(e) => {
                            error!("PreBinnedValueStream  error in stream {:?}", e);
                            self.errored = true;
                            Ready(Some(Err(e.into())))
                        }
                    },
                    Pending => Pending,
                }
            } else {
                match hyper::Request::builder()
                    .method(http::Method::GET)
                    .uri(&self.uri)
                    .body(hyper::Body::empty())
                {
                    Ok(req) => {
                        let client = hyper::Client::new();
                        self.resfut = Some(client.request(req));
                        continue 'outer;
                    }
                    Err(e) => {
                        self.errored = true;
                        Ready(Some(Err(e.into())))
                    }
                }
            };
        }
    }
}

/// Generate bins from a range of pre-binned patches.
///
/// Takes an iterator over the necessary patches.
pub struct BinnedFromPreBinned<TBT>
where
    TBT: TimeBinnableType,
{
    // TODO get rid of box:
    inp: Pin<Box<dyn Stream<Item = Sitemty<TBT>> + Send>>,
    _m1: PhantomData<TBT>,
}

impl<TBT> BinnedFromPreBinned<TBT>
where
    TBT: TimeBinnableType<Output = TBT> + Unpin + 'static,
    Sitemty<TBT>: FrameType + DeserializeOwned,
{
    pub fn new(
        patch_it: PreBinnedPatchIterator,
        channel: Channel,
        range: BinnedRange,
        shape: Shape,
        agg_kind: AggKind,
        cache_usage: CacheUsage,
        node_config: &NodeConfigCached,
        disk_stats_every: ByteSize,
        report_error: bool,
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
                let agg_kind = agg_kind.clone();
                let node_config = node_config.clone();
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
                        match FetchedPreBinned::<TBT>::new(&query, &node_config) {
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
        let inp = TBinnerStream::<_, TBT>::new(inp, range, x_bin_count(&shape, &agg_kind));
        Ok(Self {
            inp: Box::pin(inp),
            _m1: PhantomData,
        })
    }
}

impl<TBT> Stream for BinnedFromPreBinned<TBT>
where
    TBT: TimeBinnableType,
{
    type Item = Sitemty<TBT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.inp.poll_next_unpin(cx)
    }
}
