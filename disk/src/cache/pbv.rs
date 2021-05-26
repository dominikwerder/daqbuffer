use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::{Collectable, Collected, StreamItem};
use crate::binned::RangeCompletableItem::RangeComplete;
use crate::binned::{BinnedStreamKind, RangeCompletableItem};
use crate::cache::pbvfs::PreBinnedScalarValueFetchedStream;
use crate::cache::{CacheFileDesc, MergedFromRemotes, PreBinnedQuery};
use crate::frame::makeframe::{make_frame, FrameType};
use crate::raw::EventsQuery;
use crate::streamlog::Streamlog;
use bytes::Bytes;
use err::Error;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use netpod::log::*;
use netpod::streamext::SCC;
use netpod::{BinnedRange, NodeConfigCached, PerfOpts, PreBinnedPatchIterator, PreBinnedPatchRange};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;

pub type PreBinnedValueByteStream<BK> = SCC<PreBinnedValueByteStreamInner<BK>>;

pub struct PreBinnedValueByteStreamInner<SK>
where
    SK: BinnedStreamKind,
{
    inp: PreBinnedValueStream<SK>,
}

pub fn pre_binned_value_byte_stream_new<SK>(
    query: &PreBinnedQuery,
    node_config: &NodeConfigCached,
    stream_kind: SK,
) -> PreBinnedValueByteStream<SK>
where
    SK: BinnedStreamKind,
    Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::TBinnedBins>>, err::Error>: FrameType,
{
    let s1 = PreBinnedValueStream::new(query.clone(), node_config, stream_kind);
    let s2 = PreBinnedValueByteStreamInner { inp: s1 };
    SCC::new(s2)
}

impl<SK> Stream for PreBinnedValueByteStreamInner<SK>
where
    SK: BinnedStreamKind,
    Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::TBinnedBins>>, err::Error>: FrameType,
{
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => {
                match make_frame::<
                    Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::TBinnedBins>>, err::Error>,
                >(&item)
                {
                    Ok(buf) => Ready(Some(Ok(buf.freeze()))),
                    Err(e) => Ready(Some(Err(e.into()))),
                }
            }
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub struct PreBinnedValueStream<SK>
where
    SK: BinnedStreamKind,
{
    query: PreBinnedQuery,
    node_config: NodeConfigCached,
    open_check_local_file: Option<Pin<Box<dyn Future<Output = Result<File, std::io::Error>> + Send>>>,
    fut2: Option<
        Pin<
            Box<
                dyn Stream<
                        Item = Result<
                            StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::TBinnedBins>>,
                            err::Error,
                        >,
                    > + Send,
            >,
        >,
    >,
    read_from_cache: bool,
    cache_written: bool,
    data_complete: bool,
    range_complete_observed: bool,
    range_complete_emitted: bool,
    errored: bool,
    completed: bool,
    streamlog: Streamlog,
    values: <SK as BinnedStreamKind>::TBinnedBins,
    write_fut: Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
    read_cache_fut: Option<
        Pin<
            Box<
                dyn Future<
                        Output = Result<
                            StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::TBinnedBins>>,
                            err::Error,
                        >,
                    > + Send,
            >,
        >,
    >,
    stream_kind: SK,
}

impl<SK> PreBinnedValueStream<SK>
where
    SK: BinnedStreamKind,
    Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::TBinnedBins>>, err::Error>: FrameType,
{
    pub fn new(query: PreBinnedQuery, node_config: &NodeConfigCached, stream_kind: SK) -> Self {
        Self {
            query,
            node_config: node_config.clone(),
            open_check_local_file: None,
            fut2: None,
            read_from_cache: false,
            cache_written: false,
            data_complete: false,
            range_complete_observed: false,
            range_complete_emitted: false,
            errored: false,
            completed: false,
            streamlog: Streamlog::new(node_config.ix as u32),
            // TODO refactor usage of parameter
            values: <<SK as BinnedStreamKind>::TBinnedBins as Collected>::new(0),
            write_fut: None,
            read_cache_fut: None,
            stream_kind,
        }
    }

    // TODO handle errors also here via return type.
    fn setup_merged_from_remotes(&mut self) {
        let evq = EventsQuery {
            channel: self.query.channel.clone(),
            range: self.query.patch.patch_range(),
            agg_kind: self.query.agg_kind.clone(),
        };
        if self.query.patch.patch_t_len() % self.query.patch.bin_t_len() != 0 {
            error!(
                "Patch length inconsistency  {}  {}",
                self.query.patch.patch_t_len(),
                self.query.patch.bin_t_len()
            );
            return;
        }
        // TODO do I need to set up more transformations or binning to deliver the requested data?
        let count = self.query.patch.patch_t_len() / self.query.patch.bin_t_len();
        let range = BinnedRange::covering_range(evq.range.clone(), count)
            .unwrap()
            .ok_or(Error::with_msg("covering_range returns None"))
            .unwrap();
        let perf_opts = PerfOpts { inmem_bufcap: 512 };
        let s1 = MergedFromRemotes::new(
            evq,
            perf_opts,
            self.node_config.node_config.cluster.clone(),
            self.stream_kind.clone(),
        );
        let s1 = <SK as BinnedStreamKind>::xbinned_to_tbinned(s1, range);
        self.fut2 = Some(Box::pin(s1));
    }

    fn setup_from_higher_res_prebinned(&mut self, range: PreBinnedPatchRange) {
        let g = self.query.patch.bin_t_len();
        let h = range.grid_spec.bin_t_len();
        trace!(
            "try_setup_fetch_prebinned_higher_res  found  g {}  h {}  ratio {}  mod {}  {:?}",
            g,
            h,
            g / h,
            g % h,
            range,
        );
        if g / h <= 1 {
            error!("try_setup_fetch_prebinned_higher_res  g {}  h {}", g, h);
            return;
        }
        if g / h > 1024 * 10 {
            error!("try_setup_fetch_prebinned_higher_res  g {}  h {}", g, h);
            return;
        }
        if g % h != 0 {
            error!("try_setup_fetch_prebinned_higher_res  g {}  h {}", g, h);
            return;
        }
        let node_config = self.node_config.clone();
        let patch_it = PreBinnedPatchIterator::from_range(range);
        let s = futures_util::stream::iter(patch_it)
            .map({
                let q2 = self.query.clone();
                let disk_stats_every = self.query.disk_stats_every.clone();
                let stream_kind = self.stream_kind.clone();
                move |patch| {
                    let query = PreBinnedQuery {
                        patch,
                        channel: q2.channel.clone(),
                        agg_kind: q2.agg_kind.clone(),
                        cache_usage: q2.cache_usage.clone(),
                        disk_stats_every: disk_stats_every.clone(),
                    };
                    PreBinnedScalarValueFetchedStream::new(&query, &node_config, &stream_kind)
                }
            })
            .map(|k| {
                let s: Pin<Box<dyn Stream<Item = _> + Send>> = match k {
                    Ok(k) => Box::pin(k),
                    Err(e) => Box::pin(futures_util::stream::iter(vec![Err(e)])),
                };
                s
            })
            .flatten();
        self.fut2 = Some(Box::pin(s));
    }

    fn try_setup_fetch_prebinned_higher_res(&mut self) -> Result<(), Error> {
        let range = self.query.patch.patch_range();
        match PreBinnedPatchRange::covering_range(range, self.query.patch.bin_count() + 1) {
            Ok(Some(range)) => {
                self.setup_from_higher_res_prebinned(range);
            }
            Ok(None) => {
                self.setup_merged_from_remotes();
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }
}

impl<SK> Stream for PreBinnedValueStream<SK>
where
    SK: BinnedStreamKind + Unpin,
    Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::TBinnedBins>>, err::Error>: FrameType,
{
    type Item = Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::TBinnedBins>>, err::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("PreBinnedValueStream  poll_next on completed");
            } else if self.errored {
                self.completed = true;
                Ready(None)
            } else if let Some(item) = self.streamlog.pop() {
                Ready(Some(Ok(StreamItem::Log(item))))
            } else if let Some(fut) = &mut self.write_fut {
                match fut.poll_unpin(cx) {
                    Ready(item) => {
                        self.cache_written = true;
                        self.write_fut = None;
                        match item {
                            Ok(()) => {
                                self.streamlog.append(Level::INFO, format!("cache file written"));
                                continue 'outer;
                            }
                            Err(e) => {
                                self.errored = true;
                                Ready(Some(Err(e)))
                            }
                        }
                    }
                    Pending => Pending,
                }
            } else if let Some(fut) = &mut self.read_cache_fut {
                match fut.poll_unpin(cx) {
                    Ready(item) => {
                        self.read_cache_fut = None;
                        match item {
                            Ok(item) => {
                                self.data_complete = true;
                                self.range_complete_observed = true;
                                Ready(Some(Ok(item)))
                            }
                            Err(e) => {
                                self.errored = true;
                                Ready(Some(Err(e)))
                            }
                        }
                    }
                    Pending => Pending,
                }
            } else if self.range_complete_emitted {
                self.completed = true;
                Ready(None)
            } else if self.data_complete {
                if self.cache_written {
                    if self.range_complete_observed {
                        self.range_complete_emitted = true;
                        let item = RangeCompletableItem::RangeComplete;
                        Ready(Some(Ok(StreamItem::DataItem(item))))
                    } else {
                        self.completed = true;
                        Ready(None)
                    }
                } else if self.read_from_cache {
                    self.cache_written = true;
                    continue 'outer;
                } else {
                    match self.query.cache_usage {
                        super::CacheUsage::Use | super::CacheUsage::Recreate => {
                            let msg = format!(
                                "write cache file  query: {:?}  bin count: {}",
                                self.query.patch,
                                //self.values.ts1s.len()
                                // TODO create trait to extract number of bins from item:
                                0
                            );
                            self.streamlog.append(Level::INFO, msg);
                            let values = std::mem::replace(
                                &mut self.values,
                                // Do not use expectation on the number of bins here:
                                <<SK as BinnedStreamKind>::TBinnedBins as Collected>::new(0),
                            );
                            let fut = super::write_pb_cache_min_max_avg_scalar(
                                values,
                                self.query.patch.clone(),
                                self.query.agg_kind.clone(),
                                self.query.channel.clone(),
                                self.node_config.clone(),
                            );
                            self.write_fut = Some(Box::pin(fut));
                            continue 'outer;
                        }
                        _ => {
                            self.cache_written = true;
                            continue 'outer;
                        }
                    }
                }
            } else if let Some(fut) = self.fut2.as_mut() {
                match fut.poll_next_unpin(cx) {
                    Ready(Some(k)) => match k {
                        Ok(item) => match item {
                            StreamItem::Log(item) => Ready(Some(Ok(StreamItem::Log(item)))),
                            StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                            StreamItem::DataItem(item) => match item {
                                RangeCompletableItem::RangeComplete => {
                                    self.range_complete_observed = true;
                                    continue 'outer;
                                }
                                RangeCompletableItem::Data(item) => {
                                    // TODO need trait Appendable which simply appends to the same type, so that I can
                                    // write later the whole batch of numbers in one go.
                                    err::todo();
                                    //item.append_to(&mut self.values);
                                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))))
                                }
                            },
                        },
                        Err(e) => {
                            self.errored = true;
                            Ready(Some(Err(e)))
                        }
                    },
                    Ready(None) => {
                        self.data_complete = true;
                        continue 'outer;
                    }
                    Pending => Pending,
                }
            } else if let Some(fut) = self.open_check_local_file.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(item) => {
                        self.open_check_local_file = None;
                        match item {
                            Ok(file) => {
                                self.read_from_cache = true;
                                use crate::binned::ReadableFromFile;
                                let fut = <<SK as BinnedStreamKind>::TBinnedBins as crate::binned::ReadableFromFile>::read_from_file(file)?;
                                self.read_cache_fut = Some(Box::pin(fut));
                                continue 'outer;
                            }
                            Err(e) => match e.kind() {
                                std::io::ErrorKind::NotFound => match self.try_setup_fetch_prebinned_higher_res() {
                                    Ok(_) => {
                                        if self.fut2.is_none() {
                                            let e = Err(Error::with_msg(format!(
                                                "try_setup_fetch_prebinned_higher_res  failed"
                                            )));
                                            self.errored = true;
                                            Ready(Some(e))
                                        } else {
                                            continue 'outer;
                                        }
                                    }
                                    Err(e) => {
                                        let e = Error::with_msg(format!(
                                            "try_setup_fetch_prebinned_higher_res  error: {:?}",
                                            e
                                        ));
                                        self.errored = true;
                                        Ready(Some(Err(e)))
                                    }
                                },
                                _ => {
                                    error!("File I/O error: {:?}", e);
                                    self.errored = true;
                                    Ready(Some(Err(e.into())))
                                }
                            },
                        }
                    }
                    Pending => Pending,
                }
            } else {
                let cfd = CacheFileDesc {
                    channel: self.query.channel.clone(),
                    patch: self.query.patch.clone(),
                    agg_kind: self.query.agg_kind.clone(),
                };
                use super::CacheUsage;
                let path = match self.query.cache_usage {
                    CacheUsage::Use => cfd.path(&self.node_config),
                    _ => PathBuf::from("DOESNOTEXIST"),
                };
                let fut = async { tokio::fs::OpenOptions::new().read(true).open(path).await };
                self.open_check_local_file = Some(Box::pin(fut));
                continue 'outer;
            };
        }
    }
}
