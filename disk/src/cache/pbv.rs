use crate::agg::binnedt::IntoBinnedT;
use crate::agg::scalarbinbatch::{MinMaxAvgScalarBinBatch, MinMaxAvgScalarBinBatchStreamItem};
use crate::cache::pbvfs::{PreBinnedItem, PreBinnedValueFetchedStream};
use crate::cache::{CacheFileDesc, MergedFromRemotes, PreBinnedQuery};
use crate::frame::makeframe::make_frame;
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

pub type PreBinnedValueByteStream = SCC<PreBinnedValueByteStreamInner>;

pub struct PreBinnedValueByteStreamInner {
    inp: PreBinnedValueStream,
}

pub fn pre_binned_value_byte_stream_new(
    query: &PreBinnedQuery,
    node_config: &NodeConfigCached,
) -> PreBinnedValueByteStream {
    let s1 = PreBinnedValueStream::new(query.clone(), node_config);
    let s2 = PreBinnedValueByteStreamInner { inp: s1 };
    SCC::new(s2)
}

impl Stream for PreBinnedValueByteStreamInner {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => match make_frame::<Result<PreBinnedItem, Error>>(&item) {
                Ok(buf) => Ready(Some(Ok(buf.freeze()))),
                Err(e) => Ready(Some(Err(e.into()))),
            },
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub struct PreBinnedValueStream {
    query: PreBinnedQuery,
    node_config: NodeConfigCached,
    open_check_local_file: Option<Pin<Box<dyn Future<Output = Result<tokio::fs::File, std::io::Error>> + Send>>>,
    fut2: Option<Pin<Box<dyn Stream<Item = Result<PreBinnedItem, Error>> + Send>>>,
    read_from_cache: bool,
    cache_written: bool,
    data_complete: bool,
    range_complete_observed: bool,
    range_complete_emitted: bool,
    errored: bool,
    completed: bool,
    streamlog: Streamlog,
    values: MinMaxAvgScalarBinBatch,
    write_fut: Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
    read_cache_fut: Option<Pin<Box<dyn Future<Output = Result<PreBinnedItem, Error>> + Send>>>,
}

impl PreBinnedValueStream {
    pub fn new(query: PreBinnedQuery, node_config: &NodeConfigCached) -> Self {
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
            values: MinMaxAvgScalarBinBatch::empty(),
            write_fut: None,
            read_cache_fut: None,
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
        let range = BinnedRange::covering_range(evq.range.clone(), count).unwrap();
        let perf_opts = PerfOpts { inmem_bufcap: 512 };
        let s1 = MergedFromRemotes::new(evq, perf_opts, self.node_config.node_config.cluster.clone());
        let s1 = s1.into_binned_t(range);
        let s1 = s1.map(|k| {
            use MinMaxAvgScalarBinBatchStreamItem::*;
            match k {
                Ok(Values(k)) => Ok(PreBinnedItem::Batch(k)),
                Ok(RangeComplete) => Ok(PreBinnedItem::RangeComplete),
                Ok(EventDataReadStats(stats)) => {
                    info!("PreBinnedValueStream  ✙ ✙ ✙ ✙ ✙ ✙ ✙ ✙ ✙ ✙ ✙ ✙ ✙    stats {:?}", stats);
                    Ok(PreBinnedItem::EventDataReadStats(stats))
                }
                Ok(Log(item)) => Ok(PreBinnedItem::Log(item)),
                Err(e) => Err(e),
            }
        });
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
        if g / h > 200 {
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
                move |patch| {
                    let query = PreBinnedQuery {
                        patch,
                        channel: q2.channel.clone(),
                        agg_kind: q2.agg_kind.clone(),
                        cache_usage: q2.cache_usage.clone(),
                    };
                    PreBinnedValueFetchedStream::new(&query, &node_config)
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

    fn try_setup_fetch_prebinned_higher_res(&mut self) {
        trace!("try_setup_fetch_prebinned_higher_res  for {:?}", self.query.patch);
        let range = self.query.patch.patch_range();
        match PreBinnedPatchRange::covering_range(range, self.query.patch.bin_count() + 1) {
            Some(range) => {
                self.setup_from_higher_res_prebinned(range);
            }
            None => {
                self.setup_merged_from_remotes();
            }
        }
    }
}

impl Stream for PreBinnedValueStream {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = Result<PreBinnedItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("PreBinnedValueStream  poll_next on completed");
            } else if self.errored {
                self.completed = true;
                Ready(None)
            } else if let Some(item) = self.streamlog.pop() {
                Ready(Some(Ok(PreBinnedItem::Log(item))))
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
                        Ready(Some(Ok(PreBinnedItem::RangeComplete)))
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
                                "Write cache file\n{:?}\nN: {}\n\n\n",
                                self.query.patch,
                                self.values.ts1s.len()
                            );
                            self.streamlog.append(Level::INFO, msg);
                            let values = std::mem::replace(&mut self.values, MinMaxAvgScalarBinBatch::empty());
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
                        Ok(PreBinnedItem::RangeComplete) => {
                            self.range_complete_observed = true;
                            continue 'outer;
                        }
                        Ok(PreBinnedItem::Batch(batch)) => {
                            self.values.ts1s.extend(batch.ts1s.iter());
                            self.values.ts2s.extend(batch.ts2s.iter());
                            self.values.counts.extend(batch.counts.iter());
                            self.values.mins.extend(batch.mins.iter());
                            self.values.maxs.extend(batch.maxs.iter());
                            self.values.avgs.extend(batch.avgs.iter());
                            Ready(Some(Ok(PreBinnedItem::Batch(batch))))
                        }
                        Ok(PreBinnedItem::EventDataReadStats(stats)) => {
                            info!("PreBinnedValueStream  as Stream  seeing stats {:?}", stats);
                            Ready(Some(Ok(PreBinnedItem::EventDataReadStats(stats))))
                        }
                        Ok(PreBinnedItem::Log(item)) => Ready(Some(Ok(PreBinnedItem::Log(item)))),
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
                                let fut = super::read_pbv(file);
                                self.read_cache_fut = Some(Box::pin(fut));
                                continue 'outer;
                            }
                            Err(e) => match e.kind() {
                                std::io::ErrorKind::NotFound => {
                                    self.try_setup_fetch_prebinned_higher_res();
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
