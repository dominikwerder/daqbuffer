use crate::agg::binnedt::TBinnerStream;
use crate::binned::binnedfrompbv::FetchedPreBinned;
use crate::binned::query::PreBinnedQuery;
use crate::binned::WithLen;
use crate::cache::{write_pb_cache_min_max_avg_scalar, CacheFileDesc, WrittenPbCache};
use crate::decode::{Endianness, EventValueFromBytes, EventValueShape, NumFromBytes};
use crate::merge::mergedfromremotes::MergedFromRemotes;
use crate::streamlog::Streamlog;
use err::Error;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use items::numops::NumOps;
use items::{
    Appendable, Clearable, EventsNodeProcessor, EventsTypeAliases, FrameType, PushableIndex, RangeCompletableItem,
    ReadableFromFile, Sitemty, StreamItem, TimeBinnableType,
};
use netpod::log::*;
use netpod::query::{CacheUsage, RawEventsQuery};
use netpod::{
    x_bin_count, AggKind, BinnedRange, NodeConfigCached, PerfOpts, PreBinnedPatchIterator, PreBinnedPatchRange, Shape,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::{File, OpenOptions};

pub struct PreBinnedValueStream<NTY, END, EVS, ENP>
where
    NTY: NumOps + NumFromBytes<NTY, END> + Serialize + 'static,
    END: Endianness + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
    ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Batch>,
{
    query: PreBinnedQuery,
    shape: Shape,
    agg_kind: AggKind,
    node_config: NodeConfigCached,
    open_check_local_file: Option<Pin<Box<dyn Future<Output = Result<File, io::Error>> + Send>>>,
    fut2: Option<Pin<Box<dyn Stream<Item = Sitemty<<ENP as EventsTypeAliases>::TimeBinOutput>> + Send>>>,
    read_from_cache: bool,
    cache_written: bool,
    data_complete: bool,
    range_complete_observed: bool,
    range_complete_emitted: bool,
    errored: bool,
    completed: bool,
    streamlog: Streamlog,
    values: Option<<ENP as EventsTypeAliases>::TimeBinOutput>,
    write_fut: Option<Pin<Box<dyn Future<Output = Result<WrittenPbCache, Error>> + Send>>>,
    read_cache_fut: Option<Pin<Box<dyn Future<Output = Sitemty<<ENP as EventsTypeAliases>::TimeBinOutput>> + Send>>>,
    _m1: PhantomData<NTY>,
    _m2: PhantomData<END>,
    _m3: PhantomData<EVS>,
    _m4: PhantomData<ENP>,
}

impl<NTY, END, EVS, ENP> PreBinnedValueStream<NTY, END, EVS, ENP>
where
    NTY: NumOps + NumFromBytes<NTY, END> + Serialize + 'static,
    END: Endianness + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
    ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Batch> + 'static,
    <ENP as EventsNodeProcessor>::Output: PushableIndex + Appendable + Clearable,
    // TODO is this needed:
    Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType,
    // TODO who exactly needs this DeserializeOwned?
    Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>: FrameType + DeserializeOwned,
{
    pub fn new(query: PreBinnedQuery, shape: Shape, agg_kind: AggKind, node_config: &NodeConfigCached) -> Self {
        Self {
            query,
            shape,
            agg_kind,
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
            // TODO use alias via some trait associated type:
            //values: <<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output as Appendable>::empty(),
            values: None,
            write_fut: None,
            read_cache_fut: None,
            _m1: PhantomData,
            _m2: PhantomData,
            _m3: PhantomData,
            _m4: PhantomData,
        }
    }

    fn setup_merged_from_remotes(
        &mut self,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>> + Send>>,
        Error,
    > {
        let evq = RawEventsQuery {
            channel: self.query.channel().clone(),
            range: self.query.patch().patch_range(),
            agg_kind: self.query.agg_kind().clone(),
            disk_io_buffer_size: self.query.disk_io_buffer_size(),
            do_decompress: true,
        };
        if self.query.patch().patch_t_len() % self.query.patch().bin_t_len() != 0 {
            let msg = format!(
                "Patch length inconsistency  {}  {}",
                self.query.patch().patch_t_len(),
                self.query.patch().bin_t_len()
            );
            error!("{}", msg);
            return Err(Error::with_msg(msg));
        }
        // TODO do I need to set up more transformations or binning to deliver the requested data?
        let count = self.query.patch().patch_t_len() / self.query.patch().bin_t_len();
        let range = BinnedRange::covering_range(evq.range.clone(), count as u32)?
            .ok_or(Error::with_msg("covering_range returns None"))?;
        let perf_opts = PerfOpts { inmem_bufcap: 512 };
        let s = MergedFromRemotes::<ENP>::new(evq, perf_opts, self.node_config.node_config.cluster.clone()).map(|k| {
            info!(
                "setup_merged_from_remotes, MergedFromRemotes  yields {:?}",
                //show_event_basic_info(&k)
                "TODO show_event_basic_info"
            );
            k
        });
        let ret = TBinnerStream::<_, <ENP as EventsNodeProcessor>::Output>::new(
            s,
            range,
            x_bin_count(&self.shape, &self.agg_kind),
            self.agg_kind.do_time_weighted(),
        );
        Ok(Box::pin(ret))
    }

    fn setup_from_higher_res_prebinned(
        &mut self,
        range: PreBinnedPatchRange,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>> + Send>>,
        Error,
    > {
        let g = self.query.patch().bin_t_len();
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
            let msg = format!("try_setup_fetch_prebinned_higher_res  g {}  h {}", g, h);
            return Err(Error::with_msg(msg));
        }
        if g / h > 1024 * 10 {
            let msg = format!("try_setup_fetch_prebinned_higher_res  g {}  h {}", g, h);
            return Err(Error::with_msg(msg));
        }
        if g % h != 0 {
            let msg = format!("try_setup_fetch_prebinned_higher_res  g {}  h {}", g, h);
            return Err(Error::with_msg(msg));
        }
        let node_config = self.node_config.clone();
        let patch_it = PreBinnedPatchIterator::from_range(range);
        let s = futures_util::stream::iter(patch_it)
            .map({
                let q2 = self.query.clone();
                let disk_io_buffer_size = self.query.disk_io_buffer_size();
                let disk_stats_every = self.query.disk_stats_every().clone();
                let report_error = self.query.report_error();
                move |patch| {
                    let query = PreBinnedQuery::new(
                        patch,
                        q2.channel().clone(),
                        q2.agg_kind().clone(),
                        q2.cache_usage().clone(),
                        disk_io_buffer_size,
                        disk_stats_every.clone(),
                        report_error,
                    );
                    let ret =
                        FetchedPreBinned::<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>::new(
                            &query,
                            &node_config,
                        )?;
                    Ok(ret)
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
        Ok(Box::pin(s))
    }

    fn try_setup_fetch_prebinned_higher_res(&mut self) -> Result<(), Error> {
        info!("try_setup_fetch_prebinned_higher_res");
        let range = self.query.patch().patch_range();
        match PreBinnedPatchRange::covering_range(range, self.query.patch().bin_count() + 1) {
            Ok(Some(range)) => {
                self.fut2 = Some(self.setup_from_higher_res_prebinned(range)?);
            }
            Ok(None) => {
                self.fut2 = Some(self.setup_merged_from_remotes()?);
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }
}

impl<NTY, END, EVS, ENP> Stream for PreBinnedValueStream<NTY, END, EVS, ENP>
where
    NTY: NumOps + NumFromBytes<NTY, END> + Serialize + Unpin + 'static,
    END: Endianness + Unpin + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + Unpin + 'static,
    ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Batch> + Unpin + 'static,
    <ENP as EventsNodeProcessor>::Output: PushableIndex + Appendable + Clearable,
    // TODO needed?
    Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType,
    Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>: FrameType + DeserializeOwned,
{
    type Item = Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>;

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
                            Ok(res) => {
                                self.streamlog.append(
                                    Level::INFO,
                                    format!(
                                        "cache file written  bytes: {}  duration {} ms",
                                        res.bytes,
                                        res.duration.as_millis()
                                    ),
                                );
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
                    match self.query.cache_usage() {
                        CacheUsage::Use | CacheUsage::Recreate => {
                            if let Some(values) = self.values.take() {
                                let msg = format!(
                                    "write cache file  query: {:?}  bin count: {}",
                                    self.query.patch(),
                                    values.len(),
                                );
                                self.streamlog.append(Level::INFO, msg);
                                let fut = write_pb_cache_min_max_avg_scalar(
                                    values,
                                    self.query.patch().clone(),
                                    self.query.agg_kind().clone(),
                                    self.query.channel().clone(),
                                    self.node_config.clone(),
                                );
                                self.write_fut = Some(Box::pin(fut));
                                continue 'outer;
                            } else {
                                warn!("no values to write to cache");
                                continue 'outer;
                            }
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
                                    if let Some(values) = &mut self.values {
                                        values.append(&item);
                                    } else {
                                        let mut values = item.empty_like_self();
                                        values.append(&item);
                                        self.values = Some(values);
                                    }
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
                                let fut =
                                    <<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output as ReadableFromFile>::read_from_file(file)?;
                                self.read_cache_fut = Some(Box::pin(fut));
                                continue 'outer;
                            }
                            Err(e) => match e.kind() {
                                // TODO other error kinds
                                io::ErrorKind::NotFound => match self.try_setup_fetch_prebinned_higher_res() {
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
                                    error!("File I/O error:  kind {:?}  {:?}\n\n..............", e.kind(), e);
                                    self.errored = true;
                                    Ready(Some(Err(e.into())))
                                }
                            },
                        }
                    }
                    Pending => Pending,
                }
            } else {
                let cfd = CacheFileDesc::new(
                    self.query.channel().clone(),
                    self.query.patch().clone(),
                    self.query.agg_kind().clone(),
                );
                let path = match self.query.cache_usage() {
                    CacheUsage::Use => cfd.path(&self.node_config),
                    _ => PathBuf::from("DOESNOTEXIST"),
                };
                let fut = async { OpenOptions::new().read(true).open(path).await };
                self.open_check_local_file = Some(Box::pin(fut));
                continue 'outer;
            };
        }
    }
}
