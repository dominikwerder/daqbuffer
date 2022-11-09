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
    Appendable, Clearable, EventsNodeProcessor, EventsTypeAliases, FrameDecodable, FrameType, PushableIndex,
    RangeCompletableItem, ReadableFromFile, Sitemty, StreamItem, TimeBinnableType,
};
use netpod::log::*;
use netpod::query::{CacheUsage, RawEventsQuery};
use netpod::x_bin_count;
use netpod::{AggKind, BinnedRange, PreBinnedPatchIterator, PreBinnedPatchRange};
use netpod::{NodeConfigCached, PerfOpts};
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
    agg_kind: AggKind,
    node_config: NodeConfigCached,
    open_check_local_file: Option<Pin<Box<dyn Future<Output = Result<File, io::Error>> + Send>>>,
    stream_from_other_inputs:
        Option<Pin<Box<dyn Stream<Item = Sitemty<<ENP as EventsTypeAliases>::TimeBinOutput>> + Send>>>,
    read_from_cache: bool,
    cache_written: bool,
    data_complete: bool,
    range_complete_observed: bool,
    range_complete_emitted: bool,
    errored: bool,
    all_done: bool,
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
    Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>: FrameType + FrameDecodable,
{
    pub fn new(query: PreBinnedQuery, agg_kind: AggKind, node_config: &NodeConfigCached) -> Self {
        Self {
            query,
            agg_kind,
            node_config: node_config.clone(),
            open_check_local_file: None,
            stream_from_other_inputs: None,
            read_from_cache: false,
            cache_written: false,
            data_complete: false,
            range_complete_observed: false,
            range_complete_emitted: false,
            errored: false,
            all_done: false,
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
        // TODO let PreBinnedQuery provide the tune and pass to RawEventsQuery:
        let evq = RawEventsQuery::new(
            self.query.channel().clone(),
            self.query.patch().patch_range(),
            self.query.agg_kind().clone(),
        );
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
        let range = BinnedRange::covering_range(evq.range.clone(), count as u32)?;
        let perf_opts = PerfOpts { inmem_bufcap: 512 };
        let s = MergedFromRemotes::<ENP>::new(evq, perf_opts, self.node_config.node_config.cluster.clone());
        let ret = TBinnerStream::<_, <ENP as EventsNodeProcessor>::Output>::new(
            s,
            range,
            x_bin_count(&self.query.shape().clone(), &self.agg_kind),
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
                        q2.scalar_type().clone(),
                        q2.shape().clone(),
                        q2.agg_kind().clone(),
                        q2.cache_usage().clone(),
                        disk_io_buffer_size,
                        disk_stats_every.clone(),
                        report_error,
                    );
                    let nodeix = crate::cache::node_ix_for_patch(
                        &query.patch(),
                        &query.channel(),
                        &node_config.node_config.cluster,
                    );
                    let node = &node_config.node_config.cluster.nodes[nodeix as usize];
                    let ret =
                        FetchedPreBinned::<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>::new(
                            &query,
                            node.host.clone(),
                            node.port.clone(),
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
                self.stream_from_other_inputs = Some(self.setup_from_higher_res_prebinned(range)?);
            }
            Ok(None) => {
                self.stream_from_other_inputs = Some(self.setup_merged_from_remotes()?);
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }

    fn poll_write_fut(
        self: &mut Self,
        mut fut: Pin<Box<dyn Future<Output = Result<WrittenPbCache, Error>> + Send>>,
        cx: &mut Context,
    ) -> Poll<Option<Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>>> {
        trace!("poll_write_fut");
        use Poll::*;
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
                        self.all_done = true;
                        Ready(None)
                    }
                    Err(e) => {
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                }
            }
            Pending => {
                self.write_fut = Some(fut);
                Pending
            }
        }
    }

    fn poll_read_cache_fut(
        self: &mut Self,
        mut fut: Pin<
            Box<
                dyn Future<
                        Output = Result<
                            StreamItem<RangeCompletableItem<<ENP as EventsTypeAliases>::TimeBinOutput>>,
                            Error,
                        >,
                    > + Send,
            >,
        >,
        cx: &mut Context,
    ) -> Poll<Option<Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>>> {
        trace!("poll_read_cache_fut");
        use Poll::*;
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
            Pending => {
                self.read_cache_fut = Some(fut);
                Pending
            }
        }
    }

    fn handle_data_complete(
        self: &mut Self,
    ) -> Poll<Option<Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>>> {
        trace!("handle_data_complete");
        use Poll::*;
        if self.cache_written {
            // TODO can we ever get here?
            if self.range_complete_observed {
                self.range_complete_emitted = true;
                let item = RangeCompletableItem::RangeComplete;
                Ready(Some(Ok(StreamItem::DataItem(item))))
            } else {
                self.all_done = true;
                Ready(None)
            }
        } else if self.read_from_cache {
            // TODO refactor: raising cache_written even though we did not actually write is misleading.
            self.cache_written = true;
            self.all_done = true;
            Ready(None)
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
                        Ready(None)
                    } else {
                        warn!("no values to write to cache");
                        Ready(None)
                    }
                }
                _ => {
                    // TODO refactor: raising cache_written even though we did not actually write is misleading.
                    self.cache_written = true;
                    self.all_done = true;
                    Ready(None)
                }
            }
        }
    }

    fn poll_stream_from_other_inputs(
        self: &mut Self,
        mut fut: Pin<
            Box<
                dyn Stream<
                        Item = Result<
                            StreamItem<RangeCompletableItem<<ENP as EventsTypeAliases>::TimeBinOutput>>,
                            Error,
                        >,
                    > + Send,
            >,
        >,
        cx: &mut Context,
    ) -> Poll<Option<Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>>> {
        use Poll::*;
        match fut.poll_next_unpin(cx) {
            Ready(Some(k)) => match k {
                Ok(item) => {
                    self.stream_from_other_inputs = Some(fut);
                    match item {
                        StreamItem::Log(item) => Ready(Some(Ok(StreamItem::Log(item)))),
                        StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                        StreamItem::DataItem(item) => match item {
                            RangeCompletableItem::RangeComplete => {
                                self.range_complete_observed = true;
                                Ready(None)
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
                    }
                }
                Err(e) => {
                    self.errored = true;
                    Ready(Some(Err(e)))
                }
            },
            Ready(None) => {
                self.data_complete = true;
                Ready(None)
            }
            Pending => {
                self.stream_from_other_inputs = Some(fut);
                Pending
            }
        }
    }

    fn poll_open_check_local_file(
        self: &mut Self,
        mut fut: Pin<Box<dyn Future<Output = Result<File, io::Error>> + Send>>,
        cx: &mut Context,
    ) -> Poll<Option<Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>>> {
        use Poll::*;
        match fut.poll_unpin(cx) {
            Ready(item) => {
                match item {
                    Ok(file) => {
                        self.read_from_cache = true;
                        let fut =
                            <<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output as ReadableFromFile>::read_from_file(file)?;
                        self.read_cache_fut = Some(Box::pin(fut));
                        // Return Ready(None) to signal that nothing is Pending but we need to get polled again.
                        //continue 'outer;
                        Ready(None)
                    }
                    Err(e) => match e.kind() {
                        // TODO other error kinds
                        io::ErrorKind::NotFound => match self.try_setup_fetch_prebinned_higher_res() {
                            Ok(_) => {
                                if self.stream_from_other_inputs.is_none() {
                                    let e =
                                        Err(Error::with_msg(format!("try_setup_fetch_prebinned_higher_res  failed")));
                                    self.errored = true;
                                    Ready(Some(e))
                                } else {
                                    //continue 'outer;
                                    Ready(None)
                                }
                            }
                            Err(e) => {
                                let e =
                                    Error::with_msg(format!("try_setup_fetch_prebinned_higher_res  error: {:?}", e));
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
            Pending => {
                self.open_check_local_file = Some(fut);
                Pending
            }
        }
    }
}

macro_rules! some_or_continue {
    ($x:expr) => {
        if let Ready(None) = $x {
            continue;
        } else {
            $x
        }
    };
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
    Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>: FrameType + FrameDecodable,
{
    type Item = Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.completed {
                panic!("PreBinnedValueStream  poll_next on completed");
            } else if self.errored {
                self.completed = true;
                Ready(None)
            } else if self.all_done {
                self.completed = true;
                Ready(None)
            } else if let Some(item) = self.streamlog.pop() {
                Ready(Some(Ok(StreamItem::Log(item))))
            } else if let Some(fut) = self.write_fut.take() {
                let x = Self::poll_write_fut(&mut self, fut, cx);
                some_or_continue!(x)
            } else if let Some(fut) = self.read_cache_fut.take() {
                let x = Self::poll_read_cache_fut(&mut self, fut, cx);
                some_or_continue!(x)
            } else if self.range_complete_emitted {
                self.completed = true;
                Ready(None)
            } else if self.data_complete {
                let x = Self::handle_data_complete(&mut self);
                some_or_continue!(x)
            } else if let Some(fut) = self.stream_from_other_inputs.take() {
                let x = Self::poll_stream_from_other_inputs(&mut self, fut, cx);
                some_or_continue!(x)
            } else if let Some(fut) = self.open_check_local_file.take() {
                let x = Self::poll_open_check_local_file(&mut self, fut, cx);
                some_or_continue!(x)
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
                continue;
            };
        }
    }
}
