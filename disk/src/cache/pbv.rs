use crate::agg::binnedt::IntoBinnedT;
use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::cache::pbvfs::{PreBinnedHttpFrame, PreBinnedValueFetchedStream};
use crate::cache::{node_ix_for_patch, MergedFromRemotes};
use crate::frame::makeframe::make_frame;
use crate::raw::EventsQuery;
use bytes::Bytes;
use err::Error;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt, TryStreamExt};
use netpod::log::*;
use netpod::{
    AggKind, BinSpecDimT, Channel, NanoRange, NodeConfig, PreBinnedPatchCoord, PreBinnedPatchIterator,
    PreBinnedPatchRange,
};
use std::future::{ready, Future};
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct PreBinnedValueByteStream {
    inp: PreBinnedValueStream,
    errored: bool,
    completed: bool,
}

impl PreBinnedValueByteStream {
    pub fn new(patch: PreBinnedPatchCoord, channel: Channel, agg_kind: AggKind, node_config: &NodeConfig) -> Self {
        Self {
            inp: PreBinnedValueStream::new(patch, channel, agg_kind, node_config),
            errored: false,
            completed: false,
        }
    }
}

impl Stream for PreBinnedValueByteStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => match make_frame::<PreBinnedHttpFrame>(&item) {
                Ok(buf) => Ready(Some(Ok(buf.freeze()))),
                Err(e) => {
                    self.errored = true;
                    Ready(Some(Err(e.into())))
                }
            },
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub struct PreBinnedValueStream {
    patch_coord: PreBinnedPatchCoord,
    channel: Channel,
    agg_kind: AggKind,
    node_config: NodeConfig,
    open_check_local_file: Option<Pin<Box<dyn Future<Output = Result<tokio::fs::File, std::io::Error>> + Send>>>,
    fut2: Option<Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarBinBatch, Error>> + Send>>>,
}

impl PreBinnedValueStream {
    pub fn new(
        patch_coord: PreBinnedPatchCoord,
        channel: Channel,
        agg_kind: AggKind,
        node_config: &NodeConfig,
    ) -> Self {
        // TODO check that we are the correct node.
        let _node_ix = node_ix_for_patch(&patch_coord, &channel, &node_config.cluster);
        Self {
            patch_coord,
            channel,
            agg_kind,
            node_config: node_config.clone(),
            open_check_local_file: None,
            fut2: None,
        }
    }

    fn try_setup_fetch_prebinned_higher_res(&mut self) {
        info!("try to find a next better granularity for {:?}", self.patch_coord);
        let g = self.patch_coord.bin_t_len();
        let range = NanoRange {
            beg: self.patch_coord.patch_beg(),
            end: self.patch_coord.patch_end(),
        };
        match PreBinnedPatchRange::covering_range(range, self.patch_coord.bin_count() + 1) {
            Some(range) => {
                let h = range.grid_spec.bin_t_len();
                info!(
                    "FOUND NEXT GRAN  g {}  h {}  ratio {}  mod {}  {:?}",
                    g,
                    h,
                    g / h,
                    g % h,
                    range
                );
                assert!(g / h > 1);
                assert!(g / h < 200);
                assert!(g % h == 0);
                let channel = self.channel.clone();
                let agg_kind = self.agg_kind.clone();
                let node_config = self.node_config.clone();
                let patch_it = PreBinnedPatchIterator::from_range(range);
                let s = futures_util::stream::iter(patch_it)
                    .map(move |coord| {
                        PreBinnedValueFetchedStream::new(coord, channel.clone(), agg_kind.clone(), &node_config)
                    })
                    .flatten();
                self.fut2 = Some(Box::pin(s));
            }
            None => {
                warn!("no better resolution found for  g {}", g);
                let evq = EventsQuery {
                    channel: self.channel.clone(),
                    range: NanoRange {
                        beg: self.patch_coord.patch_beg(),
                        end: self.patch_coord.patch_end(),
                    },
                    agg_kind: self.agg_kind.clone(),
                };
                assert!(self.patch_coord.patch_t_len() % self.patch_coord.bin_t_len() == 0);
                error!("try_setup_fetch_prebinned_higher_res  apply all requested transformations and T-binning");
                let count = self.patch_coord.patch_t_len() / self.patch_coord.bin_t_len();
                // TODO use a ctor, remove from BinSpecDimT the redundant variable.
                // If given a timestamp range, verify that it divides.
                // For ranges, use a range type.
                let spec = BinSpecDimT {
                    bs: self.patch_coord.bin_t_len(),
                    ts1: self.patch_coord.patch_beg(),
                    ts2: self.patch_coord.patch_end(),
                    count,
                };
                let s1 = MergedFromRemotes::new(evq, self.node_config.cluster.clone());
                let s2 = s1
                    .map(|k| {
                        if k.is_err() {
                            error!("\n\n\n..................   try_setup_fetch_prebinned_higher_res  got ERROR");
                        } else {
                            trace!("try_setup_fetch_prebinned_higher_res  got some item from  MergedFromRemotes");
                        }
                        k
                    })
                    .into_binned_t(spec)
                    .map_ok({
                        let mut a = MinMaxAvgScalarBinBatch::empty();
                        move |k| {
                            a.push_single(&k);
                            if a.len() > 0 {
                                let z = std::mem::replace(&mut a, MinMaxAvgScalarBinBatch::empty());
                                Some(z)
                            } else {
                                None
                            }
                        }
                    })
                    .filter_map(|k| {
                        let g = match k {
                            Ok(Some(k)) => Some(Ok(k)),
                            Ok(None) => None,
                            Err(e) => Some(Err(e)),
                        };
                        ready(g)
                    })
                    .take_while({
                        let mut run = true;
                        move |k| {
                            if !run {
                                ready(false)
                            } else {
                                if k.is_err() {
                                    run = false;
                                }
                                ready(true)
                            }
                        }
                    });
                self.fut2 = Some(Box::pin(s2));
            }
        }
    }
}

impl Stream for PreBinnedValueStream {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = Result<MinMaxAvgScalarBinBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if let Some(fut) = self.fut2.as_mut() {
                fut.poll_next_unpin(cx)
            } else if let Some(fut) = self.open_check_local_file.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(Ok(_file)) => err::todoval(),
                    Ready(Err(e)) => match e.kind() {
                        std::io::ErrorKind::NotFound => {
                            error!("TODO LOCAL CACHE FILE NOT FOUND");
                            self.try_setup_fetch_prebinned_higher_res();
                            continue 'outer;
                        }
                        _ => {
                            error!("File I/O error: {:?}", e);
                            Ready(Some(Err(e.into())))
                        }
                    },
                    Pending => Pending,
                }
            } else {
                #[allow(unused_imports)]
                use std::os::unix::fs::OpenOptionsExt;
                let mut opts = std::fs::OpenOptions::new();
                opts.read(true);
                let fut = async { tokio::fs::OpenOptions::from(opts).open("/DOESNOTEXIST").await };
                self.open_check_local_file = Some(Box::pin(fut));
                continue 'outer;
            };
        }
    }
}
