pub mod binnedfrompbv;
pub mod dim1;
pub mod pbv;
pub mod prebinned;
pub mod query;

use crate::agg::binnedt::TBinnerStream;
use crate::binned::binnedfrompbv::BinnedFromPreBinned;
use crate::binnedstream::BoxedStream;
use crate::channelexec::{channel_exec, ChannelExecFunction};
use crate::decode::{Endianness, EventValueFromBytes, EventValueShape, NumFromBytes};
use crate::merge::mergedfromremotes::MergedFromRemotes;
use bytes::Bytes;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::numops::NumOps;
use items::streams::{collect_plain_events_json, Collectable, Collector};
use items::{
    Clearable, EventsNodeProcessor, FilterFittingInside, Framable, FrameDecodable, FrameType, PushableIndex,
    RangeCompletableItem, Sitemty, StreamItem, TimeBinnableType, WithLen,
};
use netpod::log::*;
use netpod::query::{BinnedQuery, RawEventsQuery};
use netpod::x_bin_count;
use netpod::{BinnedRange, NodeConfigCached, PerfOpts, PreBinnedPatchIterator, PreBinnedPatchRange, ScalarType, Shape};
use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub struct BinnedStreamRes<I> {
    pub binned_stream: BoxedStream<Result<StreamItem<RangeCompletableItem<I>>, Error>>,
    pub range: BinnedRange,
}

pub struct BinnedBinaryChannelExec {
    query: BinnedQuery,
    node_config: NodeConfigCached,
}

impl BinnedBinaryChannelExec {
    pub fn new(query: BinnedQuery, node_config: NodeConfigCached) -> Self {
        Self { query, node_config }
    }
}

impl ChannelExecFunction for BinnedBinaryChannelExec {
    type Output = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

    fn exec<NTY, END, EVS, ENP>(
        self,
        _byte_order: END,
        scalar_type: ScalarType,
        shape: Shape,
        event_value_shape: EVS,
        _events_node_proc: ENP,
    ) -> Result<Self::Output, Error>
    where
        NTY: NumOps + NumFromBytes<NTY, END> + 'static,
        END: Endianness + 'static,
        EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
        ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Batch> + 'static,
        // TODO require these things in general?
        <ENP as EventsNodeProcessor>::Output: Collectable + PushableIndex + Clearable,
        <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output: Debug
            + TimeBinnableType<Output = <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>
            + Collectable
            + Unpin,
        Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + Framable + 'static,
        Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>:
            FrameType + Framable + FrameDecodable,
    {
        let _ = event_value_shape;
        let range = BinnedRange::covering_range(self.query.range().clone(), self.query.bin_count())?;
        let perf_opts = PerfOpts { inmem_bufcap: 512 };
        let souter = match PreBinnedPatchRange::covering_range(self.query.range().clone(), self.query.bin_count()) {
            Ok(Some(pre_range)) => {
                debug!("BinnedBinaryChannelExec  found pre_range: {pre_range:?}");
                if range.grid_spec().bin_t_len() < pre_range.grid_spec.bin_t_len() {
                    let msg = format!(
                        "BinnedBinaryChannelExec  incompatible ranges:\npre_range: {pre_range:?}\nrange: {range:?}"
                    );
                    return Err(Error::with_msg(msg));
                }
                let s = BinnedFromPreBinned::<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>::new(
                    PreBinnedPatchIterator::from_range(pre_range),
                    self.query.channel().clone(),
                    range.clone(),
                    scalar_type,
                    shape,
                    self.query.agg_kind().clone(),
                    self.query.cache_usage().clone(),
                    self.query.disk_io_buffer_size(),
                    &self.node_config,
                    self.query.disk_stats_every().clone(),
                    self.query.report_error(),
                )?
                .map(|item| match item.make_frame() {
                    Ok(item) => Ok(item.freeze()),
                    Err(e) => Err(e),
                });
                Ok(Box::pin(s) as Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>)
            }
            Ok(None) => {
                debug!(
                    "BinnedBinaryChannelExec  no covering range for prebinned, merge from remotes instead {range:?}"
                );
                // TODO let BinnedQuery provide the DiskIoTune and pass to RawEventsQuery:
                let evq = RawEventsQuery::new(
                    self.query.channel().clone(),
                    self.query.range().clone(),
                    self.query.agg_kind().clone(),
                );
                let x_bin_count = x_bin_count(&shape, self.query.agg_kind());
                let s = MergedFromRemotes::<ENP>::new(evq, perf_opts, self.node_config.node_config.cluster.clone());
                let s = TBinnerStream::<_, <ENP as EventsNodeProcessor>::Output>::new(
                    s,
                    range,
                    x_bin_count,
                    self.query.agg_kind().do_time_weighted(),
                );
                let s = s.map(|item| match item.make_frame() {
                    Ok(item) => Ok(item.freeze()),
                    Err(e) => Err(e),
                });
                Ok(Box::pin(s) as Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>)
            }
            Err(e) => Err(e),
        }?;
        Ok(souter)
    }

    fn empty() -> Self::Output {
        Box::pin(futures_util::stream::empty())
    }
}

pub async fn binned_bytes_for_http(
    query: &BinnedQuery,
    scalar_type: ScalarType,
    shape: Shape,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>, Error> {
    let ret = channel_exec(
        BinnedBinaryChannelExec::new(query.clone(), node_config.clone()),
        query.channel(),
        query.range(),
        scalar_type,
        shape,
        query.agg_kind().clone(),
        node_config,
    )
    .await?;
    Ok(Box::pin(ret))
}

pub struct BinnedBytesForHttpStream<S> {
    inp: S,
    errored: bool,
    completed: bool,
}

impl<S> BinnedBytesForHttpStream<S> {
    pub fn new(inp: S) -> Self {
        Self {
            inp,
            errored: false,
            completed: false,
        }
    }
}

pub trait MakeBytesFrame {
    fn make_bytes_frame(&self) -> Result<Bytes, Error> {
        // TODO only implemented for one type, remove
        err::todoval()
    }
}

impl<S, I> Stream for BinnedBytesForHttpStream<S>
where
    S: Stream<Item = I> + Unpin,
    I: MakeBytesFrame,
{
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("BinnedBytesForHttpStream  poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => match item.make_bytes_frame() {
                Ok(buf) => Ready(Some(Ok(buf))),
                Err(e) => {
                    self.errored = true;
                    Ready(Some(Err(e.into())))
                }
            },
            Ready(None) => {
                self.completed = true;
                Ready(None)
            }
            Pending => Pending,
        }
    }
}

pub struct Bool {}

impl Bool {
    pub fn is_false(x: &bool) -> bool {
        *x == false
    }
}

pub async fn collect_all<T, S>(
    stream: S,
    bin_count_exp: u32,
    timeout: Duration,
    abort_after_bin_count: u32,
) -> Result<serde_json::Value, Error>
where
    S: Stream<Item = Sitemty<T>> + Unpin,
    T: Collectable,
{
    info!("\n\nConstruct deadline with timeout {timeout:?}\n\n");
    let deadline = tokio::time::Instant::now() + timeout;
    let mut collector = <T as Collectable>::new_collector(bin_count_exp);
    let mut i1 = 0;
    let mut stream = stream;
    loop {
        let item = if i1 == 0 {
            stream.next().await
        } else {
            if abort_after_bin_count > 0 && collector.len() >= abort_after_bin_count as usize {
                None
            } else {
                match tokio::time::timeout_at(deadline, stream.next()).await {
                    Ok(k) => k,
                    Err(_) => {
                        collector.set_timed_out();
                        None
                    }
                }
            }
        };
        match item {
            Some(item) => {
                match item {
                    Ok(item) => match item {
                        StreamItem::Log(_) => {}
                        StreamItem::Stats(_) => {}
                        StreamItem::DataItem(item) => match item {
                            RangeCompletableItem::RangeComplete => {
                                collector.set_range_complete();
                            }
                            RangeCompletableItem::Data(item) => {
                                collector.ingest(&item);
                                i1 += 1;
                            }
                        },
                    },
                    Err(e) => {
                        // TODO  Need to use some flags to get good enough error message for remote user.
                        Err(e)?;
                    }
                };
            }
            None => break,
        }
    }
    let ret = serde_json::to_value(collector.result()?)?;
    Ok(ret)
}

pub struct BinnedJsonChannelExec {
    query: BinnedQuery,
    node_config: NodeConfigCached,
    timeout: Duration,
}

impl BinnedJsonChannelExec {
    pub fn new(query: BinnedQuery, timeout: Duration, node_config: NodeConfigCached) -> Self {
        Self {
            query,
            node_config,
            timeout,
        }
    }
}

impl ChannelExecFunction for BinnedJsonChannelExec {
    type Output = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

    fn exec<NTY, END, EVS, ENP>(
        self,
        _byte_order: END,
        scalar_type: ScalarType,
        shape: Shape,
        event_value_shape: EVS,
        _events_node_proc: ENP,
    ) -> Result<Self::Output, Error>
    where
        NTY: NumOps + NumFromBytes<NTY, END> + 'static,
        END: Endianness + 'static,
        EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
        ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Batch> + 'static,
        // TODO require these things in general?
        <ENP as EventsNodeProcessor>::Output: Collectable + PushableIndex + Clearable,
        <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output: Debug
            + TimeBinnableType<Output = <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>
            + Collectable
            + Unpin,
        Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + Framable + 'static,
        Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>:
            FrameType + Framable + FrameDecodable,
    {
        let _ = event_value_shape;
        let range = BinnedRange::covering_range(self.query.range().clone(), self.query.bin_count())?;
        let t_bin_count = range.bin_count() as u32;
        let perf_opts = PerfOpts { inmem_bufcap: 512 };
        let souter = match PreBinnedPatchRange::covering_range(self.query.range().clone(), self.query.bin_count()) {
            Ok(Some(pre_range)) => {
                info!("BinnedJsonChannelExec  found pre_range: {pre_range:?}");
                if range.grid_spec().bin_t_len() < pre_range.grid_spec.bin_t_len() {
                    let msg = format!(
                        "BinnedJsonChannelExec  incompatible ranges:\npre_range: {pre_range:?}\nrange: {range:?}"
                    );
                    return Err(Error::with_msg(msg));
                }
                let s = BinnedFromPreBinned::<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>::new(
                    PreBinnedPatchIterator::from_range(pre_range),
                    self.query.channel().clone(),
                    range.clone(),
                    scalar_type,
                    shape,
                    self.query.agg_kind().clone(),
                    self.query.cache_usage().clone(),
                    self.query.disk_io_buffer_size(),
                    &self.node_config,
                    self.query.disk_stats_every().clone(),
                    self.query.report_error(),
                )?;
                let f = collect_plain_events_json(s, self.timeout, t_bin_count, u64::MAX, self.query.do_log());
                let s = futures_util::stream::once(f).map(|item| match item {
                    Ok(item) => Ok(Bytes::from(serde_json::to_vec(&item)?)),
                    Err(e) => Err(e.into()),
                });
                Ok(Box::pin(s) as Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>)
            }
            Ok(None) => {
                info!("BinnedJsonChannelExec  no covering range for prebinned, merge from remotes instead {range:?}");
                // TODO let BinnedQuery provide the DiskIoTune and pass to RawEventsQuery:
                let evq = RawEventsQuery::new(
                    self.query.channel().clone(),
                    self.query.range().clone(),
                    self.query.agg_kind().clone(),
                );
                let x_bin_count = x_bin_count(&shape, self.query.agg_kind());
                let s = MergedFromRemotes::<ENP>::new(evq, perf_opts, self.node_config.node_config.cluster.clone());
                let s = TBinnerStream::<_, <ENP as EventsNodeProcessor>::Output>::new(
                    s,
                    range,
                    x_bin_count,
                    self.query.agg_kind().do_time_weighted(),
                );
                let f = collect_plain_events_json(s, self.timeout, t_bin_count, u64::MAX, self.query.do_log());
                let s = futures_util::stream::once(f).map(|item| match item {
                    Ok(item) => Ok(Bytes::from(serde_json::to_vec(&item)?)),
                    Err(e) => Err(e.into()),
                });
                Ok(Box::pin(s) as Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>)
            }
            Err(e) => Err(e),
        }?;
        Ok(souter)
    }

    fn empty() -> Self::Output {
        info!("BinnedJsonChannelExec  fn empty");
        Box::pin(futures_util::stream::empty())
    }
}

pub async fn binned_json(
    query: &BinnedQuery,
    scalar_type: ScalarType,
    shape: Shape,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>, Error> {
    let ret = channel_exec(
        BinnedJsonChannelExec::new(query.clone(), query.timeout(), node_config.clone()),
        query.channel(),
        query.range(),
        scalar_type,
        shape,
        query.agg_kind().clone(),
        node_config,
    )
    .await?;
    Ok(Box::pin(ret))
}

pub trait EventsDecoder {
    type Output;
    fn ingest(&mut self, event: &[u8]);
    fn result(&mut self) -> Self::Output;
}
