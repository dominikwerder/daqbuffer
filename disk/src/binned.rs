use crate::agg::binnedt::{TBinnerStream, TimeBinnableType, TimeBinnableTypeAggregator};
use crate::agg::enp::{ts_offs_from_abs, Identity, WaveXBinner};
use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::{Appendable, Collectable, Collector, StreamItem, ToJsonBytes, ToJsonResult};
use crate::agg::{Fits, FitsInside};
use crate::binned::binnedfrompbv::BinnedFromPreBinned;
use crate::binned::query::BinnedQuery;
use crate::binnedstream::BoxedStream;
use crate::channelexec::{channel_exec, collect_plain_events_json, ChannelExecFunction};
use crate::decode::{
    BigEndian, Endianness, EventValueFromBytes, EventValueShape, EventValues, EventValuesDim0Case, EventValuesDim1Case,
    LittleEndian, NumFromBytes,
};
use crate::frame::makeframe::{Framable, FrameType, SubFrId};
use crate::merge::mergedfromremotes::MergedFromRemotes;
use crate::raw::RawEventsQuery;
use crate::Sitemty;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{
    x_bin_count, AggKind, BinnedRange, ByteOrder, NanoRange, NodeConfigCached, PerfOpts, PreBinnedPatchIterator,
    PreBinnedPatchRange, ScalarType, Shape,
};
use num_traits::{AsPrimitive, Bounded, Float, Zero};
use parse::channelconfig::{extract_matching_config_entry, read_local_config, MatchingConfigEntry};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncRead, ReadBuf};

pub mod binnedfrompbv;
pub mod dim1;
pub mod pbv;
pub mod prebinned;
pub mod query;

pub struct BinnedStreamRes<I> {
    pub binned_stream: BoxedStream<Result<StreamItem<RangeCompletableItem<I>>, Error>>,
    pub range: BinnedRange,
}

pub struct BinnedResponseStat<T> {
    stream: Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>,
    bin_count: u32,
}

// TODO Can I unify these functions with the ones from prebinned.rs?
// They also must resolve to the same types, so would be good to unify.

fn make_num_pipeline_nty_end_evs_enp_stat<NTY, END, EVS, ENP>(
    shape: Shape,
    event_value_shape: EVS,
    query: BinnedQuery,
    node_config: &NodeConfigCached,
) -> Result<BinnedResponseStat<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>, Error>
where
    NTY: NumOps + NumFromBytes<NTY, END> + Serialize + 'static,
    END: Endianness + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
    ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Output> + 'static,
    <ENP as EventsNodeProcessor>::Output: TimeBinnableType + PushableIndex + Appendable + 'static,
    <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output:
        TimeBinnableType<Output = <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output> + Unpin,
    Sitemty<
        <<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Aggregator as TimeBinnableTypeAggregator>::Output,
    >: Framable,
    // TODO require these things in general?
    Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + Framable + 'static,
    Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>:
        FrameType + Framable + DeserializeOwned,
    <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output: Sized,
{
    let _ = event_value_shape;
    let range = BinnedRange::covering_range(query.range().clone(), query.bin_count())?.ok_or(Error::with_msg(
        format!("binned_bytes_for_http  BinnedRange::covering_range returned None"),
    ))?;
    let perf_opts = PerfOpts { inmem_bufcap: 512 };
    match PreBinnedPatchRange::covering_range(query.range().clone(), query.bin_count()) {
        Ok(Some(pre_range)) => {
            info!("binned_bytes_for_http  found pre_range: {:?}", pre_range);
            if range.grid_spec.bin_t_len() < pre_range.grid_spec.bin_t_len() {
                let msg = format!(
                    "binned_bytes_for_http  incompatible ranges:\npre_range: {:?}\nrange: {:?}",
                    pre_range, range
                );
                return Err(Error::with_msg(msg));
            }
            let s = BinnedFromPreBinned::<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>::new(
                PreBinnedPatchIterator::from_range(pre_range),
                query.channel().clone(),
                range.clone(),
                shape,
                query.agg_kind().clone(),
                query.cache_usage().clone(),
                node_config,
                query.disk_stats_every().clone(),
                query.report_error(),
            )?;
            let ret = BinnedResponseStat {
                stream: Box::pin(s),
                bin_count: range.count as u32,
            };
            Ok(ret)
        }
        Ok(None) => {
            info!(
                "binned_bytes_for_http  no covering range for prebinned, merge from remotes instead {:?}",
                range
            );
            let bin_count = range.count as u32;
            let evq = RawEventsQuery {
                channel: query.channel().clone(),
                range: query.range().clone(),
                agg_kind: query.agg_kind().clone(),
            };
            let x_bin_count = if let AggKind::DimXBinsN(n) = query.agg_kind() {
                *n as usize
            } else {
                0
            };
            let s = MergedFromRemotes::<ENP>::new(evq, perf_opts, node_config.node_config.cluster.clone());
            let s = TBinnerStream::<_, <ENP as EventsNodeProcessor>::Output>::new(s, range, x_bin_count);
            let ret = BinnedResponseStat {
                stream: Box::pin(s),
                bin_count,
            };
            Ok(ret)
        }
        Err(e) => Err(e),
    }
}

pub trait BinnedResponseItem: Send + ToJsonResult + Framable {}

impl<T> BinnedResponseItem for T where T: Send + ToJsonResult + Framable {}

pub struct BinnedResponseDyn {
    stream: Pin<Box<dyn Stream<Item = Box<dyn BinnedResponseItem>> + Send>>,
}

fn make_num_pipeline_nty_end_evs_enp<PPP, NTY, END, EVS, ENP>(
    shape: Shape,
    _agg_kind: AggKind,
    event_value_shape: EVS,
    _events_node_proc: ENP,
    query: BinnedQuery,
    ppp: PPP,
    node_config: &NodeConfigCached,
) -> Result<BinnedResponseDyn, Error>
where
    PPP: PipelinePostProcessA,
    PPP: PipelinePostProcessB<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>,
    NTY: NumOps + NumFromBytes<NTY, END> + 'static,
    END: Endianness + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
    ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Output> + 'static,
    // TODO require these properties in general:
    <ENP as EventsNodeProcessor>::Output: TimeBinnableType + PushableIndex + Appendable + 'static,
    <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output:
        TimeBinnableType<Output = <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output> + Unpin,
    Sitemty<
        <<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Aggregator as TimeBinnableTypeAggregator>::Output,
    >: Framable,
    // TODO require these things in general?
    Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + Framable + 'static,
    // TODO is this correct? why do I want the Output to be Framable?
    Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>:
        FrameType + Framable + DeserializeOwned,
    Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>: ToJsonResult + Framable,
{
    let res = make_num_pipeline_nty_end_evs_enp_stat::<_, _, _, ENP>(shape, event_value_shape, query, node_config)?;
    let s = ppp.convert(res.stream, res.bin_count);
    let ret = BinnedResponseDyn { stream: Box::pin(s) };
    Ok(ret)
}

fn make_num_pipeline_nty_end<PPP, NTY, END>(
    shape: Shape,
    query: BinnedQuery,
    ppp: PPP,
    node_config: &NodeConfigCached,
) -> Result<BinnedResponseDyn, Error>
where
    PPP: PipelinePostProcessA,
    PPP: PipelinePostProcessB<MinMaxAvgBins<NTY>>,
    NTY: NumOps + NumFromBytes<NTY, END> + Serialize + 'static,
    END: Endianness + 'static,
{
    let agg_kind = query.agg_kind().clone();
    match shape {
        Shape::Scalar => {
            let evs = EventValuesDim0Case::new();
            match agg_kind {
                AggKind::DimXBins1 => {
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    make_num_pipeline_nty_end_evs_enp::<_, NTY, END, _, _>(
                        shape,
                        agg_kind,
                        evs,
                        events_node_proc,
                        query,
                        ppp,
                        node_config,
                    )
                }
                AggKind::DimXBinsN(_) => {
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToNBins as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    make_num_pipeline_nty_end_evs_enp::<_, NTY, END, _, _>(
                        shape,
                        agg_kind,
                        evs,
                        events_node_proc,
                        query,
                        ppp,
                        node_config,
                    )
                }
                AggKind::Plain => {
                    panic!();
                }
            }
        }
        Shape::Wave(n) => {
            let evs = EventValuesDim1Case::new(n);
            match agg_kind {
                AggKind::DimXBins1 => {
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    make_num_pipeline_nty_end_evs_enp::<PPP, NTY, END, _, _>(
                        shape,
                        agg_kind,
                        evs,
                        events_node_proc,
                        query,
                        ppp,
                        node_config,
                    )
                }
                AggKind::DimXBinsN(_) => {
                    /*let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToNBins as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    let yo = make_num_pipeline_nty_end_evs_enp::<PPP, NTY, END, _, _>(
                        shape,
                        agg_kind,
                        evs,
                        events_node_proc,
                        query,
                        ppp,
                        node_config,
                    );*/
                    err::todoval()
                }
                AggKind::Plain => {
                    panic!();
                }
            }
        }
    }
}

#[allow(dead_code)]
fn make_num_pipeline_nty_end_old<PPP, NTY, END>(
    shape: Shape,
    query: BinnedQuery,
    ppp: PPP,
    node_config: &NodeConfigCached,
) -> Result<BinnedResponseDyn, Error>
where
    PPP: PipelinePostProcessA,
    PPP: PipelinePostProcessB<MinMaxAvgBins<NTY>>,
    PPP: PipelinePostProcessB<MinMaxAvgWaveBins<NTY>>,
    NTY: NumOps + NumFromBytes<NTY, END> + 'static,
    END: Endianness + 'static,
{
    let agg_kind = query.agg_kind().clone();
    match shape {
        Shape::Scalar => make_num_pipeline_nty_end_evs_enp::<_, NTY, _, _, Identity<_>>(
            shape.clone(),
            agg_kind.clone(),
            EventValuesDim0Case::new(),
            Identity::create(shape.clone(), agg_kind.clone()),
            query,
            ppp,
            node_config,
        ),
        Shape::Wave(n) => make_num_pipeline_nty_end_evs_enp::<_, NTY, _, _, WaveXBinner<_>>(
            shape.clone(),
            agg_kind.clone(),
            EventValuesDim1Case::new(n),
            WaveXBinner::create(shape.clone(), agg_kind.clone()),
            query,
            ppp,
            node_config,
        ),
    }
}

macro_rules! match_end {
    ($nty:ident, $end:expr, $shape:expr, $query:expr, $ppp:expr, $node_config:expr) => {
        match $end {
            ByteOrder::LE => make_num_pipeline_nty_end::<_, $nty, LittleEndian>($shape, $query, $ppp, $node_config),
            ByteOrder::BE => make_num_pipeline_nty_end::<_, $nty, BigEndian>($shape, $query, $ppp, $node_config),
        }
    };
}

fn make_num_pipeline_entry<PPP>(
    scalar_type: ScalarType,
    byte_order: ByteOrder,
    shape: Shape,
    query: BinnedQuery,
    ppp: PPP,
    node_config: &NodeConfigCached,
) -> Result<BinnedResponseDyn, Error>
where
    PPP: PipelinePostProcessA,
    PPP: PipelinePostProcessB<MinMaxAvgBins<u8>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<u16>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<u32>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<u64>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<i8>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<i16>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<i32>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<i64>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<f32>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<f64>>,
{
    match scalar_type {
        ScalarType::U8 => match_end!(u8, byte_order, shape, query, ppp, node_config),
        ScalarType::U16 => match_end!(u16, byte_order, shape, query, ppp, node_config),
        ScalarType::U32 => match_end!(u32, byte_order, shape, query, ppp, node_config),
        ScalarType::U64 => match_end!(u64, byte_order, shape, query, ppp, node_config),
        ScalarType::I8 => match_end!(i8, byte_order, shape, query, ppp, node_config),
        ScalarType::I16 => match_end!(i16, byte_order, shape, query, ppp, node_config),
        ScalarType::I32 => match_end!(i32, byte_order, shape, query, ppp, node_config),
        ScalarType::I64 => match_end!(i64, byte_order, shape, query, ppp, node_config),
        ScalarType::F32 => match_end!(f32, byte_order, shape, query, ppp, node_config),
        ScalarType::F64 => match_end!(f64, byte_order, shape, query, ppp, node_config),
    }
}

async fn make_num_pipeline<PPP>(
    query: &BinnedQuery,
    ppp: PPP,
    node_config: &NodeConfigCached,
) -> Result<BinnedResponseDyn, Error>
where
    PPP: PipelinePostProcessA,
    PPP: PipelinePostProcessB<MinMaxAvgBins<u8>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<u16>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<u32>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<u64>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<i8>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<i16>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<i32>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<i64>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<f32>>,
    PPP: PipelinePostProcessB<MinMaxAvgBins<f64>>,
{
    if query.channel().backend != node_config.node.backend {
        let err = Error::with_msg(format!(
            "backend mismatch  node: {}  requested: {}",
            node_config.node.backend,
            query.channel().backend
        ));
        return Err(err);
    }
    let channel_config = match read_local_config(&query.channel(), &node_config.node).await {
        Ok(k) => k,
        Err(e) => {
            if e.msg().contains("ErrorKind::NotFound") {
                let s = futures_util::stream::empty();
                let ret = BinnedResponseDyn { stream: Box::pin(s) };
                return Ok(ret);
            } else {
                return Err(e);
            }
        }
    };
    match extract_matching_config_entry(query.range(), &channel_config)? {
        MatchingConfigEntry::Multiple => Err(Error::with_msg("multiple config entries found"))?,
        MatchingConfigEntry::None => {
            // TODO can I use the same binned_stream machinery to construct the matching empty result?
            // Need the requested range all with empty/nan values and zero counts.
            let s = futures_util::stream::empty();
            let ret = BinnedResponseDyn { stream: Box::pin(s) };
            Ok(ret)
        }
        MatchingConfigEntry::Entry(entry) => {
            // TODO make this a stream log:
            info!("binned_bytes_for_http  found config entry {:?}", entry);
            let ret = make_num_pipeline_entry(
                entry.scalar_type.clone(),
                entry.byte_order.clone(),
                entry.to_shape()?,
                query.clone(),
                ppp,
                node_config,
            )?;
            Ok(ret)
        }
    }
}

pub trait PipelinePostProcessA {}

struct MakeBoxedItems {}

impl PipelinePostProcessA for MakeBoxedItems {}

pub trait PipelinePostProcessB<T> {
    fn convert(
        &self,
        inp: Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>,
        bin_count_exp: u32,
    ) -> Pin<Box<dyn Stream<Item = Box<dyn BinnedResponseItem>> + Send>>;
}

impl<NTY> PipelinePostProcessB<MinMaxAvgBins<NTY>> for MakeBoxedItems
where
    NTY: NumOps,
{
    fn convert(
        &self,
        inp: Pin<Box<dyn Stream<Item = Sitemty<MinMaxAvgBins<NTY>>> + Send>>,
        _bin_count_exp: u32,
    ) -> Pin<Box<dyn Stream<Item = Box<dyn BinnedResponseItem>> + Send>> {
        let s = StreamExt::map(inp, |item| Box::new(item) as Box<dyn BinnedResponseItem>);
        Box::pin(s)
    }
}

struct CollectForJson {
    timeout: Duration,
    abort_after_bin_count: u32,
}

impl CollectForJson {
    #[allow(dead_code)]
    pub fn new(timeout: Duration, abort_after_bin_count: u32) -> Self {
        Self {
            timeout,
            abort_after_bin_count,
        }
    }
}

impl PipelinePostProcessA for CollectForJson {}

pub struct JsonCollector {
    fut: Pin<Box<dyn Future<Output = Result<serde_json::Value, Error>> + Send>>,
    completed: bool,
    done: bool,
}

impl JsonCollector {
    pub fn new<NTY>(
        inp: Pin<Box<dyn Stream<Item = Sitemty<MinMaxAvgBins<NTY>>> + Send>>,
        bin_count_exp: u32,
        timeout: Duration,
        abort_after_bin_count: u32,
    ) -> Self
    where
        NTY: NumOps + Serialize + 'static,
    {
        let fut = collect_all(inp, bin_count_exp, timeout, abort_after_bin_count);
        let fut = Box::pin(fut);
        Self {
            fut,
            completed: false,
            done: false,
        }
    }
}

impl ToJsonBytes for serde_json::Value {
    fn to_json_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(self)?)
    }
}

impl ToJsonResult for Sitemty<serde_json::Value> {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        match self {
            Ok(item) => match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::Data(item) => Ok(Box::new(item.clone())),
                    RangeCompletableItem::RangeComplete => Err(Error::with_msg("RangeComplete")),
                },
                StreamItem::Log(item) => Err(Error::with_msg(format!("Log {:?}", item))),
                StreamItem::Stats(item) => Err(Error::with_msg(format!("Stats {:?}", item))),
            },
            Err(e) => Err(Error::with_msg(format!("Error {:?}", e))),
        }
    }
}

impl Stream for JsonCollector {
    type Item = Box<dyn BinnedResponseItem>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.completed {
                panic!("poll_next on completed")
            } else if self.done {
                self.completed = true;
                Ready(None)
            } else {
                match self.fut.poll_unpin(cx) {
                    Ready(Ok(item)) => {
                        let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
                        let item = Box::new(item) as Box<dyn BinnedResponseItem>;
                        self.done = true;
                        Ready(Some(item))
                    }
                    Ready(Err(e)) => {
                        // TODO don't emit the error as json.
                        let item = Err::<StreamItem<RangeCompletableItem<serde_json::Value>>, _>(e);
                        let item = Box::new(item) as Box<dyn BinnedResponseItem>;
                        self.done = true;
                        Ready(Some(item))
                    }
                    Pending => Pending,
                }
            };
        }
    }
}

impl<NTY> PipelinePostProcessB<MinMaxAvgBins<NTY>> for CollectForJson
where
    NTY: NumOps,
{
    fn convert(
        &self,
        inp: Pin<Box<dyn Stream<Item = Sitemty<MinMaxAvgBins<NTY>>> + Send>>,
        bin_count_exp: u32,
    ) -> Pin<Box<dyn Stream<Item = Box<dyn BinnedResponseItem>> + Send>> {
        let s = JsonCollector::new(inp, bin_count_exp, self.timeout, self.abort_after_bin_count);
        Box::pin(s)
    }
}

pub async fn binned_bytes_for_http(
    query: &BinnedQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>, Error> {
    let pl = make_num_pipeline::<MakeBoxedItems>(query, MakeBoxedItems {}, node_config).await?;
    let ret = pl.stream.map(|item| {
        let fr = item.make_frame();
        let fr = fr?;
        Ok(fr.freeze())
    });
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
    fn make_bytes_frame(&self) -> Result<Bytes, Error>;
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

#[derive(Clone, Debug, Deserialize)]
pub struct IsoDateTime(chrono::DateTime<Utc>);

impl Serialize for IsoDateTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string())
    }
}

pub fn make_iso_ts(tss: &[u64]) -> Vec<IsoDateTime> {
    tss.iter()
        .map(|&k| IsoDateTime(Utc.timestamp_nanos(k as i64)))
        .collect()
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
    info!("\n\nConstruct deadline with timeout {:?}\n\n", timeout);
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
        shape: Shape,
        event_value_shape: EVS,
        _events_node_proc: ENP,
    ) -> Result<Self::Output, Error>
    where
        NTY: NumOps + NumFromBytes<NTY, END> + 'static,
        END: Endianness + 'static,
        EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
        ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Output> + 'static,
        // TODO require these things in general?
        <ENP as EventsNodeProcessor>::Output: Collectable + PushableIndex,
        <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output: Debug
            + TimeBinnableType<Output = <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>
            + Collectable
            + Unpin,
        Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + Framable + 'static,
        Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>:
            FrameType + Framable + DeserializeOwned,
    {
        let _ = event_value_shape;
        let range =
            BinnedRange::covering_range(self.query.range().clone(), self.query.bin_count())?.ok_or(Error::with_msg(
                format!("binned_bytes_for_http  BinnedRange::covering_range returned None"),
            ))?;
        let t_bin_count = range.count as u32;
        let perf_opts = PerfOpts { inmem_bufcap: 512 };
        let souter = match PreBinnedPatchRange::covering_range(self.query.range().clone(), self.query.bin_count()) {
            Ok(Some(pre_range)) => {
                info!("binned_bytes_for_http  found pre_range: {:?}", pre_range);
                if range.grid_spec.bin_t_len() < pre_range.grid_spec.bin_t_len() {
                    let msg = format!(
                        "binned_bytes_for_http  incompatible ranges:\npre_range: {:?}\nrange: {:?}",
                        pre_range, range
                    );
                    return Err(Error::with_msg(msg));
                }
                let s = BinnedFromPreBinned::<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>::new(
                    PreBinnedPatchIterator::from_range(pre_range),
                    self.query.channel().clone(),
                    range.clone(),
                    shape,
                    self.query.agg_kind().clone(),
                    self.query.cache_usage().clone(),
                    &self.node_config,
                    self.query.disk_stats_every().clone(),
                    self.query.report_error(),
                )?;
                let f = collect_plain_events_json(s, self.timeout, t_bin_count);
                let s = futures_util::stream::once(f).map(|item| match item {
                    Ok(item) => Ok(Bytes::from(serde_json::to_vec(&item)?)),
                    Err(e) => Err(e.into()),
                });
                Ok(Box::pin(s) as Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>)
            }
            Ok(None) => {
                info!(
                    "binned_bytes_for_http  no covering range for prebinned, merge from remotes instead {:?}",
                    range
                );
                let evq = RawEventsQuery {
                    channel: self.query.channel().clone(),
                    range: self.query.range().clone(),
                    agg_kind: self.query.agg_kind().clone(),
                };
                let x_bin_count = x_bin_count(&shape, self.query.agg_kind());
                let s = MergedFromRemotes::<ENP>::new(evq, perf_opts, self.node_config.node_config.cluster.clone());
                let s = TBinnerStream::<_, <ENP as EventsNodeProcessor>::Output>::new(s, range, x_bin_count);
                let f = collect_plain_events_json(s, self.timeout, t_bin_count);
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
        Box::pin(futures_util::stream::empty())
    }
}

pub async fn binned_json(
    query: &BinnedQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>, Error> {
    let ret = channel_exec(
        BinnedJsonChannelExec::new(query.clone(), query.timeout(), node_config.clone()),
        query.channel(),
        query.range(),
        query.agg_kind().clone(),
        node_config,
    )
    .await?;
    Ok(Box::pin(ret))
}

pub struct ReadPbv<T>
where
    T: ReadableFromFile,
{
    buf: Vec<u8>,
    all: Vec<u8>,
    file: Option<File>,
    _m1: PhantomData<T>,
}

impl<T> ReadPbv<T>
where
    T: ReadableFromFile,
{
    fn new(file: File) -> Self {
        Self {
            // TODO make buffer size a parameter:
            buf: vec![0; 1024 * 32],
            all: vec![],
            file: Some(file),
            _m1: PhantomData,
        }
    }
}

impl<T> Future for ReadPbv<T>
where
    T: ReadableFromFile + Unpin,
{
    type Output = Result<StreamItem<RangeCompletableItem<T>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        let mut buf = std::mem::replace(&mut self.buf, Vec::new());
        let ret = 'outer: loop {
            let mut dst = ReadBuf::new(&mut buf);
            if dst.remaining() == 0 || dst.capacity() == 0 {
                break Ready(Err(Error::with_msg("bad read buffer")));
            }
            let fp = self.file.as_mut().unwrap();
            let f = Pin::new(fp);
            break match File::poll_read(f, cx, &mut dst) {
                Ready(res) => match res {
                    Ok(_) => {
                        if dst.filled().len() > 0 {
                            self.all.extend_from_slice(dst.filled());
                            continue 'outer;
                        } else {
                            match T::from_buf(&mut self.all) {
                                Ok(item) => Ready(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))),
                                Err(e) => Ready(Err(e)),
                            }
                        }
                    }
                    Err(e) => Ready(Err(e.into())),
                },
                Pending => Pending,
            };
        };
        self.buf = buf;
        ret
    }
}

pub trait ReadableFromFile: Sized {
    fn read_from_file(file: File) -> Result<ReadPbv<Self>, Error>;
    // TODO should not need this:
    fn from_buf(buf: &[u8]) -> Result<Self, Error>;
}

impl ReadableFromFile for MinMaxAvgScalarBinBatch {
    fn read_from_file(file: File) -> Result<ReadPbv<Self>, Error> {
        Ok(ReadPbv::new(file))
    }
    fn from_buf(buf: &[u8]) -> Result<Self, Error> {
        let dec: MinMaxAvgScalarBinBatch = serde_cbor::from_slice(&buf)?;
        Ok(dec)
    }
}

pub trait FilterFittingInside: Sized {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self>;
}

impl FilterFittingInside for MinMaxAvgScalarBinBatch {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

pub trait WithLen {
    fn len(&self) -> usize;
}

impl WithLen for MinMaxAvgScalarEventBatch {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl WithLen for MinMaxAvgScalarBinBatch {
    fn len(&self) -> usize {
        self.ts1s.len()
    }
}

pub trait WithTimestamps {
    fn ts(&self, ix: usize) -> u64;
}

impl WithTimestamps for MinMaxAvgScalarEventBatch {
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

pub trait PushableIndex {
    // TODO check whether it makes sense to allow a move out of src. Or use a deque for src type and pop?
    fn push_index(&mut self, src: &Self, ix: usize);
}

impl PushableIndex for MinMaxAvgScalarEventBatch {
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push(src.tss[ix]);
        self.mins.push(src.mins[ix]);
        self.maxs.push(src.maxs[ix]);
        self.avgs.push(src.avgs[ix]);
    }
}

pub trait RangeOverlapInfo {
    fn ends_before(&self, range: NanoRange) -> bool;
    fn ends_after(&self, range: NanoRange) -> bool;
    fn starts_after(&self, range: NanoRange) -> bool;
}

pub trait NumOps:
    Sized
    + Copy
    + Send
    + Unpin
    + Debug
    + Zero
    + AsPrimitive<f32>
    + Bounded
    + PartialOrd
    + SubFrId
    + Serialize
    + DeserializeOwned
{
    fn min_or_nan() -> Self;
    fn max_or_nan() -> Self;
    fn is_nan(&self) -> bool;
    fn fourty_two() -> Self;
}

macro_rules! impl_num_ops {
    ($ty:ident, $min_or_nan:ident, $max_or_nan:ident, $is_nan:ident) => {
        impl NumOps for $ty {
            fn min_or_nan() -> Self {
                $ty::$min_or_nan
            }
            fn max_or_nan() -> Self {
                $ty::$max_or_nan
            }
            fn is_nan(&self) -> bool {
                $is_nan(self)
            }
            fn fourty_two() -> Self {
                42 as Self
            }
        }
    };
}

fn is_nan_int<T>(_x: &T) -> bool {
    false
}

fn is_nan_float<T: Float>(x: &T) -> bool {
    x.is_nan()
}

impl_num_ops!(u8, MIN, MAX, is_nan_int);
impl_num_ops!(u16, MIN, MAX, is_nan_int);
impl_num_ops!(u32, MIN, MAX, is_nan_int);
impl_num_ops!(u64, MIN, MAX, is_nan_int);
impl_num_ops!(i8, MIN, MAX, is_nan_int);
impl_num_ops!(i16, MIN, MAX, is_nan_int);
impl_num_ops!(i32, MIN, MAX, is_nan_int);
impl_num_ops!(i64, MIN, MAX, is_nan_int);
impl_num_ops!(f32, NAN, NAN, is_nan_float);
impl_num_ops!(f64, NAN, NAN, is_nan_float);

pub trait EventsDecoder {
    type Output;
    fn ingest(&mut self, event: &[u8]);
    fn result(&mut self) -> Self::Output;
}

pub trait EventsNodeProcessor: Send + Unpin {
    type Input;
    type Output: Send + Unpin + DeserializeOwned + WithTimestamps + TimeBinnableType;
    fn create(shape: Shape, agg_kind: AggKind) -> Self;
    fn process(&self, inp: EventValues<Self::Input>) -> Self::Output;
}

pub trait TimeBins: Send + Unpin + WithLen + Appendable + FilterFittingInside {
    fn ts1s(&self) -> &Vec<u64>;
    fn ts2s(&self) -> &Vec<u64>;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MinMaxAvgBins<NTY> {
    pub ts1s: Vec<u64>,
    pub ts2s: Vec<u64>,
    pub counts: Vec<u64>,
    // TODO get rid of Option:
    pub mins: Vec<Option<NTY>>,
    pub maxs: Vec<Option<NTY>>,
    pub avgs: Vec<Option<f32>>,
}

impl<NTY> fmt::Debug for MinMaxAvgBins<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "MinMaxAvgBins  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  mins {:?}  maxs {:?}  avgs {:?}",
            self.ts1s.len(),
            self.ts1s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.ts2s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.counts,
            self.mins,
            self.maxs,
            self.avgs,
        )
    }
}

impl<NTY> MinMaxAvgBins<NTY> {
    pub fn empty() -> Self {
        Self {
            ts1s: vec![],
            ts2s: vec![],
            counts: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
        }
    }
}

impl<NTY> FitsInside for MinMaxAvgBins<NTY> {
    fn fits_inside(&self, range: NanoRange) -> Fits {
        if self.ts1s.is_empty() {
            Fits::Empty
        } else {
            let t1 = *self.ts1s.first().unwrap();
            let t2 = *self.ts2s.last().unwrap();
            if t2 <= range.beg {
                Fits::Lower
            } else if t1 >= range.end {
                Fits::Greater
            } else if t1 < range.beg && t2 > range.end {
                Fits::PartlyLowerAndGreater
            } else if t1 < range.beg {
                Fits::PartlyLower
            } else if t2 > range.end {
                Fits::PartlyGreater
            } else {
                Fits::Inside
            }
        }
    }
}

impl<NTY> FilterFittingInside for MinMaxAvgBins<NTY> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> RangeOverlapInfo for MinMaxAvgBins<NTY> {
    fn ends_before(&self, range: NanoRange) -> bool {
        match self.ts2s.last() {
            Some(&ts) => ts <= range.beg,
            None => true,
        }
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        match self.ts2s.last() {
            Some(&ts) => ts > range.end,
            None => panic!(),
        }
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        match self.ts1s.first() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }
}

impl<NTY> TimeBins for MinMaxAvgBins<NTY>
where
    NTY: NumOps,
{
    fn ts1s(&self) -> &Vec<u64> {
        &self.ts1s
    }

    fn ts2s(&self) -> &Vec<u64> {
        &self.ts2s
    }
}

impl<NTY> WithLen for MinMaxAvgBins<NTY> {
    fn len(&self) -> usize {
        self.ts1s.len()
    }
}

impl<NTY> Appendable for MinMaxAvgBins<NTY>
where
    NTY: NumOps,
{
    fn empty() -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.ts1s.extend_from_slice(&src.ts1s);
        self.ts2s.extend_from_slice(&src.ts2s);
        self.counts.extend_from_slice(&src.counts);
        self.mins.extend_from_slice(&src.mins);
        self.maxs.extend_from_slice(&src.maxs);
        self.avgs.extend_from_slice(&src.avgs);
    }
}

impl<NTY> ReadableFromFile for MinMaxAvgBins<NTY>
where
    NTY: NumOps,
{
    // TODO this function is not needed in the trait:
    fn read_from_file(file: File) -> Result<ReadPbv<Self>, Error> {
        Ok(ReadPbv::new(file))
    }

    fn from_buf(buf: &[u8]) -> Result<Self, Error> {
        let dec = serde_cbor::from_slice(&buf)?;
        Ok(dec)
    }
}

impl<NTY> TimeBinnableType for MinMaxAvgBins<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgBins<NTY>;
    type Aggregator = MinMaxAvgBinsAggregator<NTY>;

    fn aggregator(range: NanoRange, _x_bin_count: usize) -> Self::Aggregator {
        Self::Aggregator::new(range)
    }
}

impl<NTY> ToJsonResult for Sitemty<MinMaxAvgBins<NTY>>
where
    NTY: NumOps,
{
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        Ok(Box::new(serde_json::Value::String(format!(
            "MinMaxAvgBins/non-json-item"
        ))))
    }
}

pub struct MinMaxAvgBinsCollected<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> MinMaxAvgBinsCollected<NTY> {
    pub fn new() -> Self {
        Self { _m1: PhantomData }
    }
}

#[derive(Serialize)]
pub struct MinMaxAvgBinsCollectedResult<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: Vec<u64>,
    //ts_bin_edges: Vec<IsoDateTime>,
    counts: Vec<u64>,
    mins: Vec<Option<NTY>>,
    maxs: Vec<Option<NTY>>,
    avgs: Vec<Option<f32>>,
    #[serde(skip_serializing_if = "Bool::is_false", rename = "finalisedRange")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "Zero::is_zero", rename = "missingBins")]
    missing_bins: u32,
    #[serde(skip_serializing_if = "Option::is_none", rename = "continueAt")]
    continue_at: Option<IsoDateTime>,
}

pub struct MinMaxAvgBinsCollector<NTY> {
    bin_count_exp: u32,
    timed_out: bool,
    range_complete: bool,
    vals: MinMaxAvgBins<NTY>,
    _m1: PhantomData<NTY>,
}

impl<NTY> MinMaxAvgBinsCollector<NTY> {
    pub fn new(bin_count_exp: u32) -> Self {
        Self {
            bin_count_exp,
            timed_out: false,
            range_complete: false,
            vals: MinMaxAvgBins::<NTY>::empty(),
            _m1: PhantomData,
        }
    }
}

impl<NTY> WithLen for MinMaxAvgBinsCollector<NTY>
where
    NTY: NumOps + Serialize,
{
    fn len(&self) -> usize {
        self.vals.ts1s.len()
    }
}

impl<NTY> Collector for MinMaxAvgBinsCollector<NTY>
where
    NTY: NumOps + Serialize,
{
    type Input = MinMaxAvgBins<NTY>;
    type Output = MinMaxAvgBinsCollectedResult<NTY>;

    fn ingest(&mut self, src: &Self::Input) {
        Appendable::append(&mut self.vals, src);
    }

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(self) -> Result<Self::Output, Error> {
        let bin_count = self.vals.ts1s.len() as u32;
        // TODO could save the copy:
        let mut ts_all = self.vals.ts1s.clone();
        if self.vals.ts2s.len() > 0 {
            ts_all.push(*self.vals.ts2s.last().unwrap());
        }
        let continue_at = if self.vals.ts1s.len() < self.bin_count_exp as usize {
            match ts_all.last() {
                Some(&k) => {
                    let iso = IsoDateTime(Utc.timestamp_nanos(k as i64));
                    Some(iso)
                }
                None => Err(Error::with_msg("partial_content but no bin in result"))?,
            }
        } else {
            None
        };
        let tst = ts_offs_from_abs(&ts_all);
        let ret = MinMaxAvgBinsCollectedResult::<NTY> {
            ts_anchor_sec: tst.0,
            ts_off_ms: tst.1,
            ts_off_ns: tst.2,
            counts: self.vals.counts,
            mins: self.vals.mins,
            maxs: self.vals.maxs,
            avgs: self.vals.avgs,
            finalised_range: self.range_complete,
            missing_bins: self.bin_count_exp - bin_count,
            continue_at,
        };
        Ok(ret)
    }
}

impl<NTY> Collectable for MinMaxAvgBins<NTY>
where
    NTY: NumOps + Serialize,
{
    type Collector = MinMaxAvgBinsCollector<NTY>;

    fn new_collector(bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new(bin_count_exp)
    }
}

pub struct EventValuesAggregator<NTY> {
    range: NanoRange,
    count: u64,
    min: Option<NTY>,
    max: Option<NTY>,
    sumc: u64,
    sum: f32,
}

impl<NTY> EventValuesAggregator<NTY> {
    pub fn new(range: NanoRange) -> Self {
        Self {
            range,
            count: 0,
            min: None,
            max: None,
            sum: 0f32,
            sumc: 0,
        }
    }
}

impl<NTY> TimeBinnableTypeAggregator for EventValuesAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = EventValues<NTY>;
    type Output = MinMaxAvgBins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            if ts < self.range.beg {
                continue;
            } else if ts >= self.range.end {
                continue;
            } else {
                let v = item.values[i1];
                self.min = match self.min {
                    None => Some(v),
                    Some(min) => {
                        if v < min {
                            Some(v)
                        } else {
                            Some(min)
                        }
                    }
                };
                self.max = match self.max {
                    None => Some(v),
                    Some(max) => {
                        if v > max {
                            Some(v)
                        } else {
                            Some(max)
                        }
                    }
                };
                let vf = v.as_();
                if vf.is_nan() {
                } else {
                    self.sum += vf;
                    self.sumc += 1;
                }
                self.count += 1;
            }
        }
    }

    fn result(self) -> Self::Output {
        let avg = if self.sumc == 0 {
            None
        } else {
            Some(self.sum / self.sumc as f32)
        };
        Self::Output {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![self.min],
            maxs: vec![self.max],
            avgs: vec![avg],
        }
    }
}

pub struct MinMaxAvgBinsAggregator<NTY> {
    range: NanoRange,
    count: u64,
    min: Option<NTY>,
    max: Option<NTY>,
    sumc: u64,
    sum: f32,
}

impl<NTY> MinMaxAvgBinsAggregator<NTY> {
    pub fn new(range: NanoRange) -> Self {
        Self {
            range,
            count: 0,
            min: None,
            max: None,
            sumc: 0,
            sum: 0f32,
        }
    }
}

impl<NTY> TimeBinnableTypeAggregator for MinMaxAvgBinsAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = MinMaxAvgBins<NTY>;
    type Output = MinMaxAvgBins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        for i1 in 0..item.ts1s.len() {
            if item.ts2s[i1] <= self.range.beg {
                continue;
            } else if item.ts1s[i1] >= self.range.end {
                continue;
            } else {
                self.min = match self.min {
                    None => item.mins[i1],
                    Some(min) => match item.mins[i1] {
                        None => Some(min),
                        Some(v) => {
                            if v < min {
                                Some(v)
                            } else {
                                Some(min)
                            }
                        }
                    },
                };
                self.max = match self.max {
                    None => item.maxs[i1],
                    Some(max) => match item.maxs[i1] {
                        None => Some(max),
                        Some(v) => {
                            if v > max {
                                Some(v)
                            } else {
                                Some(max)
                            }
                        }
                    },
                };
                match item.avgs[i1] {
                    None => {}
                    Some(v) => {
                        if v.is_nan() {
                        } else {
                            self.sum += v;
                            self.sumc += 1;
                        }
                    }
                }
                self.count += item.counts[i1];
            }
        }
    }

    fn result(self) -> Self::Output {
        let avg = if self.sumc == 0 {
            None
        } else {
            Some(self.sum / self.sumc as f32)
        };
        Self::Output {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![self.min],
            maxs: vec![self.max],
            avgs: vec![avg],
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RangeCompletableItem<T> {
    RangeComplete,
    Data(T),
}

#[derive(Serialize, Deserialize)]
pub struct MinMaxAvgWaveBins<NTY> {
    pub ts1s: Vec<u64>,
    pub ts2s: Vec<u64>,
    pub counts: Vec<u64>,
    pub mins: Vec<Option<Vec<NTY>>>,
    pub maxs: Vec<Option<Vec<NTY>>>,
    pub avgs: Vec<Option<Vec<f32>>>,
}

impl<NTY> fmt::Debug for MinMaxAvgWaveBins<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "MinMaxAvgWaveBins  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  mins {:?}  maxs {:?}  avgs {:?}",
            self.ts1s.len(),
            self.ts1s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.ts2s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.counts,
            self.mins,
            self.maxs,
            self.avgs,
        )
    }
}

impl<NTY> MinMaxAvgWaveBins<NTY> {
    pub fn empty() -> Self {
        Self {
            ts1s: vec![],
            ts2s: vec![],
            counts: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
        }
    }
}

impl<NTY> FitsInside for MinMaxAvgWaveBins<NTY> {
    fn fits_inside(&self, range: NanoRange) -> Fits {
        if self.ts1s.is_empty() {
            Fits::Empty
        } else {
            let t1 = *self.ts1s.first().unwrap();
            let t2 = *self.ts2s.last().unwrap();
            if t2 <= range.beg {
                Fits::Lower
            } else if t1 >= range.end {
                Fits::Greater
            } else if t1 < range.beg && t2 > range.end {
                Fits::PartlyLowerAndGreater
            } else if t1 < range.beg {
                Fits::PartlyLower
            } else if t2 > range.end {
                Fits::PartlyGreater
            } else {
                Fits::Inside
            }
        }
    }
}

impl<NTY> FilterFittingInside for MinMaxAvgWaveBins<NTY> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> RangeOverlapInfo for MinMaxAvgWaveBins<NTY> {
    fn ends_before(&self, range: NanoRange) -> bool {
        match self.ts2s.last() {
            Some(&ts) => ts <= range.beg,
            None => true,
        }
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        match self.ts2s.last() {
            Some(&ts) => ts > range.end,
            None => panic!(),
        }
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        match self.ts1s.first() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }
}

impl<NTY> TimeBins for MinMaxAvgWaveBins<NTY>
where
    NTY: NumOps,
{
    fn ts1s(&self) -> &Vec<u64> {
        &self.ts1s
    }

    fn ts2s(&self) -> &Vec<u64> {
        &self.ts2s
    }
}

impl<NTY> WithLen for MinMaxAvgWaveBins<NTY> {
    fn len(&self) -> usize {
        self.ts1s.len()
    }
}

impl<NTY> Appendable for MinMaxAvgWaveBins<NTY>
where
    NTY: NumOps,
{
    fn empty() -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.ts1s.extend_from_slice(&src.ts1s);
        self.ts2s.extend_from_slice(&src.ts2s);
        self.counts.extend_from_slice(&src.counts);
        self.mins.extend_from_slice(&src.mins);
        self.maxs.extend_from_slice(&src.maxs);
        self.avgs.extend_from_slice(&src.avgs);
    }
}

impl<NTY> ReadableFromFile for MinMaxAvgWaveBins<NTY>
where
    NTY: NumOps,
{
    // TODO this function is not needed in the trait:
    fn read_from_file(file: File) -> Result<ReadPbv<Self>, Error> {
        Ok(ReadPbv::new(file))
    }

    fn from_buf(buf: &[u8]) -> Result<Self, Error> {
        let dec = serde_cbor::from_slice(&buf)?;
        Ok(dec)
    }
}

impl<NTY> TimeBinnableType for MinMaxAvgWaveBins<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgWaveBins<NTY>;
    type Aggregator = MinMaxAvgWaveBinsAggregator<NTY>;

    fn aggregator(range: NanoRange, x_bin_count: usize) -> Self::Aggregator {
        Self::Aggregator::new(range, x_bin_count)
    }
}

impl<NTY> ToJsonResult for Sitemty<MinMaxAvgWaveBins<NTY>>
where
    NTY: NumOps,
{
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        Ok(Box::new(serde_json::Value::String(format!(
            "MinMaxAvgBins/non-json-item"
        ))))
    }
}

pub struct MinMaxAvgWaveBinsCollected<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> MinMaxAvgWaveBinsCollected<NTY> {
    pub fn new() -> Self {
        Self { _m1: PhantomData }
    }
}

#[derive(Serialize)]
pub struct MinMaxAvgWaveBinsCollectedResult<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: Vec<u64>,
    counts: Vec<u64>,
    mins: Vec<Option<Vec<NTY>>>,
    maxs: Vec<Option<Vec<NTY>>>,
    avgs: Vec<Option<Vec<f32>>>,
    #[serde(skip_serializing_if = "Bool::is_false", rename = "finalisedRange")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "Zero::is_zero", rename = "missingBins")]
    missing_bins: u32,
    #[serde(skip_serializing_if = "Option::is_none", rename = "continueAt")]
    continue_at: Option<IsoDateTime>,
}

pub struct MinMaxAvgWaveBinsCollector<NTY> {
    bin_count_exp: u32,
    timed_out: bool,
    range_complete: bool,
    vals: MinMaxAvgWaveBins<NTY>,
    _m1: PhantomData<NTY>,
}

impl<NTY> MinMaxAvgWaveBinsCollector<NTY> {
    pub fn new(bin_count_exp: u32) -> Self {
        Self {
            bin_count_exp,
            timed_out: false,
            range_complete: false,
            vals: MinMaxAvgWaveBins::<NTY>::empty(),
            _m1: PhantomData,
        }
    }
}

impl<NTY> WithLen for MinMaxAvgWaveBinsCollector<NTY>
where
    NTY: NumOps + Serialize,
{
    fn len(&self) -> usize {
        self.vals.ts1s.len()
    }
}

impl<NTY> Collector for MinMaxAvgWaveBinsCollector<NTY>
where
    NTY: NumOps + Serialize,
{
    type Input = MinMaxAvgWaveBins<NTY>;
    type Output = MinMaxAvgWaveBinsCollectedResult<NTY>;

    fn ingest(&mut self, src: &Self::Input) {
        Appendable::append(&mut self.vals, src);
    }

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(self) -> Result<Self::Output, Error> {
        let t_bin_count = self.vals.counts.len();
        // TODO could save the copy:
        let mut ts_all = self.vals.ts1s.clone();
        if self.vals.ts2s.len() > 0 {
            ts_all.push(*self.vals.ts2s.last().unwrap());
        }
        let continue_at = if self.vals.ts1s.len() < self.bin_count_exp as usize {
            match ts_all.last() {
                Some(&k) => {
                    let iso = IsoDateTime(Utc.timestamp_nanos(k as i64));
                    Some(iso)
                }
                None => Err(Error::with_msg("partial_content but no bin in result"))?,
            }
        } else {
            None
        };
        let tst = ts_offs_from_abs(&ts_all);
        let ret = MinMaxAvgWaveBinsCollectedResult {
            ts_anchor_sec: tst.0,
            ts_off_ms: tst.1,
            ts_off_ns: tst.2,
            counts: self.vals.counts,
            mins: self.vals.mins,
            maxs: self.vals.maxs,
            avgs: self.vals.avgs,
            finalised_range: self.range_complete,
            missing_bins: self.bin_count_exp - t_bin_count as u32,
            continue_at,
        };
        Ok(ret)
    }
}

impl<NTY> Collectable for MinMaxAvgWaveBins<NTY>
where
    NTY: NumOps + Serialize,
{
    type Collector = MinMaxAvgWaveBinsCollector<NTY>;

    fn new_collector(bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new(bin_count_exp)
    }
}

pub struct MinMaxAvgWaveBinsAggregator<NTY> {
    range: NanoRange,
    count: u64,
    min: Vec<NTY>,
    max: Vec<NTY>,
    sum: Vec<f32>,
    sumc: u64,
}

impl<NTY> MinMaxAvgWaveBinsAggregator<NTY>
where
    NTY: NumOps,
{
    pub fn new(range: NanoRange, x_bin_count: usize) -> Self {
        Self {
            range,
            count: 0,
            min: vec![NTY::max_or_nan(); x_bin_count],
            max: vec![NTY::min_or_nan(); x_bin_count],
            sum: vec![0f32; x_bin_count],
            sumc: 0,
        }
    }
}

impl<NTY> TimeBinnableTypeAggregator for MinMaxAvgWaveBinsAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = MinMaxAvgWaveBins<NTY>;
    type Output = MinMaxAvgWaveBins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        for i1 in 0..item.ts1s.len() {
            if item.ts2s[i1] <= self.range.beg {
                continue;
            } else if item.ts1s[i1] >= self.range.end {
                continue;
            } else {
                // the input can contain bins where no events did fall into.
                match &item.mins[i1] {
                    None => {}
                    Some(inp) => {
                        for (a, b) in self.min.iter_mut().zip(inp.iter()) {
                            if *b < *a || a.is_nan() {
                                *a = *b;
                            }
                        }
                    }
                }
                match &item.maxs[i1] {
                    None => {}
                    Some(inp) => {
                        for (a, b) in self.max.iter_mut().zip(inp.iter()) {
                            if *b > *a || a.is_nan() {
                                *a = *b;
                            }
                        }
                    }
                }
                match &item.avgs[i1] {
                    None => {}
                    Some(inp) => {
                        for (a, b) in self.sum.iter_mut().zip(inp.iter()) {
                            *a += *b;
                        }
                    }
                }
                self.sumc += 1;
                self.count += item.counts[i1];
            }
        }
    }

    fn result(self) -> Self::Output {
        if self.sumc == 0 {
            Self::Output {
                ts1s: vec![self.range.beg],
                ts2s: vec![self.range.end],
                counts: vec![self.count],
                mins: vec![None],
                maxs: vec![None],
                avgs: vec![None],
            }
        } else {
            let avg = self.sum.iter().map(|j| *j / self.sumc as f32).collect();
            Self::Output {
                ts1s: vec![self.range.beg],
                ts2s: vec![self.range.end],
                counts: vec![self.count],
                mins: vec![Some(self.min)],
                maxs: vec![Some(self.max)],
                avgs: vec![Some(avg)],
            }
        }
    }
}
