use crate::agg::binnedt::AggregatableTdim;
use crate::agg::binnedt2::AggregatableTdim2;
use crate::agg::binnedt3::{Agg3, BinnedT3Stream};
use crate::agg::binnedt4::{
    DefaultScalarEventsTimeBinner, DefaultSingleXBinTimeBinner, TBinnerStream, TimeBinnableType,
    TimeBinnableTypeAggregator,
};
use crate::agg::enp::{Identity, WaveXBinner, XBinnedScalarEvents};
use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::{
    Appendable, Collectable, Collectable2, Collected, CollectionSpec2, CollectionSpecMaker2, StreamItem, ToJsonResult,
};
use crate::agg::{Fits, FitsInside};
use crate::binned::binnedfrompbv::BinnedFromPreBinned;
use crate::binned::query::{BinnedQuery, PreBinnedQuery};
use crate::binned::scalar::binned_stream;
use crate::binnedstream::{BinnedScalarStreamFromPreBinnedPatches, BoxedStream};
use crate::cache::MergedFromRemotes;
use crate::decode::{
    BigEndian, Endianness, EventValueFromBytes, EventValueShape, EventValues, EventValuesDim0Case, EventValuesDim1Case,
    LittleEndian, NumFromBytes,
};
use crate::frame::makeframe::{Framable, FrameType, SubFrId};
use crate::merge::mergedfromremotes::MergedFromRemotes2;
use crate::raw::EventsQuery;
use crate::Sitemty;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{
    AggKind, BinnedRange, ByteOrder, NanoRange, NodeConfigCached, PerfOpts, PreBinnedPatchIterator,
    PreBinnedPatchRange, ScalarType, Shape,
};
use num_traits::{AsPrimitive, Bounded, Zero};
use parse::channelconfig::{extract_matching_config_entry, read_local_config, MatchingConfigEntry};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Map;
use std::any::Any;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncRead, ReadBuf};

pub mod binnedfrompbv;
pub mod pbv;
// TODO get rid of whole pbv2 mod?
pub mod pbv2;
pub mod prebinned;
pub mod query;
pub mod scalar;

pub struct BinnedStreamRes<I> {
    pub binned_stream: BoxedStream<Result<StreamItem<RangeCompletableItem<I>>, Error>>,
    pub range: BinnedRange,
}

pub struct MinMaxAvgScalarBinBatchCollected {
    batch: MinMaxAvgScalarBinBatch,
    timed_out: bool,
    finalised_range: bool,
    bin_count_exp: u32,
}

impl MinMaxAvgScalarBinBatchCollected {
    pub fn empty(bin_count_exp: u32) -> Self {
        Self {
            batch: MinMaxAvgScalarBinBatch::empty(),
            timed_out: false,
            finalised_range: false,
            bin_count_exp,
        }
    }
}

impl Collected for MinMaxAvgScalarBinBatchCollected {
    fn new(bin_count_exp: u32) -> Self {
        Self::empty(bin_count_exp)
    }

    fn timed_out(&mut self, k: bool) {
        self.timed_out = k;
    }
}

impl Collectable for MinMaxAvgScalarBinBatch {
    type Collected = MinMaxAvgScalarBinBatchCollected;

    fn append_to(&self, collected: &mut Self::Collected) {
        let batch = &mut collected.batch;
        batch.ts1s.extend_from_slice(&self.ts1s);
        batch.ts2s.extend_from_slice(&self.ts2s);
        batch.counts.extend_from_slice(&self.counts);
        batch.mins.extend_from_slice(&self.mins);
        batch.maxs.extend_from_slice(&self.maxs);
        batch.avgs.extend_from_slice(&self.avgs);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MinMaxAvgScalarBinBatchCollectedJsonResult {
    #[serde(rename = "tsBinEdges")]
    ts_bin_edges: Vec<IsoDateTime>,
    counts: Vec<u64>,
    mins: Vec<f32>,
    maxs: Vec<f32>,
    avgs: Vec<f32>,
    #[serde(skip_serializing_if = "Bool::is_false", rename = "finalisedRange")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "Zero::is_zero", rename = "missingBins")]
    missing_bins: u32,
    #[serde(skip_serializing_if = "Option::is_none", rename = "continueAt")]
    continue_at: Option<IsoDateTime>,
}

impl ToJsonResult for MinMaxAvgScalarBinBatchCollected {
    type Output = MinMaxAvgScalarBinBatchCollectedJsonResult;

    fn to_json_result(&self) -> Result<Self::Output, Error> {
        let mut tsa: Vec<_> = self
            .batch
            .ts1s
            .iter()
            .map(|&k| IsoDateTime(Utc.timestamp_nanos(k as i64)))
            .collect();
        if let Some(&z) = self.batch.ts2s.last() {
            tsa.push(IsoDateTime(Utc.timestamp_nanos(z as i64)));
        }
        let continue_at = if self.batch.ts1s.len() < self.bin_count_exp as usize {
            match tsa.last() {
                Some(k) => Some(k.clone()),
                None => Err(Error::with_msg("partial_content but no bin in result"))?,
            }
        } else {
            None
        };
        let ret = MinMaxAvgScalarBinBatchCollectedJsonResult {
            counts: self.batch.counts.clone(),
            mins: self.batch.mins.clone(),
            maxs: self.batch.maxs.clone(),
            avgs: self.batch.avgs.clone(),
            missing_bins: self.bin_count_exp - self.batch.ts1s.len() as u32,
            finalised_range: self.finalised_range,
            ts_bin_edges: tsa,
            continue_at,
        };
        Ok(ret)
    }
}

impl ToJsonResult for MinMaxAvgScalarBinBatch {
    type Output = MinMaxAvgScalarBinBatch;

    fn to_json_result(&self) -> Result<Self::Output, Error> {
        err::todo();
        let ret = MinMaxAvgScalarBinBatch {
            ts1s: self.ts1s.clone(),
            ts2s: self.ts2s.clone(),
            counts: self.counts.clone(),
            mins: self.mins.clone(),
            maxs: self.maxs.clone(),
            avgs: self.avgs.clone(),
        };
        Ok(ret)
    }
}

pub trait BinnedResponseItem: Framable {}
impl<T> BinnedResponseItem for T where T: Framable {}

pub struct BinnedResponse {
    stream: Pin<Box<dyn Stream<Item = Box<dyn BinnedResponseItem>> + Send>>,
    bin_count: u32,
    collection_spec: Box<dyn CollectionSpec2>,
}

// TODO Can I unify these functions with the ones from prebinned.rs?
// They also must resolve to the same types, so would be good to unify.

fn make_num_pipeline_nty_end_evs_enp<NTY, END, EVS, ENP>(
    query: BinnedQuery,
    _event_value_shape: EVS,
    node_config: &NodeConfigCached,
) -> Result<BinnedResponse, Error>
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
    <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output: CollectionSpecMaker2,
{
    if query.channel().backend != node_config.node.backend {
        let err = Error::with_msg(format!(
            "backend mismatch  node: {}  requested: {}",
            node_config.node.backend,
            query.channel().backend
        ));
        return Err(err);
    }
    let range = BinnedRange::covering_range(query.range().clone(), query.bin_count())?.ok_or(Error::with_msg(
        format!("binned_bytes_for_http  BinnedRange::covering_range returned None"),
    ))?;
    let perf_opts = PerfOpts { inmem_bufcap: 512 };
    //let _shape = entry.to_shape()?;
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
                query.agg_kind().clone(),
                query.cache_usage().clone(),
                node_config,
                query.disk_stats_every().clone(),
                query.report_error(),
            )?
            .map(|item| Box::new(item) as Box<dyn BinnedResponseItem>);
            let ret = BinnedResponse {
                stream: Box::pin(s),
                bin_count: range.count as u32,
                collection_spec:
                    <<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output as CollectionSpecMaker2>::spec(
                        range.count as u32,
                    ),
            };
            Ok(ret)
        }
        Ok(None) => {
            info!(
                "binned_bytes_for_http  no covering range for prebinned, merge from remotes instead {:?}",
                range
            );
            let bin_count = range.count as u32;
            let evq = EventsQuery {
                channel: query.channel().clone(),
                range: query.range().clone(),
                agg_kind: query.agg_kind().clone(),
            };
            let s = MergedFromRemotes2::<ENP>::new(evq, perf_opts, node_config.node_config.cluster.clone());
            let s = TBinnerStream::<_, <ENP as EventsNodeProcessor>::Output>::new(s, range);
            let s = StreamExt::map(s, |item| Box::new(item) as Box<dyn BinnedResponseItem>);
            let ret = BinnedResponse {
                stream: Box::pin(s),
                bin_count,
                collection_spec:
                    <<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output as CollectionSpecMaker2>::spec(
                        range.count as u32,
                    ),
            };
            Ok(ret)
        }
        Err(e) => Err(e),
    }
}

fn make_num_pipeline_nty_end<NTY, END>(
    shape: Shape,
    query: BinnedQuery,
    node_config: &NodeConfigCached,
) -> Result<BinnedResponse, Error>
where
    NTY: NumOps + NumFromBytes<NTY, END> + Serialize + 'static,
    END: Endianness + 'static,
{
    match shape {
        Shape::Scalar => make_num_pipeline_nty_end_evs_enp::<NTY, END, _, Identity<NTY>>(
            query,
            EventValuesDim0Case::new(),
            node_config,
        ),
        Shape::Wave(n) => make_num_pipeline_nty_end_evs_enp::<NTY, END, _, WaveXBinner<NTY>>(
            query,
            EventValuesDim1Case::new(n),
            node_config,
        ),
    }
}

macro_rules! match_end {
    ($nty:ident, $end:expr, $shape:expr, $query:expr, $node_config:expr) => {
        match $end {
            ByteOrder::LE => make_num_pipeline_nty_end::<$nty, LittleEndian>($shape, $query, $node_config),
            ByteOrder::BE => make_num_pipeline_nty_end::<$nty, BigEndian>($shape, $query, $node_config),
        }
    };
}

fn make_num_pipeline(
    scalar_type: ScalarType,
    byte_order: ByteOrder,
    shape: Shape,
    query: BinnedQuery,
    node_config: &NodeConfigCached,
) -> Result<BinnedResponse, Error> {
    match scalar_type {
        ScalarType::I32 => match_end!(i32, byte_order, shape, query, node_config),
        ScalarType::F64 => match_end!(f64, byte_order, shape, query, node_config),
        _ => todo!(),
    }
}

pub trait PipelinePostProcess {
    type Input;
    type Output;
    fn post(inp: Self::Input) -> Self::Output;
}

// TODO return impl Stream instead.
async fn make_num_pipeline_for_entry<PPP>(
    query: &BinnedQuery,
    node_config: &NodeConfigCached,
) -> Result<BinnedResponse, Error>
where
    PPP: PipelinePostProcess,
{
    let channel_config = read_local_config(&query.channel(), &node_config.node).await?;
    match extract_matching_config_entry(query.range(), &channel_config)? {
        MatchingConfigEntry::Multiple => Err(Error::with_msg("multiple config entries found"))?,
        MatchingConfigEntry::None => {
            // TODO can I use the same binned_stream machinery to construct the matching empty result?
            // Need the requested range all with empty/nan values and zero counts.
            let s = futures_util::stream::empty();
            let ret = BinnedResponse {
                stream: Box::pin(s),
                bin_count: 0,
                collection_spec: <MinMaxAvgBins<u8> as CollectionSpecMaker2>::spec(0),
            };
            Ok(ret)
        }
        MatchingConfigEntry::Entry(entry) => {
            // TODO make this a stream log:
            info!("binned_bytes_for_http  found config entry {:?}", entry);
            let ret = make_num_pipeline(
                entry.scalar_type.clone(),
                entry.byte_order.clone(),
                entry.to_shape()?,
                query.clone(),
                node_config,
            )?;
            Ok(ret)
        }
    }
}

struct ToFrameBytes {}

impl PipelinePostProcess for ToFrameBytes {
    type Input = ();
    type Output = ();

    fn post(inp: Self::Input) -> Self::Output {
        todo!()
    }
}

pub async fn binned_bytes_for_http(
    node_config: &NodeConfigCached,
    query: &BinnedQuery,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>, Error> {
    let pl = make_num_pipeline_for_entry::<ToFrameBytes>(query, node_config).await?;
    let ret = pl.stream.map(|item| match item.make_frame() {
        Ok(item) => Ok(item.freeze()),
        Err(e) => Err(e),
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

struct Bool {}

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

pub async fn collect_all<S>(stream: S, collection_spec: Box<dyn CollectionSpec2>) -> Result<serde_json::Value, Error>
where
    S: Stream<Item = Sitemty<Box<dyn Any>>> + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(1000);
    //let mut main_item = <T as Collectable>::Collected::new(bin_count_exp);
    let mut main_item = collection_spec.empty();
    let mut i1 = 0;
    let mut stream = stream;
    loop {
        // TODO use the trait instead to check if we have already at least one bin in the result:
        let item = if i1 == 0 {
            stream.next().await
        } else {
            match tokio::time::timeout_at(deadline, stream.next()).await {
                Ok(k) => k,
                Err(_) => {
                    // TODO

                    // TODO

                    // TODO

                    //main_item.timed_out(true);
                    None
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
                            RangeCompletableItem::RangeComplete => {}
                            RangeCompletableItem::Data(item) => {
                                main_item.append(&item);
                                //item.append_to(&mut main_item);
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
    Ok(err::todoval())
}

// TODO remove
#[derive(Debug, Serialize, Deserialize)]
pub struct UnusedBinnedJsonResult {
    ts_bin_edges: Vec<IsoDateTime>,
    counts: Vec<u64>,
    #[serde(skip_serializing_if = "Bool::is_false")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "Zero::is_zero")]
    missing_bins: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    continue_at: Option<IsoDateTime>,
}

pub async fn binned_json(node_config: &NodeConfigCached, query: &BinnedQuery) -> Result<serde_json::Value, Error> {
    err::todoval()
    /*let pl = make_num_pipeline_for_entry(query, node_config).await?;
    let collected = collect_all(pl.stream, pl.bin_count).await?;
    let ret = ToJsonResult::to_json_result(&collected)?;
    let ret = serde_json::to_value(ret)?;
    Ok(ret)*/
}

pub struct ReadPbv<T>
where
    T: ReadableFromFile,
{
    buf: Vec<u8>,
    all: Vec<u8>,
    file: Option<File>,
    _marker: std::marker::PhantomData<T>,
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
            _marker: std::marker::PhantomData::default(),
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

pub trait XBinnedEvents<SK>:
    Sized
    + Unpin
    + Send
    + Serialize
    + DeserializeOwned
    + AggregatableTdim<SK>
    + WithLen
    + WithTimestamps
    + PushableIndex
    + Appendable
where
    SK: StreamKind,
{
    fn frame_type() -> u32;
}

pub trait TBinnedBins:
    Sized
    + Unpin
    + Send
    + Serialize
    + DeserializeOwned
    + Collectable
    + ReadableFromFile
    + FilterFittingInside
    + AggregatableTdim2
    + WithLen
    + Appendable
{
    fn frame_type() -> u32;
}

impl<SK> XBinnedEvents<SK> for MinMaxAvgScalarEventBatch
where
    SK: StreamKind,
{
    fn frame_type() -> u32 {
        <Result<StreamItem<RangeCompletableItem<Self>>, Error> as FrameType>::FRAME_TYPE_ID
    }
}

impl TBinnedBins for MinMaxAvgScalarBinBatch {
    fn frame_type() -> u32 {
        <Result<StreamItem<RangeCompletableItem<Self>>, Error> as FrameType>::FRAME_TYPE_ID
    }
}

pub trait NumOps:
    Sized + Copy + Send + Unpin + Zero + AsPrimitive<f32> + Bounded + PartialOrd + SubFrId + Serialize + DeserializeOwned
{
}
impl<T> NumOps for T where
    T: Sized
        + Copy
        + Send
        + Unpin
        + Zero
        + AsPrimitive<f32>
        + Bounded
        + PartialOrd
        + SubFrId
        + Serialize
        + DeserializeOwned
{
}

pub trait EventsDecoder {
    type Output;
    fn ingest(&mut self, event: &[u8]);
    fn result(&mut self) -> Self::Output;
}

pub trait EventsNodeProcessor: Send + Unpin {
    type Input;
    type Output: Send + Unpin + DeserializeOwned + WithTimestamps + TimeBinnableType;
    fn process(inp: EventValues<Self::Input>) -> Self::Output;
}

pub trait TimeBins: Send + Unpin + WithLen + Appendable + FilterFittingInside {
    fn ts1s(&self) -> &Vec<u64>;
    fn ts2s(&self) -> &Vec<u64>;
}

// TODO remove in favor of the one in binnedt4
pub trait EventsTimeBinner: Send + Unpin {
    type Input: Unpin + RangeOverlapInfo;
    type Output: TimeBins;
    type Aggregator: EventsTimeBinnerAggregator<Input = Self::Input, Output = Self::Output> + Unpin;
    fn aggregator(range: NanoRange) -> Self::Aggregator;
}

// TODO remove in favor of the one in binnedt4
pub trait EventsTimeBinnerAggregator: Send {
    type Input: Unpin;
    type Output: Unpin;
    fn range(&self) -> &NanoRange;
    fn ingest(&mut self, item: &Self::Input);
    fn result(self) -> Self::Output;
}

pub trait BinsTimeBinner {
    type Input: TimeBins;
    type Output: TimeBins;
    fn process(inp: Self::Input) -> Self::Output;
}

#[derive(Serialize, Deserialize)]
pub struct MinMaxAvgBins<NTY> {
    pub ts1s: Vec<u64>,
    pub ts2s: Vec<u64>,
    pub counts: Vec<u64>,
    pub mins: Vec<Option<NTY>>,
    pub maxs: Vec<Option<NTY>>,
    pub avgs: Vec<Option<f32>>,
}

impl<NTY> std::fmt::Debug for MinMaxAvgBins<NTY>
where
    NTY: std::fmt::Debug,
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
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

    fn aggregator(range: NanoRange) -> Self::Aggregator {
        Self::Aggregator::new(range)
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

impl<NTY> Collectable2 for Sitemty<MinMaxAvgBins<NTY>>
where
    NTY: 'static,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }

    fn append(&mut self, src: &dyn Any) {
        todo!()
    }
}

pub struct MinMaxAvgBinsCollectionSpec<NTY> {
    bin_count_exp: u32,
    _m1: PhantomData<NTY>,
}

impl<NTY> CollectionSpec2 for MinMaxAvgBinsCollectionSpec<NTY> {
    fn empty(&self) -> Box<dyn Collectable2> {
        Box::new(MinMaxAvgBins::<NTY>::empty())
    }
}

impl<NTY> CollectionSpecMaker2 for MinMaxAvgBins<NTY>
where
    NTY: 'static,
{
    fn spec(bin_count_exp: u32) -> Box<dyn CollectionSpec2> {
        Box::new(MinMaxAvgBinsCollectionSpec::<NTY> {
            bin_count_exp,
            _m1: PhantomData,
        })
    }
}

pub struct MinMaxAvgAggregator<NTY> {
    range: NanoRange,
    count: u32,
    min: Option<NTY>,
    max: Option<NTY>,
    avg: Option<f32>,
}

impl<NTY> MinMaxAvgAggregator<NTY> {
    pub fn new(range: NanoRange) -> Self {
        Self {
            range,
            count: 0,
            min: None,
            max: None,
            avg: None,
        }
    }
}

// TODO rename to EventValuesAggregator
impl<NTY> TimeBinnableTypeAggregator for MinMaxAvgAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = EventValues<NTY>;
    type Output = MinMaxAvgBins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        todo!()
    }

    fn result(self) -> Self::Output {
        todo!()
    }
}

// TODO after refactor get rid of this impl:
impl<NTY> EventsTimeBinnerAggregator for MinMaxAvgAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = EventValues<NTY>;
    type Output = MinMaxAvgBins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        todo!()
    }

    fn result(self) -> Self::Output {
        todo!()
    }
}

pub struct MinMaxAvgBinsAggregator<NTY> {
    range: NanoRange,
    count: u32,
    min: Option<NTY>,
    max: Option<NTY>,
    avg: Option<f32>,
    sum: f32,
    sumc: u32,
}

impl<NTY> MinMaxAvgBinsAggregator<NTY> {
    pub fn new(range: NanoRange) -> Self {
        Self {
            range,
            // TODO: count events here?
            count: 0,
            min: None,
            max: None,
            avg: None,
            sum: 0f32,
            sumc: 0,
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
            // TODO
            counts: vec![0],
            mins: vec![self.min],
            maxs: vec![self.max],
            avgs: vec![avg],
        }
    }
}

pub struct SingleXBinAggregator<NTY> {
    range: NanoRange,
    count: u32,
    min: Option<NTY>,
    max: Option<NTY>,
    avg: Option<f32>,
}

impl<NTY> SingleXBinAggregator<NTY> {
    pub fn new(range: NanoRange) -> Self {
        Self {
            range,
            count: 0,
            min: None,
            max: None,
            avg: None,
        }
    }
}

impl<NTY> EventsTimeBinnerAggregator for SingleXBinAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = XBinnedScalarEvents<NTY>;
    // TODO do I need another type to carry the x-bin count as well?  No xbincount is static anyways.
    type Output = MinMaxAvgBins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        todo!()
    }

    fn result(self) -> Self::Output {
        todo!()
    }
}

pub trait StreamKind: Clone + Unpin + Send + Sync + 'static {
    type TBinnedStreamType: Stream<Item = Result<StreamItem<RangeCompletableItem<Self::TBinnedBins>>, Error>> + Send;
    type XBinnedEvents: XBinnedEvents<Self>;
    type TBinnedBins: TBinnedBins;
    type XBinnedToTBinnedAggregator;
    type XBinnedToTBinnedStream: Stream<Item = Result<StreamItem<RangeCompletableItem<Self::TBinnedBins>>, Error>>
        + Send;

    fn new_binned_from_prebinned(
        &self,
        query: &BinnedQuery,
        range: BinnedRange,
        pre_range: PreBinnedPatchRange,
        node_config: &NodeConfigCached,
    ) -> Result<Self::TBinnedStreamType, Error>;

    fn new_binned_from_merged(
        &self,
        evq: EventsQuery,
        perf_opts: PerfOpts,
        range: BinnedRange,
        node_config: &NodeConfigCached,
    ) -> Result<Self::TBinnedStreamType, Error>;

    fn xbinned_to_tbinned<S>(inp: S, spec: BinnedRange) -> Self::XBinnedToTBinnedStream
    where
        S: Stream<Item = Result<StreamItem<RangeCompletableItem<Self::XBinnedEvents>>, Error>> + Send + 'static;
}

#[derive(Clone)]
pub struct BinnedStreamKindScalar {}

#[derive(Clone)]
pub struct BinnedStreamKindWave {}

impl BinnedStreamKindScalar {
    pub fn new() -> Self {
        Self {}
    }
}

impl BinnedStreamKindWave {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RangeCompletableItem<T> {
    RangeComplete,
    Data(T),
}

impl StreamKind for BinnedStreamKindScalar {
    // TODO is this really needed?
    type TBinnedStreamType = BoxedStream<Result<StreamItem<RangeCompletableItem<Self::TBinnedBins>>, Error>>;
    type XBinnedEvents = MinMaxAvgScalarEventBatch;
    type TBinnedBins = MinMaxAvgScalarBinBatch;
    type XBinnedToTBinnedAggregator = Agg3;
    type XBinnedToTBinnedStream = BinnedT3Stream;

    fn new_binned_from_prebinned(
        &self,
        query: &BinnedQuery,
        range: BinnedRange,
        pre_range: PreBinnedPatchRange,
        node_config: &NodeConfigCached,
    ) -> Result<Self::TBinnedStreamType, Error> {
        let s = BinnedScalarStreamFromPreBinnedPatches::new(
            PreBinnedPatchIterator::from_range(pre_range),
            query.channel().clone(),
            range.clone(),
            query.agg_kind().clone(),
            query.cache_usage().clone(),
            node_config,
            query.disk_stats_every().clone(),
            query.report_error(),
            self.clone(),
        )?;
        Ok(BoxedStream::new(Box::pin(s))?)
    }

    fn new_binned_from_merged(
        &self,
        evq: EventsQuery,
        perf_opts: PerfOpts,
        range: BinnedRange,
        node_config: &NodeConfigCached,
    ) -> Result<Self::TBinnedStreamType, Error> {
        let s = MergedFromRemotes::new(evq, perf_opts, node_config.node_config.cluster.clone(), self.clone());
        let s = Self::xbinned_to_tbinned(s, range);
        Ok(BoxedStream::new(Box::pin(s))?)
    }

    fn xbinned_to_tbinned<S>(inp: S, spec: BinnedRange) -> Self::XBinnedToTBinnedStream
    where
        S: Stream<Item = Result<StreamItem<RangeCompletableItem<Self::XBinnedEvents>>, Error>> + Send + 'static,
    {
        Self::XBinnedToTBinnedStream::new(inp, spec)
    }
}
