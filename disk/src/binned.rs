use crate::agg::binnedt::AggregatableTdim;
use crate::agg::binnedt2::AggregatableTdim2;
use crate::agg::binnedt3::{Agg3, BinnedT3Stream};
use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::{Appendable, Collectable, Collected, StreamItem, ToJsonResult};
use crate::agg::{Fits, FitsInside};
use crate::binned::scalar::binned_stream;
use crate::binnedstream::{BinnedScalarStreamFromPreBinnedPatches, BoxedStream};
use crate::cache::{BinnedQuery, MergedFromRemotes};
use crate::decode::{Endianness, EventValues};
use crate::frame::makeframe::FrameType;
use crate::raw::EventsQuery;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{
    AggKind, BinnedRange, NanoRange, NodeConfigCached, PerfOpts, PreBinnedPatchIterator, PreBinnedPatchRange,
};
use num_traits::{AsPrimitive, Bounded, Zero};
use parse::channelconfig::{extract_matching_config_entry, read_local_config, MatchingConfigEntry};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Map;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::BitXor;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncRead, ReadBuf};

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

type BinnedBytesStreamBox = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

pub async fn binned_bytes_for_http(
    node_config: &NodeConfigCached,
    query: &BinnedQuery,
) -> Result<BinnedBytesStreamBox, Error> {
    let channel_config = read_local_config(&query.channel(), &node_config.node).await?;
    match extract_matching_config_entry(query.range(), &channel_config)? {
        MatchingConfigEntry::None => {
            // TODO can I use the same binned_stream machinery to construct the matching empty result?
            let s = futures_util::stream::empty();
            Ok(Box::pin(s))
        }
        MatchingConfigEntry::Multiple => Err(Error::with_msg("multiple config entries found"))?,
        MatchingConfigEntry::Entry(entry) => {
            info!("binned_bytes_for_http  found config entry {:?}", entry);
            match query.agg_kind() {
                AggKind::DimXBins1 => {
                    let res = binned_stream(node_config, query, BinnedStreamKindScalar::new()).await?;
                    let ret = BinnedBytesForHttpStream::new(res.binned_stream);
                    Ok(Box::pin(ret))
                }
                AggKind::DimXBinsN(_) => {
                    // TODO pass a different stream kind here:
                    err::todo();
                    let res = binned_stream(node_config, query, BinnedStreamKindScalar::new()).await?;
                    let ret = BinnedBytesForHttpStream::new(res.binned_stream);
                    Ok(Box::pin(ret))
                }
            }
        }
    }
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

pub async fn collect_all<T>(
    stream: impl Stream<Item = Result<StreamItem<RangeCompletableItem<T>>, Error>> + Unpin,
    bin_count_exp: u32,
) -> Result<<T as Collectable>::Collected, Error>
where
    T: Collectable,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(1000);
    let mut main_item = <T as Collectable>::Collected::new(bin_count_exp);
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
                    main_item.timed_out(true);
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
                                item.append_to(&mut main_item);
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
    Ok(main_item)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BinnedJsonResult {
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
    let channel_config = read_local_config(&query.channel(), &node_config.node).await?;
    match extract_matching_config_entry(query.range(), &channel_config)? {
        MatchingConfigEntry::None => {
            // TODO can I use the same binned_stream machinery to construct the matching empty result?
            Ok(serde_json::Value::Object(Map::new()))
        }
        MatchingConfigEntry::Multiple => Err(Error::with_msg("multiple config entries found"))?,
        MatchingConfigEntry::Entry(entry) => {
            info!("binned_json  found config entry {:?}", entry);
            match query.agg_kind() {
                AggKind::DimXBins1 => {
                    let res = binned_stream(node_config, query, BinnedStreamKindScalar::new()).await?;
                    //let ret = BinnedBytesForHttpStream::new(res.binned_stream);
                    //Ok(Box::pin(ret))
                    // TODO need to collect also timeout, number of missing expected bins, ...
                    let collected = collect_all(res.binned_stream, res.range.count as u32).await?;
                    let ret = ToJsonResult::to_json_result(&collected)?;
                    Ok(serde_json::to_value(ret)?)
                }
                AggKind::DimXBinsN(_xbincount) => {
                    // TODO pass a different stream kind here:
                    err::todo();
                    let res = binned_stream(node_config, query, BinnedStreamKindScalar::new()).await?;
                    // TODO need to collect also timeout, number of missing expected bins, ...
                    let collected = collect_all(res.binned_stream, res.range.count as u32).await?;
                    let ret = ToJsonResult::to_json_result(&collected)?;
                    Ok(serde_json::to_value(ret)?)
                }
            }
        }
    }
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

// TODO

// TODO

// Write a function (as demo instead of a full Stream) which couples together the flow between the
// event decoding and the event node processing.
// That should require me to require Input/Output combinations in the StreamKind.

// TODO try to get the binning generic over the actual numeric disk-dtype.
// That should require me to make StreamKind generic over the numeric dtype.
// I then need a away to compose the StreamKind:
// Instead of writing some if-else-match-monster over all possible disk-dtype and AggKind combinations,
// I would like to decide on the disk-dtype first and get some generic intermediate type, and the
// decide the AggKind, and maybe even other generic types.

pub trait NumOps: Sized + Copy + Send + Unpin + Zero + BitXor + AsPrimitive<f32> + Bounded + PartialOrd {}
impl<T> NumOps for T where T: Sized + Copy + Send + Unpin + Zero + BitXor + AsPrimitive<f32> + Bounded + PartialOrd {}

pub trait EventsDecoder {
    type Output;
    fn ingest(&mut self, event: &[u8]);
    fn result(&mut self) -> Self::Output;
}

pub trait EventsNodeProcessor {
    type Input;
    type Output;
    fn process(inp: EventValues<Self::Input>) -> Self::Output;
}

pub struct NumEvents<N> {
    _m: PhantomData<N>,
}
impl<N> NumEvents<N> {
    pub fn new() -> Self {
        Self { _m: PhantomData }
    }
}

pub struct NumSingleXBin<N> {
    _m: PhantomData<N>,
}
impl<N> NumSingleXBin<N> {
    pub fn new() -> Self {
        Self { _m: PhantomData }
    }
}

pub struct NumEventsDecoder<N, END>
where
    END: Endianness,
{
    _m1: PhantomData<N>,
    _m2: PhantomData<END>,
}

impl<N, END> NumEventsDecoder<N, END>
where
    END: Endianness,
{
    pub fn new() -> Self {
        Self {
            _m1: PhantomData,
            _m2: PhantomData,
        }
    }
}

impl<N, END> EventsDecoder for NumEventsDecoder<N, END>
where
    END: Endianness,
{
    type Output = NumEvents<N>;
    fn ingest(&mut self, _event: &[u8]) {}
    fn result(&mut self) -> Self::Output {
        err::todoval()
    }
}

pub trait BinnedPipeline {
    type EventsDecoder: EventsDecoder;
    type EventsNodeProcessor: EventsNodeProcessor;
    fn events_decoder(&self) -> Self::EventsDecoder;
    fn events_node_processor(&self) -> Self::EventsNodeProcessor;
}

pub struct NumBinnedPipeline<N, END, ENP>
where
    END: Endianness,
{
    _m1: PhantomData<N>,
    _m2: PhantomData<END>,
    _m3: PhantomData<ENP>,
}

impl<N, END, ENP> NumBinnedPipeline<N, END, ENP>
where
    END: Endianness,
{
    pub fn new() -> Self {
        Self {
            _m1: PhantomData,
            _m2: PhantomData,
            _m3: PhantomData,
        }
    }
}

impl<N, END, ENP> BinnedPipeline for NumBinnedPipeline<N, END, ENP>
where
    ENP: EventsNodeProcessor,
    END: Endianness,
{
    type EventsDecoder = NumEventsDecoder<N, END>;
    type EventsNodeProcessor = ENP;
    fn events_decoder(&self) -> Self::EventsDecoder {
        todo!()
    }
    fn events_node_processor(&self) -> Self::EventsNodeProcessor {
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
