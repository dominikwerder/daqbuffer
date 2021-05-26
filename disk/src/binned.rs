use crate::agg::binnedt::{AggregatableTdim, AggregatorTdim, IntoBinnedT};
use crate::agg::binnedt2::AggregatableTdim2;
use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::scalarbinbatch::{MinMaxAvgScalarBinBatch, MinMaxAvgScalarBinBatchAggregator};
use crate::agg::streams::{Collectable, Collected, StreamItem, ToJsonResult};
use crate::agg::{AggregatableXdim1Bin, Fits, FitsInside};
use crate::binned::scalar::binned_stream;
use crate::binnedstream::{BinnedScalarStreamFromPreBinnedPatches, BoxedStream};
use crate::cache::{BinnedQuery, MergedFromRemotes};
use crate::channelconfig::{extract_matching_config_entry, read_local_config};
use crate::frame::makeframe::{make_frame, FrameType};
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
use num_traits::Zero;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};
use std::future::Future;
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

fn append_to_min_max_avg_scalar_bin_batch(batch: &mut MinMaxAvgScalarBinBatch, item: &mut MinMaxAvgScalarBinBatch) {
    batch.ts1s.append(&mut item.ts1s);
    batch.ts2s.append(&mut item.ts2s);
    batch.counts.append(&mut item.counts);
    batch.mins.append(&mut item.mins);
    batch.maxs.append(&mut item.maxs);
    batch.avgs.append(&mut item.avgs);
}

impl Collected for MinMaxAvgScalarBinBatchCollected {
    fn new(bin_count_exp: u32) -> Self {
        Self::empty(bin_count_exp)
    }

    fn timed_out(&mut self, k: bool) {
        self.timed_out = k;
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MinMaxAvgScalarBinBatchCollectedJsonResult {
    ts_bin_edges: Vec<IsoDateTime>,
    counts: Vec<u64>,
    #[serde(skip_serializing_if = "Bool::is_false")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "Zero::is_zero")]
    missing_bins: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
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
    let entry = extract_matching_config_entry(query.range(), &channel_config)?;
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
                            RangeCompletableItem::Data(mut item) => {
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
    let entry = extract_matching_config_entry(query.range(), &channel_config)?;
    info!("binned_json  found config entry {:?}", entry);

    // TODO create the matching stream based on AggKind and ConfigEntry.

    let t = binned_stream(node_config, query, BinnedStreamKindScalar::new()).await?;
    // TODO need to collect also timeout, number of missing expected bins, ...
    let collected = collect_all(t.binned_stream, t.range.count as u32).await?;
    let ret = ToJsonResult::to_json_result(&collected)?;
    Ok(serde_json::to_value(ret)?)
}

pub struct ReadPbv<T>
where
    T: ReadableFromFile,
{
    buf: Vec<u8>,
    file: Option<File>,
    _mark: std::marker::PhantomData<T>,
}

impl<T> ReadPbv<T>
where
    T: ReadableFromFile,
{
    fn new(file: File) -> Self {
        Self {
            buf: vec![],
            file: Some(file),
            _mark: std::marker::PhantomData::default(),
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
        let mut buf = vec![];
        let mut dst = ReadBuf::new(&mut buf);
        let fp = self.file.as_mut().unwrap();
        let f = Pin::new(fp);
        match File::poll_read(f, cx, &mut dst) {
            Ready(res) => match res {
                Ok(_) => {
                    if dst.filled().len() > 0 {
                        self.buf.extend_from_slice(&mut buf);
                        Pending
                    } else {
                        match T::from_buf(&mut self.buf) {
                            Ok(item) => Ready(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))),
                            Err(e) => Ready(Err(e)),
                        }
                    }
                }
                Err(e) => Ready(Err(e.into())),
            },
            Pending => Pending,
        }
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

impl Collected for MinMaxAvgScalarEventBatch {
    // TODO for this case we don't have an expected number of events. Factor out into another trait?
    fn new(bin_count_exp: u32) -> Self {
        // TODO factor out the concept of RangeComplete into another trait layer:
        MinMaxAvgScalarEventBatch::empty()
    }

    fn timed_out(&mut self, k: bool) {}
}

impl Collectable for MinMaxAvgScalarEventBatch {
    type Collected = MinMaxAvgScalarEventBatch;

    fn append_to(&self, collected: &mut Self::Collected) {
        // TODO create separate traits for different concerns:
        // Some occasion I want to just append.
        // In other case, I need to collect also timeout flag, missing bin count and such.
        collected.tss.extend_from_slice(&self.tss);
        collected.mins.extend_from_slice(&self.mins);
        collected.maxs.extend_from_slice(&self.maxs);
        collected.avgs.extend_from_slice(&self.avgs);
    }
}

impl Collected for MinMaxAvgScalarBinBatch {
    fn new(bin_count_exp: u32) -> Self {
        MinMaxAvgScalarBinBatch::empty()
    }
    fn timed_out(&mut self, k: bool) {}
}

impl Collectable for MinMaxAvgScalarBinBatch {
    type Collected = MinMaxAvgScalarBinBatch;
    fn append_to(&self, collected: &mut Self::Collected) {
        collected.ts1s.extend_from_slice(&self.ts1s);
        collected.ts2s.extend_from_slice(&self.ts2s);
        collected.counts.extend_from_slice(&self.counts);
        collected.mins.extend_from_slice(&self.mins);
        collected.maxs.extend_from_slice(&self.maxs);
        collected.avgs.extend_from_slice(&self.avgs);
    }
}

pub trait XBinnedEvents:
    Sized
    + Unpin
    + Send
    + Serialize
    + DeserializeOwned
    + Collectable
    + Collected
    + AggregatableTdim
    + WithLen
    + WithTimestamps
    + PushableIndex
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
    + Collected
    + ReadableFromFile
    + FilterFittingInside
    + AggregatableTdim2
    + WithLen
{
    fn frame_type() -> u32;
}

impl XBinnedEvents for MinMaxAvgScalarEventBatch {
    fn frame_type() -> u32 {
        <Result<StreamItem<RangeCompletableItem<Self>>, Error> as FrameType>::FRAME_TYPE_ID
    }
}

impl TBinnedBins for MinMaxAvgScalarBinBatch {
    fn frame_type() -> u32 {
        <Result<StreamItem<RangeCompletableItem<Self>>, Error> as FrameType>::FRAME_TYPE_ID
    }
}

pub trait BinnedStreamKind: Clone + Unpin + Send + Sync + 'static
// TODO would it be better to express it here?
//where Result<StreamItem<RangeCompletableItem<Self::XBinnedEvents>>, Error>: FrameType,
{
    type TBinnedStreamType: Stream<Item = Result<StreamItem<RangeCompletableItem<Self::TBinnedBins>>, Error>>
        + Send
        + 'static;
    type XBinnedEvents: XBinnedEvents;
    type TBinnedBins: TBinnedBins;

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

impl BinnedStreamKind for BinnedStreamKindScalar {
    // TODO is this really needed?
    type TBinnedStreamType = BoxedStream<Result<StreamItem<RangeCompletableItem<Self::TBinnedBins>>, Error>>;
    type XBinnedEvents = MinMaxAvgScalarEventBatch;
    type TBinnedBins = MinMaxAvgScalarBinBatch;

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
        // TODO use the binned2 instead
        let s = crate::agg::binnedt::IntoBinnedT::into_binned_t(s, range);
        Ok(BoxedStream::new(Box::pin(s))?)
    }
}

// TODO this code is needed somewhere:
fn pbv_handle_fut2_item(
    item: StreamItem<RangeCompletableItem<MinMaxAvgScalarBinBatch>>,
) -> Option<StreamItem<RangeCompletableItem<MinMaxAvgScalarBinBatch>>> {
    // TODO make this code work in this context:
    // Do I need more parameters here?
    /*Ok(item) => match item {
        StreamItem::DataItem(item) => match item {
            PreBinnedScalarItem::Batch(batch) => {
                self.values.ts1s.extend(batch.ts1s.iter());
                self.values.ts2s.extend(batch.ts2s.iter());
                self.values.counts.extend(batch.counts.iter());
                self.values.mins.extend(batch.mins.iter());
                self.values.maxs.extend(batch.maxs.iter());
                self.values.avgs.extend(batch.avgs.iter());
                StreamItem::DataItem(PreBinnedScalarItem::Batch(batch))
            }
        },
    },*/
    err::todo();
    None
}
