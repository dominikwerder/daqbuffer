use crate::agg::binnedt::{AggregatableTdim, AggregatorTdim};
use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::{Collectable, Collected, StreamItem, ToJsonResult};
use crate::agg::AggregatableXdim1Bin;
use crate::binned::scalar::binned_scalar_stream;
use crate::binnedstream::{BinnedScalarStreamFromPreBinnedPatches, BinnedStream};
use crate::cache::BinnedQuery;
use crate::channelconfig::{extract_matching_config_entry, read_local_config};
use crate::frame::makeframe::make_frame;
use crate::streamlog::LogItem;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{AggKind, BinnedRange, EventDataReadStats, NodeConfigCached};
use num_traits::Zero;
use serde::{Deserialize, Serialize, Serializer};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub mod scalar;

pub struct BinnedStreamRes<I> {
    pub binned_stream: BinnedStream<I>,
    pub range: BinnedRange,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BinnedScalarStreamItem {
    Values(MinMaxAvgScalarBinBatch),
    RangeComplete,
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

impl Collectable for BinnedScalarStreamItem {
    type Collected = MinMaxAvgScalarBinBatchCollected;

    fn append_to(&mut self, collected: &mut Self::Collected) {
        use BinnedScalarStreamItem::*;
        match self {
            Values(item) => {
                append_to_min_max_avg_scalar_bin_batch(&mut collected.batch, item);
            }
            RangeComplete => {
                // TODO use some other batch type in order to raise the range complete flag.
            }
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

impl MakeBytesFrame for Result<StreamItem<BinnedScalarStreamItem>, Error> {
    fn make_bytes_frame(&self) -> Result<Bytes, Error> {
        Ok(make_frame(self)?.freeze())
    }
}

impl AggregatableXdim1Bin for BinnedScalarStreamItem {
    // TODO does this already include all cases?
    type Output = BinnedScalarStreamItem;

    fn into_agg(self) -> Self::Output {
        todo!()
    }
}

pub struct BinnedScalarStreamItemAggregator {}

impl AggregatorTdim for BinnedScalarStreamItemAggregator {
    type InputValue = BinnedScalarStreamItem;
    // TODO using the same type for the output, does this cover all cases?
    type OutputValue = BinnedScalarStreamItem;

    fn ends_before(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn ingest(&mut self, inp: &mut Self::InputValue) {
        todo!()
    }

    fn result(self) -> Vec<Self::OutputValue> {
        todo!()
    }
}

impl AggregatableTdim for BinnedScalarStreamItem {
    type Aggregator = BinnedScalarStreamItemAggregator;
    // TODO isn't this already defined in terms of the Aggregator?
    type Output = BinnedScalarStreamItem;

    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator {
        todo!()
    }

    fn is_range_complete(&self) -> bool {
        todo!()
    }

    fn make_range_complete_item() -> Option<Self> {
        todo!()
    }

    fn is_log_item(&self) -> bool {
        todo!()
    }

    fn log_item(self) -> Option<LogItem> {
        todo!()
    }

    fn make_log_item(item: LogItem) -> Option<Self> {
        todo!()
    }

    fn is_stats_item(&self) -> bool {
        todo!()
    }

    fn stats_item(self) -> Option<EventDataReadStats> {
        todo!()
    }

    fn make_stats_item(item: EventDataReadStats) -> Option<Self> {
        todo!()
    }
}

type BinnedStreamBox = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

pub async fn binned_bytes_for_http(
    node_config: &NodeConfigCached,
    query: &BinnedQuery,
) -> Result<BinnedStreamBox, Error> {
    let channel_config = read_local_config(&query.channel(), &node_config.node).await?;
    let entry = extract_matching_config_entry(query.range(), &channel_config)?;
    info!("binned_bytes_for_http  found config entry {:?}", entry);
    match query.agg_kind() {
        AggKind::DimXBins1 => {
            let res = binned_scalar_stream(node_config, query).await?;
            let ret = BinnedBytesForHttpStream::new(res.binned_stream);
            Ok(Box::pin(ret))
        }
        AggKind::DimXBinsN(_) => {
            let res = binned_scalar_stream(node_config, query).await?;
            let ret = BinnedBytesForHttpStream::new(res.binned_stream);
            Ok(Box::pin(ret))
        }
    }
}

// TODO remove this when no longer used, gets replaced by Result<StreamItem<BinnedStreamItem>, Error>
pub type BinnedBytesForHttpStreamFrame = <BinnedScalarStreamFromPreBinnedPatches as Stream>::Item;

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
    stream: impl Stream<Item = Result<StreamItem<T>, Error>> + Unpin,
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
                        StreamItem::DataItem(mut item) => {
                            item.append_to(&mut main_item);
                            i1 += 1;
                        }
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

    let t = binned_scalar_stream(node_config, query).await?;
    let collected = collect_all(t.binned_stream, t.range.count as u32).await?;
    let ret = collected.to_json_result();
    Ok(serde_json::to_value(ret)?)
}
