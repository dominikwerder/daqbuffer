use crate::agg::binnedt::IntoBinnedT;
use crate::agg::scalarbinbatch::{MinMaxAvgScalarBinBatch, MinMaxAvgScalarBinBatchStreamItem};
use crate::agg::streams::{Batchable, Bins, StatsItem, StreamItem};
use crate::binnedstream::{BinnedStream, BinnedStreamFromPreBinnedPatches};
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
use netpod::{AggKind, BinnedRange, NodeConfigCached, PerfOpts, PreBinnedPatchIterator, PreBinnedPatchRange};
use num_traits::Zero;
use serde::{Deserialize, Serialize, Serializer};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub struct BinnedStreamRes<I> {
    pub binned_stream: BinnedStream<I>,
    pub range: BinnedRange,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BinnedScalarStreamItem {
    Values(MinMaxAvgScalarBinBatch),
    RangeComplete,
}

impl MakeBytesFrame for Result<StreamItem<BinnedScalarStreamItem>, Error> {
    fn make_bytes_frame(&self) -> Result<Bytes, Error> {
        Ok(make_frame::<Self>(self)?.freeze())
    }
}

impl FrameType for Result<StreamItem<BinnedScalarStreamItem>, Error> {
    const FRAME_TYPE_ID: u32 = 0x02;
}

fn adapter_to_stream_item(
    k: Result<MinMaxAvgScalarBinBatchStreamItem, Error>,
) -> Result<StreamItem<BinnedScalarStreamItem>, Error> {
    match k {
        Ok(k) => match k {
            MinMaxAvgScalarBinBatchStreamItem::Log(item) => Ok(StreamItem::Log(item)),
            MinMaxAvgScalarBinBatchStreamItem::EventDataReadStats(item) => {
                Ok(StreamItem::Stats(StatsItem::EventDataReadStats(item)))
            }
            MinMaxAvgScalarBinBatchStreamItem::RangeComplete => {
                Ok(StreamItem::DataItem(BinnedScalarStreamItem::RangeComplete))
            }
            MinMaxAvgScalarBinBatchStreamItem::Values(item) => {
                Ok(StreamItem::DataItem(BinnedScalarStreamItem::Values(item)))
            }
        },
        Err(e) => Err(e),
    }
}

pub async fn binned_scalar_stream(
    node_config: &NodeConfigCached,
    query: &BinnedQuery,
) -> Result<BinnedStreamRes<Result<StreamItem<BinnedScalarStreamItem>, Error>>, Error> {
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
            let s1 = BinnedStreamFromPreBinnedPatches::new(
                PreBinnedPatchIterator::from_range(pre_range),
                query.channel().clone(),
                range.clone(),
                query.agg_kind().clone(),
                query.cache_usage().clone(),
                node_config,
                query.disk_stats_every().clone(),
            )?
            .map(adapter_to_stream_item);
            let s = BinnedStream::new(Box::pin(s1))?;
            let ret = BinnedStreamRes {
                binned_stream: s,
                range,
            };
            Ok(ret)
        }
        Ok(None) => {
            info!(
                "binned_bytes_for_http  no covering range for prebinned, merge from remotes instead {:?}",
                range
            );
            let evq = EventsQuery {
                channel: query.channel().clone(),
                range: query.range().clone(),
                agg_kind: query.agg_kind().clone(),
            };
            // TODO do I need to set up more transformations or binning to deliver the requested data?
            let s = MergedFromRemotes::new(evq, perf_opts, node_config.node_config.cluster.clone());
            let s = s.into_binned_t(range.clone());
            let s = s.map(adapter_to_stream_item);
            let s = BinnedStream::new(Box::pin(s))?;
            let ret = BinnedStreamRes {
                binned_stream: s,
                range,
            };
            Ok(ret)
        }
        Err(e) => Err(e),
    }
}

type BinnedStreamBox = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

pub async fn binned_bytes_for_http(
    node_config: &NodeConfigCached,
    query: &BinnedQuery,
) -> Result<BinnedStreamBox, Error> {
    // TODO must decide here already which AggKind so that I can call into the generic code.
    // Really here?
    // But don't I need the channel config to make a decision if the requested binning actually makes sense?
    // In that case, the function I call here, should return a boxed trait object.
    // What traits must the returned stream fulfill?
    // Also, the items in that case must be trait objects as well.
    // But that is ok, because there are only few items in the stream anyways at this stage.

    // TODO what traits do I need downstream from here?
    // TODO use boxed trait objects from here on.
    // Must be able to convert them to bytes frames..
    // TODO but the json endpoint: how should that be handled?
    // Maybe it is enough if the items can turn themselves into serde_json::Value ?

    //todo::todo;

    let channel_config = read_local_config(&query.channel(), &node_config.node).await?;
    let entry = extract_matching_config_entry(query.range(), &channel_config)?;
    info!("binned_bytes_for_http  found config entry {:?}", entry);

    match query.agg_kind() {
        AggKind::DimXBins1 => {
            let res = binned_scalar_stream(node_config, query).await?;
            let ret = BinnedBytesForHttpStream::new(res.binned_stream);
            Ok(Box::pin(ret))
        }
        AggKind::DimXBinsN(_) => err::todoval(),
    }
}

// TODO remove this when no longer used, gets replaced by Result<StreamItem<BinnedStreamItem>, Error>
pub type BinnedBytesForHttpStreamFrame = <BinnedStreamFromPreBinnedPatches as Stream>::Item;

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

    // TODO must batch the whole result stream. Can I have a trait to append all to the first received?

    // TODO must convert the all-batched last item together.

    let deadline = tokio::time::Instant::now() + Duration::from_millis(1000);
    let t = binned_scalar_stream(node_config, query).await?;
    let bin_count_exp = t.range.count;
    let mut bin_count = 0;
    let mut items = t.binned_stream;
    let mut i1 = 0;
    let mut partial_content = false;
    let mut finalised_range = false;

    // TODO factor out the collecting:
    // How can I make this generic on the item type?
    let mut main_item: Option<MinMaxAvgScalarBinBatch> = None;
    loop {
        let item = if i1 == 0 {
            items.next().await
        } else {
            match tokio::time::timeout_at(deadline, items.next()).await {
                Ok(k) => k,
                Err(_) => {
                    partial_content = true;
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
                            BinnedScalarStreamItem::RangeComplete => {
                                finalised_range = true;
                            }
                            BinnedScalarStreamItem::Values(mut vals) => {
                                bin_count += vals.bin_count();
                                match &mut main_item {
                                    Some(main) => {
                                        main.append(&mut vals);
                                    }
                                    None => {
                                        main_item = Some(vals);
                                    }
                                }
                                // TODO solve this via some trait to append to a batch:
                                // TODO gather stats about the batch sizes.
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

    // TODO handle the case when I didn't get any items..
    let batch = main_item.unwrap();
    let mut tsa: Vec<_> = batch
        .ts1s
        .iter()
        .map(|&k| IsoDateTime(Utc.timestamp_nanos(k as i64)))
        .collect();
    if let Some(&z) = batch.ts2s.last() {
        tsa.push(IsoDateTime(Utc.timestamp_nanos(z as i64)));
    }
    let continue_at = if partial_content {
        match tsa.last() {
            Some(k) => Some(k.clone()),
            None => Err(Error::with_msg("partial_content but no bin in result"))?,
        }
    } else {
        None
    };
    if bin_count_exp < bin_count as u64 {
        Err(Error::with_msg("bin_count_exp < bin_count"))?
    }
    let ret = BinnedJsonResult {
        ts_bin_edges: tsa,
        counts: batch.counts,
        missing_bins: bin_count_exp - bin_count as u64,
        finalised_range,
        continue_at,
    };
    Ok(serde_json::to_value(ret)?)
}
