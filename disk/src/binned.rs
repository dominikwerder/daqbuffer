use crate::agg::binnedt::IntoBinnedT;
use crate::agg::scalarbinbatch::{MinMaxAvgScalarBinBatch, MinMaxAvgScalarBinBatchStreamItem};
use crate::binnedstream::{BinnedStream, BinnedStreamFromPreBinnedPatches};
use crate::cache::{BinnedQuery, MergedFromRemotes};
use crate::channelconfig::{extract_matching_config_entry, read_local_config};
use crate::frame::makeframe::make_frame;
use crate::raw::EventsQuery;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{BinnedRange, NodeConfigCached, PerfOpts, PreBinnedPatchIterator, PreBinnedPatchRange};
use num_traits::Zero;
use serde::{Deserialize, Serialize, Serializer};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub struct BinnedStreamRes {
    pub binned_stream: BinnedStream,
    pub range: BinnedRange,
}

pub async fn binned_stream(node_config: &NodeConfigCached, query: &BinnedQuery) -> Result<BinnedStreamRes, Error> {
    if query.channel().backend != node_config.node.backend {
        let err = Error::with_msg(format!(
            "backend mismatch  node: {}  requested: {}",
            node_config.node.backend,
            query.channel().backend
        ));
        return Err(err);
    }
    let range = query.range();
    let channel_config = read_local_config(&query.channel(), &node_config.node).await?;
    let entry = extract_matching_config_entry(range, &channel_config)?;
    info!("binned_bytes_for_http  found config entry {:?}", entry);
    let range = BinnedRange::covering_range(range.clone(), query.bin_count())?.ok_or(Error::with_msg(format!(
        "binned_bytes_for_http  BinnedRange::covering_range returned None"
    )))?;
    let perf_opts = PerfOpts { inmem_bufcap: 512 };
    let _shape = entry.to_shape()?;
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
            )?;
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
    let res = binned_stream(node_config, query).await?;
    let ret = BinnedBytesForHttpStream::new(res.binned_stream);
    Ok(Box::pin(ret))
}

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

impl<S> Stream for BinnedBytesForHttpStream<S>
where
    S: Stream<Item = Result<MinMaxAvgScalarBinBatchStreamItem, Error>> + Unpin,
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
            Ready(Some(item)) => match make_frame::<BinnedBytesForHttpStreamFrame>(&item) {
                Ok(buf) => Ready(Some(Ok(buf.freeze()))),
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
    let deadline = tokio::time::Instant::now() + Duration::from_millis(1000);
    let mut batch = MinMaxAvgScalarBinBatch::empty();
    let t = binned_stream(node_config, query).await?;
    let bin_count_exp = t.range.count;
    let mut bin_count = 0;
    let mut items = t.binned_stream;
    let mut i1 = 0;
    let mut partial_content = false;
    let mut finalised_range = false;
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
                use MinMaxAvgScalarBinBatchStreamItem::*;
                match item {
                    Ok(item) => match item {
                        Values(mut vals) => {
                            // TODO gather stats about the batch sizes.
                            bin_count += vals.ts1s.len() as u64;
                            batch.ts1s.append(&mut vals.ts1s);
                            batch.ts2s.append(&mut vals.ts2s);
                            batch.counts.append(&mut vals.counts);
                            batch.mins.append(&mut vals.mins);
                            batch.maxs.append(&mut vals.maxs);
                            batch.avgs.append(&mut vals.avgs);
                            i1 += 1;
                        }
                        Log(_) => {}
                        EventDataReadStats(_) => {}
                        RangeComplete => {
                            finalised_range = true;
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
    if bin_count_exp < bin_count {
        Err(Error::with_msg("bin_count_exp < bin_count"))?
    }
    let ret = BinnedJsonResult {
        ts_bin_edges: tsa,
        counts: batch.counts,
        missing_bins: bin_count_exp - bin_count,
        finalised_range,
        continue_at,
    };
    Ok(serde_json::to_value(ret)?)
}
