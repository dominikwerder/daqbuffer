use crate::agg::binnedt::IntoBinnedT;
use crate::agg::scalarbinbatch::{MinMaxAvgScalarBinBatch, MinMaxAvgScalarBinBatchStreamItem};
use crate::binnedstream::{BinnedStream, BinnedStreamFromPreBinnedPatches};
use crate::cache::{BinnedQuery, MergedFromRemotes};
use crate::channelconfig::{extract_matching_config_entry, read_local_config};
use crate::frame::makeframe::make_frame;
use crate::raw::EventsQuery;
use bytes::Bytes;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{BinnedRange, NodeConfigCached, PerfOpts, PreBinnedPatchIterator, PreBinnedPatchRange};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub async fn binned_stream(node_config: &NodeConfigCached, query: &BinnedQuery) -> Result<BinnedStream, Error> {
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
                range,
                query.agg_kind().clone(),
                query.cache_usage().clone(),
                node_config,
                query.disk_stats_every().clone(),
            )?;
            let ret = BinnedStream::new(Box::pin(s1))?;
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
            let s1 = MergedFromRemotes::new(evq, perf_opts, node_config.node_config.cluster.clone());
            let s1 = s1.into_binned_t(range);
            let ret = BinnedStream::new(Box::pin(s1))?;
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
    let s1 = binned_stream(node_config, query).await?;
    let ret = BinnedBytesForHttpStream::new(s1);
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

#[derive(Debug, Serialize, Deserialize)]
pub struct BinnedJsonResult {
    ts_bin_edges: Vec<u64>,
    counts: Vec<u64>,
}

pub async fn binned_json(node_config: &NodeConfigCached, query: &BinnedQuery) -> Result<serde_json::Value, Error> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(0);
    let mut batch = MinMaxAvgScalarBinBatch::empty();
    let mut items = binned_stream(node_config, query).await?;
    let mut i1 = 0;
    loop {
        let item = if i1 == 0 {
            items.next().await
        } else {
            match tokio::time::timeout_at(deadline, items.next()).await {
                Ok(k) => k,
                Err(_) => {
                    error!("TIMEOUT");
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
                            info!("APPEND BATCH  {}", vals.ts1s.len());
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
                        RangeComplete => {}
                    },
                    Err(e) => {
                        // TODO  Need to use some flags to get good enough error message for remote user.
                        Err(e)?
                    }
                };
            }
            None => break,
        }
    }
    let mut ret = BinnedJsonResult {
        ts_bin_edges: batch.ts1s,
        counts: batch.counts,
    };
    if let Some(&z) = batch.ts2s.last() {
        ret.ts_bin_edges.push(z);
    }
    Ok(serde_json::to_value(ret)?)
}
