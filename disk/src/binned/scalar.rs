use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatchStreamItem;
use crate::agg::streams::StreamItem;
use crate::binned::{BinnedScalarStreamItem, BinnedStreamKind, BinnedStreamRes};
use crate::binnedstream::BinnedStream;
use crate::cache::BinnedQuery;
use crate::raw::EventsQuery;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{BinnedRange, NodeConfigCached, PerfOpts, PreBinnedPatchRange};

pub fn adapter_to_stream_item(
    k: Result<StreamItem<MinMaxAvgScalarBinBatchStreamItem>, Error>,
) -> Result<StreamItem<BinnedScalarStreamItem>, Error> {
    match k {
        Ok(k) => match k {
            StreamItem::Log(item) => Ok(StreamItem::Log(item)),
            StreamItem::Stats(item) => Ok(StreamItem::Stats(item)),
            StreamItem::DataItem(item) => match item {
                MinMaxAvgScalarBinBatchStreamItem::RangeComplete => {
                    Ok(StreamItem::DataItem(BinnedScalarStreamItem::RangeComplete))
                }
                MinMaxAvgScalarBinBatchStreamItem::Values(item) => {
                    Ok(StreamItem::DataItem(BinnedScalarStreamItem::Values(item)))
                }
            },
        },
        Err(e) => Err(e),
    }
}

pub async fn binned_stream<BK>(
    node_config: &NodeConfigCached,
    query: &BinnedQuery,
    stream_kind: BK,
) -> Result<BinnedStreamRes<<BK::BinnedStreamType as Stream>::Item>, Error>
where
    BK: BinnedStreamKind,
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
            let s = BK::new_binned_from_prebinned(&stream_kind, query, range.clone(), pre_range, node_config)?;
            let s = BinnedStream::new(Box::pin(s))?;
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
            let s = BK::new_binned_from_merged(&stream_kind, evq, perf_opts, range.clone(), node_config)?;
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
