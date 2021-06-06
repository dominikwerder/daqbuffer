use crate::agg::streams::StreamItem;
use crate::binned::query::PreBinnedQuery;
use crate::binned::{RangeCompletableItem, StreamKind};
use crate::cache::node_ix_for_patch;
use crate::cache::pbv::PreBinnedValueByteStream;
use crate::frame::makeframe::FrameType;
use err::Error;
use netpod::NodeConfigCached;

pub fn pre_binned_bytes_for_http<SK>(
    node_config: &NodeConfigCached,
    query: &PreBinnedQuery,
    stream_kind: SK,
) -> Result<PreBinnedValueByteStream<SK>, Error>
where
    SK: StreamKind,
    Result<StreamItem<RangeCompletableItem<SK::TBinnedBins>>, err::Error>: FrameType,
{
    if query.channel().backend != node_config.node.backend {
        let err = Error::with_msg(format!(
            "backend mismatch  node: {}  requested: {}",
            node_config.node.backend,
            query.channel().backend
        ));
        return Err(err);
    }
    let patch_node_ix = node_ix_for_patch(query.patch(), query.channel(), &node_config.node_config.cluster);
    if node_config.ix as u32 != patch_node_ix {
        let err = Error::with_msg(format!(
            "pre_binned_bytes_for_http node mismatch  node_config.ix {}  patch_node_ix {}",
            node_config.ix, patch_node_ix
        ));
        return Err(err);
    }
    let ret = crate::cache::pbv::pre_binned_value_byte_stream_new(query, node_config, stream_kind);
    Ok(ret)
}
