use crate::agg::streams::StreamItem;
use crate::binned::pbv2::{pre_binned_value_byte_stream_new, PreBinnedValueByteStream, PreBinnedValueStream};
use crate::binned::query::PreBinnedQuery;
use crate::binned::{EventsNodeProcessor, NumOps, RangeCompletableItem, StreamKind};
use crate::cache::node_ix_for_patch;
use crate::decode::{
    BigEndian, Endianness, EventValueFromBytes, EventValueShape, EventValuesDim0Case, EventValuesDim1Case,
    LittleEndian, NumFromBytes, ProcAA, ProcBB,
};
use crate::frame::makeframe::{Framable, FrameType};
use crate::Sitemty;
use err::Error;
use futures_core::Stream;
use netpod::{ByteOrder, NodeConfigCached, ScalarType, Shape};
use parse::channelconfig::{extract_matching_config_entry, read_local_config, MatchingConfigEntry};
use std::pin::Pin;

// TODO instead of EventNodeProcessor, use a T-binning processor here
// TODO might also want another stateful processor which can run on the merged event stream, like smoothing.

fn make_num_pipeline_nty_end_evs_enp<NTY, END, EVS, ENP>(
    event_value_shape: EVS,
) -> Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>
where
    NTY: NumOps + NumFromBytes<NTY, END> + 'static,
    END: Endianness + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
    ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Output>,
    Sitemty<<ENP as EventsNodeProcessor>::Output>: Framable + 'static,
    <ENP as EventsNodeProcessor>::Output: 'static,
{
    err::todoval()
}

fn make_num_pipeline_nty_end<NTY, END>(shape: Shape) -> Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>
where
    NTY: NumOps + NumFromBytes<NTY, END> + 'static,
    END: Endianness + 'static,
{
    match shape {
        Shape::Scalar => make_num_pipeline_nty_end_evs_enp::<NTY, END, _, ProcAA<NTY>>(EventValuesDim0Case::new()),
        Shape::Wave(n) => make_num_pipeline_nty_end_evs_enp::<NTY, END, _, ProcBB<NTY>>(EventValuesDim1Case::new(n)),
    }
}

macro_rules! match_end {
    ($nty:ident, $end:expr, $shape:expr) => {
        match $end {
            ByteOrder::LE => make_num_pipeline_nty_end::<$nty, LittleEndian>($shape),
            ByteOrder::BE => make_num_pipeline_nty_end::<$nty, BigEndian>($shape),
        }
    };
}

fn make_num_pipeline(
    scalar_type: ScalarType,
    byte_order: ByteOrder,
    shape: Shape,
) -> Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>> {
    match scalar_type {
        ScalarType::I32 => match_end!(i32, byte_order, shape),
        _ => todo!(),
    }
}

pub async fn pre_binned_bytes_for_http<SK>(
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

    let channel_config = read_local_config(&query.channel(), &node_config.node).await?;
    let entry_res = extract_matching_config_entry(&query.patch().patch_range(), &channel_config)?;
    let entry = match entry_res {
        MatchingConfigEntry::None => return Err(Error::with_msg("no config entry found")),
        MatchingConfigEntry::Multiple => return Err(Error::with_msg("multiple config entries found")),
        MatchingConfigEntry::Entry(entry) => entry,
    };
    let _shape = match entry.to_shape() {
        Ok(k) => k,
        Err(e) => return Err(e),
    };

    // TODO enable
    if false {
        // TODO
        // Decide here analogue to conn in some steps which generic pipeline we use.

        PreBinnedValueStream::new(query.clone(), node_config, stream_kind.clone());
        err::todoval()
    } else {
        let ret = pre_binned_value_byte_stream_new(query, node_config, stream_kind);
        Ok(ret)
    }
}
