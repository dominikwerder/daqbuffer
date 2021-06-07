use crate::agg::binnedt4::{DefaultBinsTimeBinner, DefaultScalarEventsTimeBinner, DefaultSingleXBinTimeBinner};
use crate::agg::enp::{Identity, WaveXBinner};
use crate::agg::streams::{Appendable, StreamItem};
use crate::binned::pbv2::{
    pre_binned_value_byte_stream_new, PreBinnedValueByteStream, PreBinnedValueByteStreamInner, PreBinnedValueStream,
};
use crate::binned::query::PreBinnedQuery;
use crate::binned::{
    BinsTimeBinner, EventsNodeProcessor, EventsTimeBinner, NumOps, PushableIndex, RangeCompletableItem,
    ReadableFromFile, StreamKind,
};
use crate::cache::node_ix_for_patch;
use crate::decode::{
    BigEndian, Endianness, EventValueFromBytes, EventValueShape, EventValuesDim0Case, EventValuesDim1Case,
    LittleEndian, NumFromBytes, ProcAA, ProcBB,
};
use crate::frame::makeframe::{Framable, FrameType};
use crate::Sitemty;
use bytes::Bytes;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::streamext::SCC;
use netpod::{ByteOrder, NodeConfigCached, ScalarType, Shape};
use parse::channelconfig::{extract_matching_config_entry, read_local_config, MatchingConfigEntry};
use serde::Serialize;
use std::pin::Pin;

fn make_num_pipeline_nty_end_evs_enp<NTY, END, EVS, ENP, ETB>(
    query: PreBinnedQuery,
    _event_value_shape: EVS,
    node_config: &NodeConfigCached,
) -> Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>
where
    NTY: NumOps + NumFromBytes<NTY, END> + Serialize + 'static,
    END: Endianness + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
    ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Output> + 'static,
    ETB: EventsTimeBinner<Input = <ENP as EventsNodeProcessor>::Output> + 'static,
    <ENP as EventsNodeProcessor>::Output: PushableIndex + Appendable + 'static,
    <ETB as EventsTimeBinner>::Output: Serialize + ReadableFromFile + 'static,
    Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + Framable + 'static,
    Sitemty<<ETB as EventsTimeBinner>::Output>: Framable,
{
    // TODO
    // Currently, this mod uses stuff from pbv2, therefore complete path:
    let ret = crate::binned::pbv::PreBinnedValueStream::<NTY, END, EVS, ENP, ETB>::new(query, node_config);
    let ret = StreamExt::map(ret, |item| Box::new(item) as Box<dyn Framable>);
    Box::pin(ret)
}

fn make_num_pipeline_nty_end<NTY, END>(
    shape: Shape,
    query: PreBinnedQuery,
    node_config: &NodeConfigCached,
) -> Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>
where
    NTY: NumOps + NumFromBytes<NTY, END> + Serialize + 'static,
    END: Endianness + 'static,
{
    match shape {
        Shape::Scalar => {
            make_num_pipeline_nty_end_evs_enp::<NTY, END, _, Identity<NTY>, DefaultScalarEventsTimeBinner<NTY>>(
                query,
                EventValuesDim0Case::new(),
                node_config,
            )
        }
        Shape::Wave(n) => {
            make_num_pipeline_nty_end_evs_enp::<NTY, END, _, WaveXBinner<NTY>, DefaultSingleXBinTimeBinner<NTY>>(
                query,
                EventValuesDim1Case::new(n),
                node_config,
            )
        }
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
    query: PreBinnedQuery,
    node_config: &NodeConfigCached,
) -> Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>> {
    match scalar_type {
        ScalarType::I32 => match_end!(i32, byte_order, shape, query, node_config),
        ScalarType::F64 => match_end!(f64, byte_order, shape, query, node_config),
        _ => todo!(),
    }
}

// TODO after the refactor, return direct value instead of boxed.
pub async fn pre_binned_bytes_for_http<SK>(
    node_config: &NodeConfigCached,
    query: &PreBinnedQuery,
    stream_kind: SK,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>, Error>
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

    if true {
        let ret = make_num_pipeline(
            entry.scalar_type.clone(),
            entry.byte_order.clone(),
            entry.to_shape().unwrap(),
            query.clone(),
            node_config,
        )
        .map(|item| match item.make_frame() {
            Ok(item) => Ok(item.freeze()),
            Err(e) => Err(e),
        });
        let ret = Box::pin(ret);
        Ok(ret)
    } else {
        let ret = pre_binned_value_byte_stream_new(query, node_config, stream_kind);
        let ret = Box::pin(ret);
        Ok(ret)
    }
}
