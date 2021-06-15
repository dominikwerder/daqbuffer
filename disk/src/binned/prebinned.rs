use crate::agg::binnedt::TimeBinnableType;
use crate::agg::streams::Appendable;
use crate::binned::pbv::PreBinnedValueStream;
use crate::binned::query::PreBinnedQuery;
use crate::binned::{EventsNodeProcessor, NumOps, PushableIndex};
use crate::cache::node_ix_for_patch;
use crate::decode::{
    BigEndian, Endianness, EventValueFromBytes, EventValueShape, EventValuesDim0Case, EventValuesDim1Case,
    LittleEndian, NumFromBytes,
};
use crate::frame::makeframe::{Framable, FrameType};
use crate::Sitemty;
use bytes::Bytes;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::{AggKind, ByteOrder, NodeConfigCached, ScalarType, Shape};
use parse::channelconfig::{extract_matching_config_entry, read_local_config, MatchingConfigEntry};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::pin::Pin;

fn make_num_pipeline_nty_end_evs_enp<NTY, END, EVS, ENP>(
    shape: Shape,
    agg_kind: AggKind,
    _event_value_shape: EVS,
    _events_node_proc: ENP,
    query: PreBinnedQuery,
    node_config: &NodeConfigCached,
) -> Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>
where
    NTY: NumOps + NumFromBytes<NTY, END> + Serialize + 'static,
    END: Endianness + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
    ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Output> + 'static,
    <ENP as EventsNodeProcessor>::Output: PushableIndex + Appendable + 'static,
    Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + Framable + 'static,
    Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>:
        Framable + FrameType + DeserializeOwned,
{
    let ret = PreBinnedValueStream::<NTY, END, EVS, ENP>::new(query, shape, agg_kind, node_config);
    let ret = StreamExt::map(ret, |item| Box::new(item) as Box<dyn Framable>);
    Box::pin(ret)
}

fn make_num_pipeline_nty_end<NTY, END>(
    shape: Shape,
    agg_kind: AggKind,
    query: PreBinnedQuery,
    node_config: &NodeConfigCached,
) -> Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>
where
    NTY: NumOps + NumFromBytes<NTY, END> + Serialize + 'static,
    END: Endianness + 'static,
{
    match shape {
        Shape::Scalar => {
            let evs = EventValuesDim0Case::new();
            match agg_kind {
                AggKind::DimXBins1 => {
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    make_num_pipeline_nty_end_evs_enp::<NTY, END, _, _>(
                        shape,
                        agg_kind,
                        evs,
                        events_node_proc,
                        query,
                        node_config,
                    )
                }
                AggKind::DimXBinsN(_) => {
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToNBins as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    make_num_pipeline_nty_end_evs_enp::<NTY, END, _, _>(
                        shape,
                        agg_kind,
                        evs,
                        events_node_proc,
                        query,
                        node_config,
                    )
                }
                AggKind::Plain => {
                    panic!();
                }
            }
        }
        Shape::Wave(n) => {
            let evs = EventValuesDim1Case::new(n);
            match agg_kind {
                AggKind::DimXBins1 => {
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    make_num_pipeline_nty_end_evs_enp::<NTY, END, _, _>(
                        shape,
                        agg_kind,
                        evs,
                        events_node_proc,
                        query,
                        node_config,
                    )
                }
                AggKind::DimXBinsN(_) => {
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToNBins as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    make_num_pipeline_nty_end_evs_enp::<NTY, END, _, _>(
                        shape,
                        agg_kind,
                        evs,
                        events_node_proc,
                        query,
                        node_config,
                    )
                }
                AggKind::Plain => {
                    panic!();
                }
            }
        }
    }
}

macro_rules! match_end {
    ($nty:ident, $end:expr, $shape:expr, $agg_kind:expr, $query:expr, $node_config:expr) => {
        match $end {
            ByteOrder::LE => make_num_pipeline_nty_end::<$nty, LittleEndian>($shape, $agg_kind, $query, $node_config),
            ByteOrder::BE => make_num_pipeline_nty_end::<$nty, BigEndian>($shape, $agg_kind, $query, $node_config),
        }
    };
}

fn make_num_pipeline(
    scalar_type: ScalarType,
    byte_order: ByteOrder,
    shape: Shape,
    agg_kind: AggKind,
    query: PreBinnedQuery,
    node_config: &NodeConfigCached,
) -> Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>> {
    match scalar_type {
        ScalarType::U8 => match_end!(u8, byte_order, shape, agg_kind, query, node_config),
        ScalarType::U16 => match_end!(u16, byte_order, shape, agg_kind, query, node_config),
        ScalarType::U32 => match_end!(u32, byte_order, shape, agg_kind, query, node_config),
        ScalarType::U64 => match_end!(u64, byte_order, shape, agg_kind, query, node_config),
        ScalarType::I8 => match_end!(i8, byte_order, shape, agg_kind, query, node_config),
        ScalarType::I16 => match_end!(i16, byte_order, shape, agg_kind, query, node_config),
        ScalarType::I32 => match_end!(i32, byte_order, shape, agg_kind, query, node_config),
        ScalarType::I64 => match_end!(i64, byte_order, shape, agg_kind, query, node_config),
        ScalarType::F32 => match_end!(f32, byte_order, shape, agg_kind, query, node_config),
        ScalarType::F64 => match_end!(f64, byte_order, shape, agg_kind, query, node_config),
    }
}

// TODO after the refactor, return direct value instead of boxed.
pub async fn pre_binned_bytes_for_http(
    node_config: &NodeConfigCached,
    query: &PreBinnedQuery,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>, Error> {
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
    let channel_config = match read_local_config(&query.channel(), &node_config.node).await {
        Ok(k) => k,
        Err(e) => {
            if e.msg().contains("ErrorKind::NotFound") {
                let s = futures_util::stream::empty();
                let ret = Box::pin(s);
                return Ok(ret);
            } else {
                return Err(e);
            }
        }
    };
    let entry_res = extract_matching_config_entry(&query.patch().patch_range(), &channel_config)?;
    let entry = match entry_res {
        MatchingConfigEntry::None => return Err(Error::with_msg("no config entry found")),
        MatchingConfigEntry::Multiple => return Err(Error::with_msg("multiple config entries found")),
        MatchingConfigEntry::Entry(entry) => entry,
    };
    let ret = make_num_pipeline(
        entry.scalar_type.clone(),
        entry.byte_order.clone(),
        entry.to_shape()?,
        query.agg_kind().clone(),
        query.clone(),
        node_config,
    )
    .map(|item| match item.make_frame() {
        Ok(item) => Ok(item.freeze()),
        Err(e) => Err(e),
    });
    let ret = Box::pin(ret);
    Ok(ret)
}
