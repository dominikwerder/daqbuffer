use crate::binned::pbv::PreBinnedValueStream;
use crate::binned::query::PreBinnedQuery;
use crate::cache::node_ix_for_patch;
use crate::decode::{
    BigEndian, Endianness, EventValueFromBytes, EventValueShape, EventValuesDim0Case, EventValuesDim1Case,
    LittleEndian, NumFromBytes,
};
use bytes::Bytes;
use dbconn::bincache::pre_binned_value_stream;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::numops::{BoolNum, NumOps, StringNum};
use items::{
    Appendable, Clearable, EventsNodeProcessor, Framable, FrameType, PushableIndex, Sitemty, TimeBinnableType,
};
use netpod::log::*;
use netpod::{AggKind, ByteOrder, ChannelTyped, NodeConfigCached, ScalarType, Shape};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::pin::Pin;

async fn make_num_pipeline_nty_end_evs_enp<NTY, END, EVS, ENP>(
    scalar_type: ScalarType,
    shape: Shape,
    agg_kind: AggKind,
    _event_value_shape: EVS,
    _events_node_proc: ENP,
    query: PreBinnedQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error>
where
    NTY: NumOps + NumFromBytes<NTY, END> + Serialize + 'static,
    END: Endianness + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
    ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Batch> + 'static,
    <ENP as EventsNodeProcessor>::Output: PushableIndex + Appendable + Clearable + 'static,
    Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + Framable + 'static,
    Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>:
        Framable + FrameType + DeserializeOwned,
{
    if let Some(scyconf) = &node_config.node_config.cluster.cache_scylla {
        info!("~~~~~~~~~~~~~~~  make_num_pipeline_nty_end_evs_enp using scylla as cache");
        let chn = ChannelTyped {
            channel: query.channel().clone(),
            scalar_type,
            shape,
        };
        let stream = pre_binned_value_stream(&chn, query.patch(), scyconf).await?;
        let stream = stream.map(|x| {
            //
            match x {
                Ok(k) => {
                    let g = Box::new(k) as Box<dyn Framable>;
                    g
                }
                Err(e) => {
                    let u: Sitemty<items::scalarevents::ScalarEvents<f32>> = Err(e);
                    Box::new(u) as Box<dyn Framable>
                }
            }
        });
        let stream = Box::pin(stream) as Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>;
        Ok(stream)
    } else {
        let ret = PreBinnedValueStream::<NTY, END, EVS, ENP>::new(query, agg_kind, node_config);
        let ret = StreamExt::map(ret, |item| Box::new(item) as Box<dyn Framable>);
        Ok(Box::pin(ret))
    }
}

async fn make_num_pipeline_nty_end<NTY, END>(
    scalar_type: ScalarType,
    shape: Shape,
    agg_kind: AggKind,
    query: PreBinnedQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error>
where
    NTY: NumOps + NumFromBytes<NTY, END> + Serialize + 'static,
    END: Endianness + 'static,
{
    match shape {
        Shape::Scalar => {
            let evs = EventValuesDim0Case::new();
            match agg_kind {
                AggKind::EventBlobs => panic!(),
                AggKind::TimeWeightedScalar | AggKind::DimXBins1 => {
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    make_num_pipeline_nty_end_evs_enp::<NTY, END, _, _>(
                        scalar_type,
                        shape,
                        agg_kind,
                        evs,
                        events_node_proc,
                        query,
                        node_config,
                    )
                    .await
                }
                AggKind::DimXBinsN(_) => {
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToNBins as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    make_num_pipeline_nty_end_evs_enp::<NTY, END, _, _>(
                        scalar_type,
                        shape,
                        agg_kind,
                        evs,
                        events_node_proc,
                        query,
                        node_config,
                    )
                    .await
                }
                AggKind::Plain => {
                    panic!();
                }
                AggKind::Stats1 => {
                    // Currently not meant to be binned.
                    panic!();
                }
            }
        }
        Shape::Wave(n) => {
            let evs = EventValuesDim1Case::new(n);
            match agg_kind {
                AggKind::EventBlobs => panic!(),
                AggKind::TimeWeightedScalar | AggKind::DimXBins1 => {
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    make_num_pipeline_nty_end_evs_enp::<NTY, END, _, _>(
                        scalar_type,
                        shape,
                        agg_kind,
                        evs,
                        events_node_proc,
                        query,
                        node_config,
                    )
                    .await
                }
                AggKind::DimXBinsN(_) => {
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToNBins as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    make_num_pipeline_nty_end_evs_enp::<NTY, END, _, _>(
                        scalar_type,
                        shape,
                        agg_kind,
                        evs,
                        events_node_proc,
                        query,
                        node_config,
                    )
                    .await
                }
                AggKind::Plain => {
                    panic!();
                }
                AggKind::Stats1 => {
                    // Currently not meant to be binned.
                    panic!();
                }
            }
        }
        Shape::Image(..) => {
            // TODO image binning/aggregation
            err::todoval()
        }
    }
}

macro_rules! match_end {
    ($nty:ident, $end:expr, $scalar_type:expr, $shape:expr, $agg_kind:expr, $query:expr, $node_config:expr) => {
        match $end {
            ByteOrder::LE => {
                make_num_pipeline_nty_end::<$nty, LittleEndian>($scalar_type, $shape, $agg_kind, $query, $node_config)
                    .await
            }
            ByteOrder::BE => {
                make_num_pipeline_nty_end::<$nty, BigEndian>($scalar_type, $shape, $agg_kind, $query, $node_config)
                    .await
            }
        }
    };
}

async fn make_num_pipeline(
    scalar_type: ScalarType,
    byte_order: ByteOrder,
    shape: Shape,
    agg_kind: AggKind,
    query: PreBinnedQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error> {
    match scalar_type {
        ScalarType::U8 => match_end!(u8, byte_order, scalar_type, shape, agg_kind, query, node_config),
        ScalarType::U16 => match_end!(u16, byte_order, scalar_type, shape, agg_kind, query, node_config),
        ScalarType::U32 => match_end!(u32, byte_order, scalar_type, shape, agg_kind, query, node_config),
        ScalarType::U64 => match_end!(u64, byte_order, scalar_type, shape, agg_kind, query, node_config),
        ScalarType::I8 => match_end!(i8, byte_order, scalar_type, shape, agg_kind, query, node_config),
        ScalarType::I16 => match_end!(i16, byte_order, scalar_type, shape, agg_kind, query, node_config),
        ScalarType::I32 => match_end!(i32, byte_order, scalar_type, shape, agg_kind, query, node_config),
        ScalarType::I64 => match_end!(i64, byte_order, scalar_type, shape, agg_kind, query, node_config),
        ScalarType::F32 => match_end!(f32, byte_order, scalar_type, shape, agg_kind, query, node_config),
        ScalarType::F64 => match_end!(f64, byte_order, scalar_type, shape, agg_kind, query, node_config),
        ScalarType::BOOL => match_end!(BoolNum, byte_order, scalar_type, shape, agg_kind, query, node_config),
        ScalarType::STRING => match_end!(StringNum, byte_order, scalar_type, shape, agg_kind, query, node_config),
    }
}

pub async fn pre_binned_bytes_for_http(
    node_config: &NodeConfigCached,
    query: &PreBinnedQuery,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>, Error> {
    if query.channel().backend != node_config.node_config.cluster.backend {
        let err = Error::with_msg(format!(
            "backend mismatch  node: {}  requested: {}",
            node_config.node_config.cluster.backend,
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
    let ret = make_num_pipeline(
        query.scalar_type().clone(),
        // TODO actually, make_num_pipeline should not depend on endianness.
        ByteOrder::LE,
        query.shape().clone(),
        query.agg_kind().clone(),
        query.clone(),
        node_config,
    )
    .await?
    .map(|item| match item.make_frame() {
        Ok(item) => Ok(item.freeze()),
        Err(e) => Err(e),
    });
    let ret = Box::pin(ret);
    Ok(ret)
}
