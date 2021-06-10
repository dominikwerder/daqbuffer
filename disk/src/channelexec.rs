use crate::agg::enp::Identity;
use crate::binned::NumOps;
use crate::decode::{
    BigEndian, Endianness, EventValueFromBytes, EventValueShape, EventValuesDim0Case, EventValuesDim1Case,
    LittleEndian, NumFromBytes,
};
use crate::frame::makeframe::Framable;
use crate::merge::mergedfromremotes::MergedFromRemotes;
use crate::raw::EventsQuery;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::{AggKind, ByteOrder, Channel, NanoRange, NodeConfigCached, PerfOpts, ScalarType, Shape};
use parse::channelconfig::{extract_matching_config_entry, read_local_config, MatchingConfigEntry};
use std::pin::Pin;

pub trait ChannelExecFunction {
    type Output;

    fn exec<NTY, END, EVS>(self, byte_order: END, event_value_shape: EVS) -> Result<Self::Output, Error>
    where
        NTY: NumOps + NumFromBytes<NTY, END> + 'static,
        END: Endianness + 'static,
        EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static;
}

fn channel_exec_nty_end_evs_enp<F, NTY, END, EVS>(
    f: F,
    byte_order: END,
    event_value_shape: EVS,
) -> Result<F::Output, Error>
where
    F: ChannelExecFunction,
    NTY: NumOps + NumFromBytes<NTY, END> + 'static,
    END: Endianness + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
{
    Ok(f.exec::<NTY, _, _>(byte_order, event_value_shape)?)
}

fn channel_exec_nty_end<F, NTY, END>(f: F, byte_order: END, shape: Shape) -> Result<F::Output, Error>
where
    F: ChannelExecFunction,
    NTY: NumOps + NumFromBytes<NTY, END> + 'static,
    END: Endianness + 'static,
{
    match shape {
        Shape::Scalar => channel_exec_nty_end_evs_enp::<_, NTY, _, _>(f, byte_order, EventValuesDim0Case::new()),
        Shape::Wave(n) => channel_exec_nty_end_evs_enp::<_, NTY, _, _>(f, byte_order, EventValuesDim1Case::new(n)),
    }
}

macro_rules! match_end {
    ($f:expr, $nty:ident, $end:expr, $shape:expr, $node_config:expr) => {
        match $end {
            ByteOrder::LE => channel_exec_nty_end::<_, $nty, _>($f, LittleEndian {}, $shape),
            ByteOrder::BE => channel_exec_nty_end::<_, $nty, _>($f, BigEndian {}, $shape),
        }
    };
}

fn channel_exec_config<F>(
    f: F,
    scalar_type: ScalarType,
    byte_order: ByteOrder,
    shape: Shape,
    _node_config: &NodeConfigCached,
) -> Result<F::Output, Error>
where
    F: ChannelExecFunction,
{
    match scalar_type {
        ScalarType::U8 => match_end!(f, u8, byte_order, shape, node_config),
        ScalarType::U16 => match_end!(f, u16, byte_order, shape, node_config),
        ScalarType::U32 => match_end!(f, u32, byte_order, shape, node_config),
        ScalarType::U64 => match_end!(f, u64, byte_order, shape, node_config),
        ScalarType::I8 => match_end!(f, i8, byte_order, shape, node_config),
        ScalarType::I16 => match_end!(f, i16, byte_order, shape, node_config),
        ScalarType::I32 => match_end!(f, i32, byte_order, shape, node_config),
        ScalarType::I64 => match_end!(f, i64, byte_order, shape, node_config),
        ScalarType::F32 => match_end!(f, f32, byte_order, shape, node_config),
        ScalarType::F64 => match_end!(f, f64, byte_order, shape, node_config),
    }
}

pub async fn channel_exec<F>(
    f: F,
    channel: &Channel,
    range: &NanoRange,
    node_config: &NodeConfigCached,
) -> Result<F::Output, Error>
where
    F: ChannelExecFunction,
{
    let channel_config = read_local_config(channel, &node_config.node).await?;
    match extract_matching_config_entry(range, &channel_config)? {
        MatchingConfigEntry::Multiple => Err(Error::with_msg("multiple config entries found"))?,
        MatchingConfigEntry::None => {
            // TODO function needs to provide some default.
            err::todoval()
        }
        MatchingConfigEntry::Entry(entry) => {
            let ret = channel_exec_config(
                f,
                entry.scalar_type.clone(),
                entry.byte_order.clone(),
                entry.to_shape()?,
                node_config,
            )?;
            Ok(ret)
        }
    }
}

pub struct PlainEvents {
    channel: Channel,
    range: NanoRange,
    agg_kind: AggKind,
    node_config: NodeConfigCached,
}

impl PlainEvents {
    pub fn new(channel: Channel, range: NanoRange, node_config: NodeConfigCached) -> Self {
        Self {
            channel,
            range,
            agg_kind: AggKind::DimXBins1,
            node_config,
        }
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn range(&self) -> &NanoRange {
        &self.range
    }
}

impl ChannelExecFunction for PlainEvents {
    type Output = Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>;

    fn exec<NTY, END, EVS>(self, byte_order: END, event_value_shape: EVS) -> Result<Self::Output, Error>
    where
        NTY: NumOps + NumFromBytes<NTY, END> + 'static,
        END: Endianness + 'static,
        EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
    {
        let _ = byte_order;
        let _ = event_value_shape;
        let perf_opts = PerfOpts { inmem_bufcap: 4096 };
        let evq = EventsQuery {
            channel: self.channel,
            range: self.range,
            agg_kind: self.agg_kind,
        };
        let s = MergedFromRemotes::<Identity<NTY>>::new(evq, perf_opts, self.node_config.node_config.cluster);
        let s = s.map(|item| Box::new(item) as Box<dyn Framable>);
        Ok(Box::pin(s))
    }
}
