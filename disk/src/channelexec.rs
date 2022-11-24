use crate::agg::enp::Identity;
use crate::decode::{BigEndian, Endianness, EventValueFromBytes, EventValueShape, LittleEndian, NumFromBytes};
use crate::decode::{EventValuesDim0Case, EventValuesDim1Case};
use crate::merge::mergedfromremotes::MergedFromRemotes;
use bytes::Bytes;
use err::Error;
use futures_core::Stream;
use futures_util::future::FutureExt;
use futures_util::StreamExt;
use items::numops::{BoolNum, NumOps, StringNum};
use items::scalarevents::ScalarEvents;
use items::streams::{collect_plain_events_json, Collectable};
use items::{Clearable, EventsNodeProcessor, Framable, FrameType, FrameTypeStatic};
use items::{PushableIndex, Sitemty, TimeBinnableType};
use netpod::query::{PlainEventsQuery, RawEventsQuery};
use netpod::{AggKind, ByteOrder, Channel, NanoRange, NodeConfigCached, PerfOpts, ScalarType, Shape};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::pin::Pin;
use std::time::Duration;

pub trait ChannelExecFunction {
    type Output;

    fn exec<NTY, END, EVS, ENP>(
        self,
        byte_order: END,
        scalar_type: ScalarType,
        shape: Shape,
        event_value_shape: EVS,
        events_node_proc: ENP,
    ) -> Result<Self::Output, Error>
    where
        NTY: NumOps + NumFromBytes<NTY, END> + 'static,
        END: Endianness + 'static,
        EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
        ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Batch> + 'static,
        // TODO require these things in general?
        <ENP as EventsNodeProcessor>::Output: Debug + Collectable + PushableIndex + Clearable,
        <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output: Debug
            + TimeBinnableType<Output = <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>
            + Collectable
            + Unpin,
        Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + Framable + 'static,
        Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>:
            FrameType + Framable + DeserializeOwned;

    fn empty() -> Self::Output;
}

fn channel_exec_nty_end_evs_enp<F, NTY, END, EVS, ENP>(
    f: F,
    byte_order: END,
    scalar_type: ScalarType,
    shape: Shape,
    event_value_shape: EVS,
    events_node_proc: ENP,
) -> Result<F::Output, Error>
where
    F: ChannelExecFunction,
    NTY: NumOps + NumFromBytes<NTY, END> + 'static,
    END: Endianness + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
    ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Batch> + 'static,
    // TODO require these things in general?
    <ENP as EventsNodeProcessor>::Output: Debug + Collectable + PushableIndex + Clearable,
    <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output: Debug
        + TimeBinnableType<Output = <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>
        + Collectable
        + Unpin,
    // TODO shouldn't one of FrameType or FrameTypeStatic be enough?
    Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + FrameTypeStatic + Framable + 'static,
    Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>:
        FrameType + FrameTypeStatic + Framable + DeserializeOwned,
{
    Ok(f.exec(byte_order, scalar_type, shape, event_value_shape, events_node_proc)?)
}

fn channel_exec_nty_end<F, NTY, END>(
    f: F,
    byte_order: END,
    scalar_type: ScalarType,
    shape: Shape,
    agg_kind: AggKind,
) -> Result<F::Output, Error>
where
    F: ChannelExecFunction,
    NTY: NumOps + NumFromBytes<NTY, END> + 'static,
    END: Endianness + 'static,
    ScalarEvents<NTY>: Collectable,
{
    match shape {
        Shape::Scalar => {
            let evs = EventValuesDim0Case::new();
            match agg_kind {
                AggKind::EventBlobs => panic!(),
                AggKind::Plain => {
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggPlain as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, scalar_type, shape, evs, events_node_proc)
                }
                AggKind::TimeWeightedScalar => {
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, scalar_type, shape, evs, events_node_proc)
                }
                AggKind::DimXBins1 => {
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, scalar_type, shape, evs, events_node_proc)
                }
                AggKind::DimXBinsN(_) => {
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToNBins as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, scalar_type, shape, evs, events_node_proc)
                }
                AggKind::Stats1 => {
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToStats1 as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, scalar_type, shape, evs, events_node_proc)
                }
            }
        }
        Shape::Wave(n) => {
            let evs = EventValuesDim1Case::new(n);
            match agg_kind {
                AggKind::EventBlobs => panic!(),
                AggKind::Plain => {
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggPlain as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, scalar_type, shape, evs, events_node_proc)
                }
                AggKind::TimeWeightedScalar => {
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, scalar_type, shape, evs, events_node_proc)
                }
                AggKind::DimXBins1 => {
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, scalar_type, shape, evs, events_node_proc)
                }
                AggKind::DimXBinsN(_) => {
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToNBins as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, scalar_type, shape, evs, events_node_proc)
                }
                AggKind::Stats1 => {
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToStats1 as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, scalar_type, shape, evs, events_node_proc)
                }
            }
        }
        Shape::Image(..) => {
            // TODO needed for binning or json event retrieval
            err::todoval()
        }
    }
}

macro_rules! match_end {
    ($f:expr, $nty:ident, $end:expr, $scalar_type:expr, $shape:expr, $agg_kind:expr, $node_config:expr) => {
        match $end {
            ByteOrder::Little => {
                channel_exec_nty_end::<_, $nty, _>($f, LittleEndian {}, $scalar_type, $shape, $agg_kind)
            }
            ByteOrder::Big => channel_exec_nty_end::<_, $nty, _>($f, BigEndian {}, $scalar_type, $shape, $agg_kind),
        }
    };
}

fn channel_exec_config<F>(
    f: F,
    scalar_type: ScalarType,
    byte_order: ByteOrder,
    shape: Shape,
    agg_kind: AggKind,
    _node_config: &NodeConfigCached,
) -> Result<F::Output, Error>
where
    F: ChannelExecFunction,
{
    match scalar_type {
        ScalarType::U8 => match_end!(f, u8, byte_order, scalar_type, shape, agg_kind, node_config),
        ScalarType::U16 => match_end!(f, u16, byte_order, scalar_type, shape, agg_kind, node_config),
        ScalarType::U32 => match_end!(f, u32, byte_order, scalar_type, shape, agg_kind, node_config),
        ScalarType::U64 => match_end!(f, u64, byte_order, scalar_type, shape, agg_kind, node_config),
        ScalarType::I8 => match_end!(f, i8, byte_order, scalar_type, shape, agg_kind, node_config),
        ScalarType::I16 => match_end!(f, i16, byte_order, scalar_type, shape, agg_kind, node_config),
        ScalarType::I32 => match_end!(f, i32, byte_order, scalar_type, shape, agg_kind, node_config),
        ScalarType::I64 => match_end!(f, i64, byte_order, scalar_type, shape, agg_kind, node_config),
        ScalarType::F32 => match_end!(f, f32, byte_order, scalar_type, shape, agg_kind, node_config),
        ScalarType::F64 => match_end!(f, f64, byte_order, scalar_type, shape, agg_kind, node_config),
        ScalarType::BOOL => match_end!(f, BoolNum, byte_order, scalar_type, shape, agg_kind, node_config),
        ScalarType::STRING => match_end!(f, StringNum, byte_order, scalar_type, shape, agg_kind, node_config),
    }
}

pub async fn channel_exec<F>(
    f: F,
    _channel: &Channel,
    _range: &NanoRange,
    scalar_type: ScalarType,
    shape: Shape,
    agg_kind: AggKind,
    node_config: &NodeConfigCached,
) -> Result<F::Output, Error>
where
    F: ChannelExecFunction,
{
    let ret = channel_exec_config(
        f,
        scalar_type,
        // TODO TODO TODO is the byte order ever important here?
        ByteOrder::Little,
        shape,
        agg_kind,
        node_config,
    )?;
    Ok(ret)
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
            agg_kind: AggKind::Plain,
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
    type Output = Pin<Box<dyn Stream<Item = Box<dyn Framable + Send>> + Send>>;

    fn exec<NTY, END, EVS, ENP>(
        self,
        byte_order: END,
        _scalar_type: ScalarType,
        _shape: Shape,
        event_value_shape: EVS,
        _events_node_proc: ENP,
    ) -> Result<Self::Output, Error>
    where
        NTY: NumOps + NumFromBytes<NTY, END> + 'static,
        END: Endianness + 'static,
        EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
        ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Batch> + 'static,
    {
        let _ = byte_order;
        let _ = event_value_shape;
        let perf_opts = PerfOpts { inmem_bufcap: 4096 };
        // TODO let upstream provide DiskIoTune and pass in RawEventsQuery:
        let evq = RawEventsQuery::new(self.channel, self.range, self.agg_kind);
        let s = MergedFromRemotes::<Identity<NTY>>::new(evq, perf_opts, self.node_config.node_config.cluster);
        let s = s.map(|item| Box::new(item) as Box<dyn Framable + Send>);
        Ok(Box::pin(s))
    }

    fn empty() -> Self::Output {
        Box::pin(futures_util::stream::empty())
    }
}

pub struct PlainEventsJson {
    query: PlainEventsQuery,
    channel: Channel,
    range: NanoRange,
    agg_kind: AggKind,
    timeout: Duration,
    node_config: NodeConfigCached,
    events_max: u64,
    do_log: bool,
}

impl PlainEventsJson {
    pub fn new(
        query: PlainEventsQuery,
        channel: Channel,
        range: NanoRange,
        timeout: Duration,
        node_config: NodeConfigCached,
        events_max: u64,
        do_log: bool,
    ) -> Self {
        Self {
            query,
            channel,
            range,
            agg_kind: AggKind::Plain,
            timeout,
            node_config,
            events_max,
            do_log,
        }
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn range(&self) -> &NanoRange {
        &self.range
    }
}

impl ChannelExecFunction for PlainEventsJson {
    type Output = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

    fn exec<NTY, END, EVS, ENP>(
        self,
        _byte_order: END,
        _scalar_type: ScalarType,
        _shape: Shape,
        _event_value_shape: EVS,
        _events_node_proc: ENP,
    ) -> Result<Self::Output, Error>
    where
        NTY: NumOps + NumFromBytes<NTY, END> + 'static,
        END: Endianness + 'static,
        EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
        ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Batch> + 'static,
        // TODO require these things in general?
        <ENP as EventsNodeProcessor>::Output: Debug + Collectable + PushableIndex + Clearable,
        <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output: Debug
            + TimeBinnableType<Output = <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>
            + Collectable
            + Unpin,
        Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + Framable + 'static,
        Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>:
            FrameType + Framable + DeserializeOwned,
    {
        let perf_opts = PerfOpts { inmem_bufcap: 4096 };
        // TODO let upstream provide DiskIoTune and set in RawEventsQuery.
        let mut evq = RawEventsQuery::new(self.channel, self.range, self.agg_kind);
        evq.do_test_main_error = self.query.do_test_main_error();
        evq.do_test_stream_error = self.query.do_test_stream_error();
        let s = MergedFromRemotes::<ENP>::new(evq, perf_opts, self.node_config.node_config.cluster);
        let f = collect_plain_events_json(s, self.timeout, 0, self.events_max, self.do_log);
        let f = FutureExt::map(f, |item| match item {
            Ok(item) => {
                // TODO add channel entry info here?
                //let obj = item.as_object_mut().unwrap();
                //obj.insert("channelName", JsonValue::String(en));
                Ok(Bytes::from(serde_json::to_vec(&item)?))
            }
            Err(e) => Err(e.into()),
        });
        let s = futures_util::stream::once(f);
        Ok(Box::pin(s))
    }

    fn empty() -> Self::Output {
        Box::pin(futures_util::stream::empty())
    }
}
