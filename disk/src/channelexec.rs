use crate::agg::enp::{Identity, WavePlainProc};
use crate::agg::streams::{Collectable, Collector, StreamItem};
use crate::binned::{EventsNodeProcessor, NumOps, PushableIndex, RangeCompletableItem};
use crate::decode::{
    BigEndian, Endianness, EventValueFromBytes, EventValueShape, EventValues, EventValuesDim0Case, EventValuesDim1Case,
    LittleEndian, NumFromBytes,
};
use crate::frame::makeframe::{Framable, FrameType};
use crate::merge::mergedfromremotes::MergedFromRemotes;
use crate::raw::EventsQuery;
use crate::Sitemty;
use bytes::Bytes;
use err::Error;
use futures_core::Stream;
use futures_util::future::FutureExt;
use futures_util::StreamExt;
use netpod::{AggKind, ByteOrder, Channel, NanoRange, NodeConfigCached, PerfOpts, ScalarType, Shape};
use parse::channelconfig::{extract_matching_config_entry, read_local_config, MatchingConfigEntry};
use serde_json::Value as JsonValue;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::timeout_at;

pub trait ChannelExecFunction {
    type Output;

    fn exec<NTY, END, EVS>(self, byte_order: END, event_value_shape: EVS) -> Result<Self::Output, Error>
    where
        NTY: NumOps + NumFromBytes<NTY, END> + 'static,
        END: Endianness + 'static,
        EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + PlainEventsAggMethod + 'static,
        EventValues<NTY>: Collectable,
        Sitemty<<<EVS as PlainEventsAggMethod>::Method as EventsNodeProcessor>::Output>: FrameType,
        <<EVS as PlainEventsAggMethod>::Method as EventsNodeProcessor>::Output: Collectable + PushableIndex;

    fn empty() -> Self::Output;
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
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + PlainEventsAggMethod + 'static,
    EventValues<NTY>: Collectable,
    Sitemty<<<EVS as PlainEventsAggMethod>::Method as EventsNodeProcessor>::Output>: FrameType,
    <<EVS as PlainEventsAggMethod>::Method as EventsNodeProcessor>::Output: Collectable + PushableIndex,
{
    Ok(f.exec::<NTY, _, _>(byte_order, event_value_shape)?)
}

fn channel_exec_nty_end<F, NTY, END>(f: F, byte_order: END, shape: Shape) -> Result<F::Output, Error>
where
    F: ChannelExecFunction,
    NTY: NumOps + NumFromBytes<NTY, END> + 'static,
    END: Endianness + 'static,
    EventValues<NTY>: Collectable,
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
    let channel_config = match read_local_config(channel, &node_config.node).await {
        Ok(k) => k,
        Err(e) => {
            if e.msg().contains("ErrorKind::NotFound") {
                return Ok(F::empty());
            } else {
                return Err(e);
            }
        }
    };
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
    type Output = Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>;

    fn exec<NTY, END, EVS>(self, byte_order: END, event_value_shape: EVS) -> Result<Self::Output, Error>
    where
        NTY: NumOps + NumFromBytes<NTY, END> + 'static,
        END: Endianness + 'static,
        EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
        EventValues<NTY>: Collectable,
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

    fn empty() -> Self::Output {
        Box::pin(futures_util::stream::empty())
    }
}

pub struct PlainEventsJson {
    channel: Channel,
    range: NanoRange,
    agg_kind: AggKind,
    timeout: Duration,
    node_config: NodeConfigCached,
}

impl PlainEventsJson {
    pub fn new(channel: Channel, range: NanoRange, timeout: Duration, node_config: NodeConfigCached) -> Self {
        Self {
            channel,
            range,
            agg_kind: AggKind::Plain,
            timeout,
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

pub async fn collect_plain_events_json<T, S>(stream: S, timeout: Duration) -> Result<JsonValue, Error>
where
    S: Stream<Item = Sitemty<T>> + Unpin,
    T: Collectable,
{
    let deadline = tokio::time::Instant::now() + timeout;
    // TODO in general a Collector does not need to know about the expected number of bins.
    // It would make more sense for some specific Collector kind to know.
    // Therefore introduce finer grained types.
    let mut collector = <T as Collectable>::new_collector(0);
    let mut i1 = 0;
    let mut stream = stream;
    loop {
        let item = if i1 == 0 {
            stream.next().await
        } else {
            if false {
                None
            } else {
                match timeout_at(deadline, stream.next()).await {
                    Ok(k) => k,
                    Err(_) => {
                        collector.set_timed_out();
                        None
                    }
                }
            }
        };
        match item {
            Some(item) => {
                match item {
                    Ok(item) => match item {
                        StreamItem::Log(_) => {}
                        StreamItem::Stats(_) => {}
                        StreamItem::DataItem(item) => match item {
                            RangeCompletableItem::RangeComplete => {
                                collector.set_range_complete();
                            }
                            RangeCompletableItem::Data(item) => {
                                collector.ingest(&item);
                                i1 += 1;
                            }
                        },
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
    let ret = serde_json::to_value(collector.result()?)?;
    Ok(ret)
}

pub trait PlainEventsAggMethod {
    type Method: EventsNodeProcessor;
}

impl<NTY> PlainEventsAggMethod for EventValuesDim0Case<NTY>
where
    NTY: NumOps,
{
    type Method = Identity<NTY>;
}

impl<NTY> PlainEventsAggMethod for EventValuesDim1Case<NTY>
where
    NTY: NumOps,
{
    type Method = WavePlainProc<NTY>;
}

impl ChannelExecFunction for PlainEventsJson {
    type Output = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

    fn exec<NTY, END, EVS>(self, byte_order: END, event_value_shape: EVS) -> Result<Self::Output, Error>
    where
        NTY: NumOps + NumFromBytes<NTY, END> + 'static,
        END: Endianness + 'static,
        EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + PlainEventsAggMethod + 'static,
        EventValues<NTY>: Collectable,
        Sitemty<<<EVS as PlainEventsAggMethod>::Method as EventsNodeProcessor>::Output>: FrameType,
        <<EVS as PlainEventsAggMethod>::Method as EventsNodeProcessor>::Output: Collectable + PushableIndex,
    {
        let _ = byte_order;
        let _ = event_value_shape;
        let perf_opts = PerfOpts { inmem_bufcap: 4096 };
        let evq = EventsQuery {
            channel: self.channel,
            range: self.range,
            agg_kind: self.agg_kind,
        };
        let s = MergedFromRemotes::<<EVS as PlainEventsAggMethod>::Method>::new(
            evq,
            perf_opts,
            self.node_config.node_config.cluster,
        );
        let f = collect_plain_events_json(s, self.timeout);
        let f = FutureExt::map(f, |item| match item {
            Ok(item) => Ok(Bytes::from(serde_json::to_vec(&item)?)),
            Err(e) => Err(e.into()),
        });
        let s = futures_util::stream::once(f);
        Ok(Box::pin(s))
    }

    fn empty() -> Self::Output {
        Box::pin(futures_util::stream::empty())
    }
}
