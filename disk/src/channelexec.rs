use crate::agg::enp::Identity;
use crate::decode::{
    BigEndian, Endianness, EventValueFromBytes, EventValueShape, EventValuesDim0Case, EventValuesDim1Case,
    LittleEndian, NumFromBytes,
};
use crate::merge::mergedfromremotes::MergedFromRemotes;
use bytes::Bytes;
use err::Error;
use futures_core::Stream;
use futures_util::future::FutureExt;
use futures_util::StreamExt;
use items::eventvalues::EventValues;
use items::numops::{BoolNum, NumOps};
use items::streams::{Collectable, Collector};
use items::{
    Clearable, EventsNodeProcessor, Framable, FrameType, PushableIndex, RangeCompletableItem, Sitemty, StreamItem,
    TimeBinnableType,
};
use netpod::log::*;
use netpod::query::RawEventsQuery;
use netpod::{
    AggKind, ByteOrder, Channel, ChannelConfigQuery, NanoRange, NodeConfigCached, PerfOpts, ScalarType, Shape,
};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::fmt::Debug;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::timeout_at;

pub trait ChannelExecFunction {
    type Output;

    fn exec<NTY, END, EVS, ENP>(
        self,
        byte_order: END,
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
    Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + Framable + 'static,
    Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>:
        FrameType + Framable + DeserializeOwned,
{
    Ok(f.exec(byte_order, shape, event_value_shape, events_node_proc)?)
}

fn channel_exec_nty_end<F, NTY, END>(f: F, byte_order: END, shape: Shape, agg_kind: AggKind) -> Result<F::Output, Error>
where
    F: ChannelExecFunction,
    NTY: NumOps + NumFromBytes<NTY, END> + 'static,
    END: Endianness + 'static,
    EventValues<NTY>: Collectable,
{
    match shape {
        Shape::Scalar => {
            //
            match agg_kind {
                AggKind::EventBlobs => panic!(),
                AggKind::Plain => {
                    let evs = EventValuesDim0Case::new();
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggPlain as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, shape, evs, events_node_proc)
                }
                AggKind::TimeWeightedScalar => {
                    let evs = EventValuesDim0Case::new();
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, shape, evs, events_node_proc)
                }
                AggKind::DimXBins1 => {
                    let evs = EventValuesDim0Case::new();
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, shape, evs, events_node_proc)
                }
                AggKind::DimXBinsN(_) => {
                    let evs = EventValuesDim0Case::new();
                    let events_node_proc = <<EventValuesDim0Case<NTY> as EventValueShape<NTY, END>>::NumXAggToNBins as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, shape, evs, events_node_proc)
                }
            }
        }
        Shape::Wave(n) => {
            //
            match agg_kind {
                AggKind::EventBlobs => panic!(),
                AggKind::Plain => {
                    let evs = EventValuesDim1Case::new(n);
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggPlain as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, shape, evs, events_node_proc)
                }
                AggKind::TimeWeightedScalar => {
                    let evs = EventValuesDim1Case::new(n);
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, shape, evs, events_node_proc)
                }
                AggKind::DimXBins1 => {
                    let evs = EventValuesDim1Case::new(n);
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToSingleBin as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, shape, evs, events_node_proc)
                }
                AggKind::DimXBinsN(_) => {
                    let evs = EventValuesDim1Case::new(n);
                    let events_node_proc = <<EventValuesDim1Case<NTY> as EventValueShape<NTY, END>>::NumXAggToNBins as EventsNodeProcessor>::create(shape.clone(), agg_kind.clone());
                    channel_exec_nty_end_evs_enp(f, byte_order, shape, evs, events_node_proc)
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
    ($f:expr, $nty:ident, $end:expr, $shape:expr, $agg_kind:expr, $node_config:expr) => {
        match $end {
            ByteOrder::LE => channel_exec_nty_end::<_, $nty, _>($f, LittleEndian {}, $shape, $agg_kind),
            ByteOrder::BE => channel_exec_nty_end::<_, $nty, _>($f, BigEndian {}, $shape, $agg_kind),
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
        ScalarType::U8 => match_end!(f, u8, byte_order, shape, agg_kind, node_config),
        ScalarType::U16 => match_end!(f, u16, byte_order, shape, agg_kind, node_config),
        ScalarType::U32 => match_end!(f, u32, byte_order, shape, agg_kind, node_config),
        ScalarType::U64 => match_end!(f, u64, byte_order, shape, agg_kind, node_config),
        ScalarType::I8 => match_end!(f, i8, byte_order, shape, agg_kind, node_config),
        ScalarType::I16 => match_end!(f, i16, byte_order, shape, agg_kind, node_config),
        ScalarType::I32 => match_end!(f, i32, byte_order, shape, agg_kind, node_config),
        ScalarType::I64 => match_end!(f, i64, byte_order, shape, agg_kind, node_config),
        ScalarType::F32 => match_end!(f, f32, byte_order, shape, agg_kind, node_config),
        ScalarType::F64 => match_end!(f, f64, byte_order, shape, agg_kind, node_config),
        ScalarType::BOOL => match_end!(f, BoolNum, byte_order, shape, agg_kind, node_config),
    }
}

pub async fn channel_exec<F>(
    f: F,
    channel: &Channel,
    range: &NanoRange,
    agg_kind: AggKind,
    node_config: &NodeConfigCached,
) -> Result<F::Output, Error>
where
    F: ChannelExecFunction,
{
    let q = ChannelConfigQuery {
        channel: channel.clone(),
        range: range.clone(),
    };
    let conf = httpclient::get_channel_config(&q, node_config).await?;
    let ret = channel_exec_config(
        f,
        conf.scalar_type.clone(),
        // TODO is the byte order ever important here?
        conf.byte_order.unwrap_or(ByteOrder::LE).clone(),
        conf.shape.clone(),
        agg_kind,
        node_config,
    )?;
    Ok(ret)
}

pub struct PlainEvents {
    channel: Channel,
    range: NanoRange,
    agg_kind: AggKind,
    disk_io_buffer_size: usize,
    node_config: NodeConfigCached,
}

impl PlainEvents {
    pub fn new(channel: Channel, range: NanoRange, disk_io_buffer_size: usize, node_config: NodeConfigCached) -> Self {
        Self {
            channel,
            range,
            agg_kind: AggKind::Plain,
            disk_io_buffer_size,
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

    fn exec<NTY, END, EVS, ENP>(
        self,
        byte_order: END,
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
        let evq = RawEventsQuery {
            channel: self.channel,
            range: self.range,
            agg_kind: self.agg_kind,
            disk_io_buffer_size: self.disk_io_buffer_size,
            do_decompress: true,
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
    disk_io_buffer_size: usize,
    timeout: Duration,
    node_config: NodeConfigCached,
    do_log: bool,
}

impl PlainEventsJson {
    pub fn new(
        channel: Channel,
        range: NanoRange,
        disk_io_buffer_size: usize,
        timeout: Duration,
        node_config: NodeConfigCached,
        do_log: bool,
    ) -> Self {
        Self {
            channel,
            range,
            agg_kind: AggKind::Plain,
            disk_io_buffer_size,
            timeout,
            node_config,
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

// TODO rename, it is also used for binned:
pub async fn collect_plain_events_json<T, S>(
    stream: S,
    timeout: Duration,
    bin_count_exp: u32,
    do_log: bool,
) -> Result<JsonValue, Error>
where
    S: Stream<Item = Sitemty<T>> + Unpin,
    T: Collectable + Debug,
{
    let deadline = tokio::time::Instant::now() + timeout;
    // TODO in general a Collector does not need to know about the expected number of bins.
    // It would make more sense for some specific Collector kind to know.
    // Therefore introduce finer grained types.
    let mut collector = <T as Collectable>::new_collector(bin_count_exp);
    let mut i1 = 0;
    let mut stream = stream;
    let mut total_duration = Duration::ZERO;
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
                        StreamItem::Log(item) => {
                            if do_log {
                                debug!("collect_plain_events_json log {:?}", item);
                            }
                        }
                        StreamItem::Stats(item) => match item {
                            items::StatsItem::EventDataReadStats(_) => {}
                            items::StatsItem::RangeFilterStats(_) => {}
                            items::StatsItem::DiskStats(item) => match item {
                                netpod::DiskStats::OpenStats(k) => {
                                    total_duration += k.duration;
                                }
                                netpod::DiskStats::SeekStats(k) => {
                                    total_duration += k.duration;
                                }
                                netpod::DiskStats::ReadStats(k) => {
                                    total_duration += k.duration;
                                }
                                netpod::DiskStats::ReadExactStats(k) => {
                                    total_duration += k.duration;
                                }
                            },
                        },
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
    debug!("Total duration: {:?}", total_duration);
    Ok(ret)
}

impl ChannelExecFunction for PlainEventsJson {
    type Output = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

    fn exec<NTY, END, EVS, ENP>(
        self,
        byte_order: END,
        _shape: Shape,
        event_value_shape: EVS,
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
        let _ = byte_order;
        let _ = event_value_shape;
        let perf_opts = PerfOpts { inmem_bufcap: 4096 };
        let evq = RawEventsQuery {
            channel: self.channel,
            range: self.range,
            agg_kind: self.agg_kind,
            disk_io_buffer_size: self.disk_io_buffer_size,
            do_decompress: true,
        };
        let s = MergedFromRemotes::<ENP>::new(evq, perf_opts, self.node_config.node_config.cluster);
        let f = collect_plain_events_json(s, self.timeout, 0, self.do_log);
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

pub fn dummy_impl() {
    let channel: Channel = err::todoval();
    let range: NanoRange = err::todoval();
    let agg_kind: AggKind = err::todoval();
    let node_config: NodeConfigCached = err::todoval();
    let timeout: Duration = err::todoval();
    let f = PlainEventsJson::new(channel.clone(), range.clone(), 0, timeout, node_config.clone(), false);
    let _ = channel_exec(f, &channel, &range, agg_kind, &node_config);
}
