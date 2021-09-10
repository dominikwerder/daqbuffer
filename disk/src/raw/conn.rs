use crate::decode::{
    BigEndian, Endianness, EventValueFromBytes, EventValueShape, EventValuesDim0Case, EventValuesDim1Case,
    EventsDecodedStream, LittleEndian, NumFromBytes,
};
use crate::eventblobs::EventChunkerMultifile;
use crate::eventchunker::EventChunkerConf;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::numops::{BoolNum, NumOps};
use items::{EventsNodeProcessor, Framable, RangeCompletableItem, Sitemty, StreamItem};
use netpod::query::RawEventsQuery;
use netpod::{AggKind, ByteOrder, ByteSize, NodeConfigCached, ScalarType, Shape};
use parse::channelconfig::{extract_matching_config_entry, read_local_config, MatchingConfigEntry};
use std::pin::Pin;

fn make_num_pipeline_stream_evs<NTY, END, EVS, ENP>(
    event_value_shape: EVS,
    events_node_proc: ENP,
    event_blobs: EventChunkerMultifile,
) -> Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>
where
    NTY: NumOps + NumFromBytes<NTY, END> + 'static,
    END: Endianness + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
    ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Batch> + 'static,
    Sitemty<<ENP as EventsNodeProcessor>::Output>: Framable + 'static,
    <ENP as EventsNodeProcessor>::Output: 'static,
{
    let decs = EventsDecodedStream::<NTY, END, EVS>::new(event_value_shape, event_blobs);
    let s2 = StreamExt::map(decs, move |item| match item {
        Ok(item) => match item {
            StreamItem::DataItem(item) => match item {
                RangeCompletableItem::Data(item) => {
                    let item = events_node_proc.process(item);
                    Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                }
                RangeCompletableItem::RangeComplete => Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)),
            },
            StreamItem::Log(item) => Ok(StreamItem::Log(item)),
            StreamItem::Stats(item) => Ok(StreamItem::Stats(item)),
        },
        Err(e) => Err(e),
    })
    .map(|item| Box::new(item) as Box<dyn Framable>);
    Box::pin(s2)
}

macro_rules! pipe4 {
    ($nty:ident, $end:ident, $shape:expr, $evs:ident, $evsv:expr, $agg_kind:expr, $event_blobs:expr) => {
        match $agg_kind {
            AggKind::EventBlobs => panic!(),
            AggKind::TimeWeightedScalar | AggKind::DimXBins1 => {
                make_num_pipeline_stream_evs::<$nty, $end, $evs<$nty>, _>(
                    $evsv,
                    <$evs<$nty> as EventValueShape<$nty, $end>>::NumXAggToSingleBin::create($shape, $agg_kind),
                    $event_blobs,
                )
            }
            AggKind::DimXBinsN(_) => make_num_pipeline_stream_evs::<$nty, $end, $evs<$nty>, _>(
                $evsv,
                <$evs<$nty> as EventValueShape<$nty, $end>>::NumXAggToNBins::create($shape, $agg_kind),
                $event_blobs,
            ),
            AggKind::Plain => make_num_pipeline_stream_evs::<$nty, $end, $evs<$nty>, _>(
                $evsv,
                <$evs<$nty> as EventValueShape<$nty, $end>>::NumXAggPlain::create($shape, $agg_kind),
                $event_blobs,
            ),
        }
    };
}

macro_rules! pipe3 {
    ($nty:ident, $end:ident, $shape:expr, $agg_kind:expr, $event_blobs:expr) => {
        match $shape {
            Shape::Scalar => {
                pipe4!(
                    $nty,
                    $end,
                    $shape,
                    EventValuesDim0Case,
                    EventValuesDim0Case::<$nty>::new(),
                    $agg_kind,
                    $event_blobs
                )
            }
            Shape::Wave(n) => {
                pipe4!(
                    $nty,
                    $end,
                    $shape,
                    EventValuesDim1Case,
                    EventValuesDim1Case::<$nty>::new(n),
                    $agg_kind,
                    $event_blobs
                )
            }
        }
    };
}

macro_rules! pipe2 {
    ($nty:ident, $end:expr, $shape:expr, $agg_kind:expr, $event_blobs:expr) => {
        match $end {
            ByteOrder::LE => pipe3!($nty, LittleEndian, $shape, $agg_kind, $event_blobs),
            ByteOrder::BE => pipe3!($nty, BigEndian, $shape, $agg_kind, $event_blobs),
        }
    };
}

macro_rules! pipe1 {
    ($nty:expr, $end:expr, $shape:expr, $agg_kind:expr, $event_blobs:expr) => {
        match $nty {
            ScalarType::U8 => pipe2!(u8, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::U16 => pipe2!(u16, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::U32 => pipe2!(u32, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::U64 => pipe2!(u64, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::I8 => pipe2!(i8, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::I16 => pipe2!(i16, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::I32 => pipe2!(i32, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::I64 => pipe2!(i64, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::F32 => pipe2!(f32, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::F64 => pipe2!(f64, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::BOOL => pipe2!(BoolNum, $end, $shape, $agg_kind, $event_blobs),
        }
    };
}

pub async fn make_event_pipe(
    evq: &RawEventsQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error> {
    if false {
        match dbconn::channel_exists(&evq.channel, &node_config).await {
            Ok(_) => (),
            Err(e) => return Err(e)?,
        }
    }
    let range = &evq.range;
    let channel_config = match read_local_config(&evq.channel, &node_config.node).await {
        Ok(k) => k,
        Err(e) => {
            if e.msg().contains("ErrorKind::NotFound") {
                let s = futures_util::stream::empty();
                return Ok(Box::pin(s));
            } else {
                return Err(e)?;
            }
        }
    };
    let entry_res = match extract_matching_config_entry(range, &channel_config) {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    let entry = match entry_res {
        MatchingConfigEntry::None => return Err(Error::with_msg("no config entry found"))?,
        MatchingConfigEntry::Multiple => return Err(Error::with_msg("multiple config entries found"))?,
        MatchingConfigEntry::Entry(entry) => entry,
    };
    let shape = match entry.to_shape() {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    let channel_config = netpod::ChannelConfig {
        channel: evq.channel.clone(),
        keyspace: entry.ks as u8,
        time_bin_size: entry.bs,
        shape: shape,
        scalar_type: entry.scalar_type.clone(),
        byte_order: entry.byte_order.clone(),
        array: entry.is_array,
        compression: entry.is_compressed,
    };
    let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
    let event_blobs = EventChunkerMultifile::new(
        range.clone(),
        channel_config.clone(),
        node_config.node.clone(),
        node_config.ix,
        evq.disk_io_buffer_size,
        event_chunker_conf,
        true,
    );
    let shape = entry.to_shape()?;
    let pipe = pipe1!(
        entry.scalar_type,
        entry.byte_order,
        shape,
        evq.agg_kind.clone(),
        event_blobs
    );
    Ok(pipe)
}

pub async fn make_event_blobs_pipe(
    evq: &RawEventsQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error> {
    if false {
        match dbconn::channel_exists(&evq.channel, &node_config).await {
            Ok(_) => (),
            Err(e) => return Err(e)?,
        }
    }
    let range = &evq.range;
    let channel_config = match read_local_config(&evq.channel, &node_config.node).await {
        Ok(k) => k,
        Err(e) => {
            if e.msg().contains("ErrorKind::NotFound") {
                let s = futures_util::stream::empty();
                return Ok(Box::pin(s));
            } else {
                return Err(e)?;
            }
        }
    };
    let entry_res = match extract_matching_config_entry(range, &channel_config) {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    let entry = match entry_res {
        MatchingConfigEntry::None => return Err(Error::with_msg("no config entry found"))?,
        MatchingConfigEntry::Multiple => return Err(Error::with_msg("multiple config entries found"))?,
        MatchingConfigEntry::Entry(entry) => entry,
    };
    let shape = match entry.to_shape() {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    let channel_config = netpod::ChannelConfig {
        channel: evq.channel.clone(),
        keyspace: entry.ks as u8,
        time_bin_size: entry.bs,
        shape: shape,
        scalar_type: entry.scalar_type.clone(),
        byte_order: entry.byte_order.clone(),
        array: entry.is_array,
        compression: entry.is_compressed,
    };
    let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
    let event_blobs = EventChunkerMultifile::new(
        range.clone(),
        channel_config.clone(),
        node_config.node.clone(),
        node_config.ix,
        evq.disk_io_buffer_size,
        event_chunker_conf,
        true,
    );
    let s = event_blobs.map(|item| Box::new(item) as Box<dyn Framable>);
    let pipe: Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>;
    pipe = Box::pin(s);
    Ok(pipe)
}
