use crate::decode::{BigEndian, Endianness, LittleEndian};
use crate::decode::{EventValueFromBytes, EventValueShape, EventsDecodedStream, NumFromBytes};
use crate::decode::{EventValuesDim0Case, EventValuesDim1Case};
use crate::eventblobs::EventChunkerMultifile;
use err::Error;
use futures_util::{Stream, StreamExt};
use items::eventfull::EventFull;
use items::numops::{BoolNum, NumOps, StringNum};
use items::{EventsNodeProcessor, Framable, RangeCompletableItem, Sitemty, StreamItem};
use items_2::channelevents::ChannelEvents;
use items_2::eventsdim0::EventsDim0;
use netpod::log::*;
use netpod::query::RawEventsQuery;
use netpod::{AggKind, ByteOrder, ByteSize, Channel, DiskIoTune, NanoRange, NodeConfigCached, ScalarType, Shape};
use parse::channelconfig::{extract_matching_config_entry, read_local_config, ConfigEntry, MatchingConfigEntry};
use std::collections::VecDeque;
use std::pin::Pin;
use streams::eventchunker::EventChunkerConf;

fn make_num_pipeline_stream_evs<NTY, END, EVS, ENP>(
    event_value_shape: EVS,
    events_node_proc: ENP,
    event_blobs: EventChunkerMultifile,
) -> Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>
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
                    // TODO fix super ugly slow glue code
                    use items::EventsNodeProcessorOutput;
                    let mut item = events_node_proc.process(item);
                    if let Some(item) = item
                        .as_any_mut()
                        .downcast_mut::<items::scalarevents::ScalarEvents<NTY>>()
                    {
                        trace!("ScalarEvents");
                        let tss: VecDeque<u64> = item.tss.iter().map(|x| *x).collect();
                        let pulses: VecDeque<u64> = item.pulses.iter().map(|x| *x).collect();
                        let values: VecDeque<NTY> = item.values.iter().map(|x| x.clone()).collect();
                        let item = EventsDim0 { tss, pulses, values };
                        let item = ChannelEvents::Events(Box::new(item));
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                    } else if let Some(item) = item.as_any_mut().downcast_mut::<items::waveevents::WaveEvents<NTY>>() {
                        trace!("WaveEvents");
                        let _tss: VecDeque<u64> = item.tss.iter().map(|x| *x).collect();
                        let _pulses: VecDeque<u64> = item.pulses.iter().map(|x| *x).collect();
                        let _values: VecDeque<Vec<NTY>> = item.vals.iter().map(|x| x.clone()).collect();
                        //let item = EventsDim1 { tss, pulses, values };
                        //let item = ChannelEvents::Events(Box::new(item));
                        //Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                    } else if let Some(item) = item
                        .as_any_mut()
                        .downcast_mut::<items::xbinnedscalarevents::XBinnedScalarEvents<NTY>>()
                    {
                        trace!("XBinnedScalarEvents");
                        let tss: VecDeque<u64> = item.tss.iter().map(|x| *x).collect();
                        let pulses: VecDeque<u64> = (0..tss.len()).map(|_| 0).collect();
                        let _avgs: VecDeque<f32> = item.avgs.iter().map(|x| x.clone()).collect();
                        let mins: VecDeque<NTY> = item.mins.iter().map(|x| x.clone()).collect();
                        let _maxs: VecDeque<NTY> = item.maxs.iter().map(|x| x.clone()).collect();
                        let item = EventsDim0 {
                            tss,
                            pulses,
                            values: mins,
                        };
                        let item = ChannelEvents::Events(Box::new(item));
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                    } else {
                        error!("TODO bad, no idea what this item is\n\n{:?}\n\n", item);
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                    }
                }
                RangeCompletableItem::RangeComplete => Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)),
            },
            StreamItem::Log(item) => Ok(StreamItem::Log(item)),
            StreamItem::Stats(item) => Ok(StreamItem::Stats(item)),
        },
        Err(e) => Err(e),
    });
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
            AggKind::Stats1 => make_num_pipeline_stream_evs::<$nty, $end, $evs<$nty>, _>(
                $evsv,
                <$evs<$nty> as EventValueShape<$nty, $end>>::NumXAggToStats1::create($shape, $agg_kind),
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
            Shape::Image(_, _) => {
                // TODO not needed for python data api v3 protocol, but later for api4.
                err::todoval()
            }
        }
    };
}

macro_rules! pipe2 {
    ($nty:ident, $end:expr, $shape:expr, $agg_kind:expr, $event_blobs:expr) => {
        match $end {
            ByteOrder::Little => pipe3!($nty, LittleEndian, $shape, $agg_kind, $event_blobs),
            ByteOrder::Big => pipe3!($nty, BigEndian, $shape, $agg_kind, $event_blobs),
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
            ScalarType::STRING => pipe2!(StringNum, $end, $shape, $agg_kind, $event_blobs),
        }
    };
}

pub async fn make_event_pipe(
    evq: &RawEventsQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    if false {
        match dbconn::channel_exists(&evq.channel, &node_config).await {
            Ok(_) => (),
            Err(e) => return Err(e)?,
        }
    }
    let range = &evq.range;
    let channel_config = match read_local_config(evq.channel.clone(), node_config.node.clone()).await {
        Ok(k) => k,
        Err(e) => {
            // TODO introduce detailed error type
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
        MatchingConfigEntry::None => return Err(Error::with_public_msg("no config entry found"))?,
        MatchingConfigEntry::Multiple => return Err(Error::with_public_msg("multiple config entries found"))?,
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
    trace!(
        "make_event_pipe  need_expand {need_expand}  {evq:?}",
        need_expand = evq.agg_kind.need_expand()
    );
    let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
    let event_blobs = EventChunkerMultifile::new(
        range.clone(),
        channel_config.clone(),
        node_config.node.clone(),
        node_config.ix,
        evq.disk_io_tune.clone(),
        event_chunker_conf,
        evq.agg_kind.need_expand(),
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

pub async fn get_applicable_entry(
    range: &NanoRange,
    channel: Channel,
    node_config: &NodeConfigCached,
) -> Result<ConfigEntry, Error> {
    let channel_config = read_local_config(channel, node_config.node.clone()).await?;
    let entry_res = match extract_matching_config_entry(range, &channel_config) {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    let entry = match entry_res {
        MatchingConfigEntry::None => return Err(Error::with_public_msg("no config entry found"))?,
        MatchingConfigEntry::Multiple => return Err(Error::with_public_msg("multiple config entries found"))?,
        MatchingConfigEntry::Entry(entry) => entry,
    };
    Ok(entry.clone())
}

pub fn make_local_event_blobs_stream(
    range: NanoRange,
    channel: Channel,
    entry: &ConfigEntry,
    expand: bool,
    do_decompress: bool,
    event_chunker_conf: EventChunkerConf,
    disk_io_tune: DiskIoTune,
    node_config: &NodeConfigCached,
) -> Result<EventChunkerMultifile, Error> {
    info!("make_local_event_blobs_stream  do_decompress {do_decompress}  disk_io_tune {disk_io_tune:?}");
    if do_decompress {
        warn!("Possible issue: decompress central storage event blob stream");
    }
    let shape = match entry.to_shape() {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    let channel_config = netpod::ChannelConfig {
        channel,
        keyspace: entry.ks as u8,
        time_bin_size: entry.bs,
        shape: shape,
        scalar_type: entry.scalar_type.clone(),
        byte_order: entry.byte_order.clone(),
        array: entry.is_array,
        compression: entry.is_compressed,
    };
    let event_blobs = EventChunkerMultifile::new(
        range,
        channel_config.clone(),
        node_config.node.clone(),
        node_config.ix,
        disk_io_tune,
        event_chunker_conf,
        expand,
        do_decompress,
    );
    Ok(event_blobs)
}

pub fn make_remote_event_blobs_stream(
    range: NanoRange,
    channel: Channel,
    entry: &ConfigEntry,
    expand: bool,
    do_decompress: bool,
    event_chunker_conf: EventChunkerConf,
    disk_io_tune: DiskIoTune,
    node_config: &NodeConfigCached,
) -> Result<impl Stream<Item = Sitemty<EventFull>>, Error> {
    let shape = match entry.to_shape() {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    let channel_config = netpod::ChannelConfig {
        channel,
        keyspace: entry.ks as u8,
        time_bin_size: entry.bs,
        shape: shape,
        scalar_type: entry.scalar_type.clone(),
        byte_order: entry.byte_order.clone(),
        array: entry.is_array,
        compression: entry.is_compressed,
    };
    let event_blobs = EventChunkerMultifile::new(
        range,
        channel_config.clone(),
        node_config.node.clone(),
        node_config.ix,
        disk_io_tune,
        event_chunker_conf,
        expand,
        do_decompress,
    );
    Ok(event_blobs)
}

pub async fn make_event_blobs_pipe(
    evq: &RawEventsQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable + Send>> + Send>>, Error> {
    if false {
        match dbconn::channel_exists(&evq.channel, &node_config).await {
            Ok(_) => (),
            Err(e) => return Err(e)?,
        }
    }
    let expand = evq.agg_kind.need_expand();
    let range = &evq.range;
    let entry = get_applicable_entry(&evq.range, evq.channel.clone(), node_config).await?;
    let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
    type ItemType = Sitemty<EventFull>;
    // TODO should depend on host config
    let pipe = if true {
        let event_blobs = make_remote_event_blobs_stream(
            range.clone(),
            evq.channel.clone(),
            &entry,
            expand,
            evq.do_decompress,
            event_chunker_conf,
            evq.disk_io_tune.clone(),
            node_config,
        )?;
        let s = event_blobs.map(|item: ItemType| Box::new(item) as Box<dyn Framable + Send>);
        //let s = tracing_futures::Instrumented::instrument(s, tracing::info_span!("make_event_blobs_pipe"));
        let pipe: Pin<Box<dyn Stream<Item = Box<dyn Framable + Send>> + Send>>;
        pipe = Box::pin(s);
        pipe
    } else {
        let event_blobs = make_local_event_blobs_stream(
            range.clone(),
            evq.channel.clone(),
            &entry,
            expand,
            evq.do_decompress,
            event_chunker_conf,
            evq.disk_io_tune.clone(),
            node_config,
        )?;
        let s = event_blobs.map(|item: ItemType| Box::new(item) as Box<dyn Framable + Send>);
        //let s = tracing_futures::Instrumented::instrument(s, tracing::info_span!("make_event_blobs_pipe"));
        let pipe: Pin<Box<dyn Stream<Item = Box<dyn Framable + Send>> + Send>>;
        pipe = Box::pin(s);
        pipe
    };
    Ok(pipe)
}
