use crate::eventblobs::EventChunkerMultifile;
use err::Error;
use futures_util::stream;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::channelevents::ChannelEvents;
use items_2::eventfull::EventFull;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::AggKind;
use netpod::ByteSize;
use netpod::Channel;
use netpod::DiskIoTune;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::Shape;
use parse::channelconfig::extract_matching_config_entry;
use parse::channelconfig::read_local_config;
use parse::channelconfig::ConfigEntry;
use parse::channelconfig::MatchingConfigEntry;
use query::api4::events::PlainEventsQuery;
use std::pin::Pin;
use streams::eventchunker::EventChunkerConf;

fn make_num_pipeline_stream_evs(
    scalar_type: ScalarType,
    shape: Shape,
    agg_kind: AggKind,
    event_blobs: EventChunkerMultifile,
) -> Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>> {
    let event_stream = match crate::decode::EventsDynStream::new(scalar_type, shape, agg_kind, event_blobs) {
        Ok(k) => k,
        Err(e) => {
            return Box::pin(stream::iter([Err(e)]));
        }
    };
    let stream = event_stream.map(|item| match item {
        Ok(item) => match item {
            StreamItem::DataItem(item) => match item {
                RangeCompletableItem::RangeComplete => Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)),
                RangeCompletableItem::Data(item) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                    ChannelEvents::Events(item),
                ))),
            },
            StreamItem::Log(k) => Ok(StreamItem::Log(k)),
            StreamItem::Stats(k) => Ok(StreamItem::Stats(k)),
        },
        Err(e) => Err(e),
    });
    Box::pin(stream)
}

pub async fn make_event_pipe(
    evq: &PlainEventsQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    info!("----------  disk::raw::conn::make_event_pipe");
    if false {
        match dbconn::channel_exists(&evq.channel(), &node_config).await {
            Ok(_) => (),
            Err(e) => return Err(e)?,
        }
    }
    let range = evq.range().clone();
    let channel_config =
        crate::channelconfig::config(evq.range().try_into()?, evq.channel().clone(), node_config).await;
    let channel_config = match channel_config {
        Ok(x) => x,
        Err(e) => {
            error!("make_event_pipe  can not find config");
            if e.msg().contains("ErrorKind::NotFound") {
                warn!("{e}");
                let s = futures_util::stream::empty();
                return Ok(Box::pin(s));
            } else {
                return Err(e)?;
            }
        }
    };
    trace!(
        "make_event_pipe  need_expand {need_expand}  {evq:?}",
        need_expand = evq.one_before_range()
    );
    let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
    // TODO should not need this for correctness.
    // Should limit based on return size and latency.
    let out_max_len = if node_config.node_config.cluster.is_central_storage {
        128
    } else {
        128
    };
    let event_blobs = EventChunkerMultifile::new(
        (&range).try_into()?,
        channel_config.clone(),
        node_config.node.clone(),
        node_config.ix,
        DiskIoTune::default(),
        event_chunker_conf,
        evq.one_before_range(),
        true,
        out_max_len,
    );
    let scalar_type = channel_config.scalar_type.clone();
    let shape = channel_config.shape.clone();
    error!("TODO replace AggKind in the called code");
    let pipe = make_num_pipeline_stream_evs(scalar_type, shape.clone(), AggKind::TimeWeightedScalar, event_blobs);
    Ok(pipe)
}

pub async fn get_applicable_entry(
    range: &NanoRange,
    channel: Channel,
    node_config: &NodeConfigCached,
) -> Result<ConfigEntry, Error> {
    let channel_config = read_local_config(channel.clone(), node_config.node.clone()).await?;
    let entry_res = match extract_matching_config_entry(range, &channel_config) {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    let entry = match entry_res {
        MatchingConfigEntry::None => {
            return Err(Error::with_public_msg(format!(
                "get_applicable_entry no config entry found {:?}",
                channel
            )))?
        }
        MatchingConfigEntry::Multiple => {
            return Err(Error::with_public_msg(format!(
                "get_applicable_entry multiple config entries found for {:?}",
                channel
            )))?
        }
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
        time_bin_size: entry.bs.clone(),
        shape,
        scalar_type: entry.scalar_type.clone(),
        byte_order: entry.byte_order.clone(),
        array: entry.is_array,
        compression: entry.is_compressed,
    };
    // TODO should not need this for correctness.
    // Should limit based on return size and latency.
    let out_max_len = if node_config.node_config.cluster.is_central_storage {
        128
    } else {
        128
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
        out_max_len,
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
    info!("make_remote_event_blobs_stream");
    let shape = match entry.to_shape() {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    let channel_config = netpod::ChannelConfig {
        channel,
        keyspace: entry.ks as u8,
        time_bin_size: entry.bs.clone(),
        shape: shape,
        scalar_type: entry.scalar_type.clone(),
        byte_order: entry.byte_order.clone(),
        array: entry.is_array,
        compression: entry.is_compressed,
    };
    // TODO should not need this for correctness.
    // Should limit based on return size and latency.
    let out_max_len = if node_config.node_config.cluster.is_central_storage {
        128
    } else {
        128
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
        out_max_len,
    );
    Ok(event_blobs)
}

pub async fn make_event_blobs_pipe(
    evq: &PlainEventsQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    info!("make_event_blobs_pipe {evq:?}");
    if false {
        match dbconn::channel_exists(evq.channel(), &node_config).await {
            Ok(_) => (),
            Err(e) => return Err(e)?,
        }
    }
    let expand = evq.one_before_range();
    let range = evq.range();
    let entry = match get_applicable_entry(&evq.range().try_into()?, evq.channel().clone(), node_config).await {
        Ok(x) => x,
        Err(e) => {
            if e.to_public_error().msg().contains("no config entry found") {
                let item = items_0::streamitem::LogItem {
                    node_ix: node_config.ix as _,
                    level: Level::WARN,
                    msg: format!("{} {}", node_config.node.host, e),
                };
                return Ok(Box::pin(stream::iter([Ok(StreamItem::Log(item))])));
            } else {
                return Err(e);
            }
        }
    };
    let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
    type ItemType = Sitemty<EventFull>;
    // TODO should depend on host config
    let do_local = node_config.node_config.cluster.is_central_storage;
    let pipe = if do_local {
        let event_blobs = make_local_event_blobs_stream(
            range.try_into()?,
            evq.channel().clone(),
            &entry,
            expand,
            false,
            event_chunker_conf,
            DiskIoTune::default(),
            node_config,
        )?;
        Box::pin(event_blobs) as _
    } else {
        let event_blobs = make_remote_event_blobs_stream(
            range.try_into()?,
            evq.channel().clone(),
            &entry,
            expand,
            true,
            event_chunker_conf,
            DiskIoTune::default(),
            node_config,
        )?;
        /*let s = event_blobs.map(|item: ItemType| Box::new(item) as Box<dyn Framable + Send>);
        //let s = tracing_futures::Instrumented::instrument(s, tracing::info_span!("make_event_blobs_pipe"));
        let pipe: Pin<Box<dyn Stream<Item = Box<dyn Framable + Send>> + Send>>;
        pipe = Box::pin(s);
        pipe*/
        Box::pin(event_blobs) as _
    };
    Ok(pipe)
}
