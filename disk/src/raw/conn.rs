use crate::channelconfig::config_entry_best_match;
use crate::eventblobs::EventChunkerMultifile;
use crate::eventchunker::EventChunkerConf;
use crate::raw::generated::EventBlobsGeneratorI32Test00;
use crate::raw::generated::EventBlobsGeneratorI32Test01;
use crate::SfDbChConf;
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
use netpod::ChConf;
use netpod::DiskIoTune;
use netpod::NodeConfigCached;
use netpod::SfDbChannel;
use parse::channelconfig::ConfigEntry;
use query::api4::events::PlainEventsQuery;
use std::pin::Pin;

const TEST_BACKEND: &str = "testbackend-00";

fn make_num_pipeline_stream_evs(
    chconf: ChConf,
    agg_kind: AggKind,
    event_blobs: EventChunkerMultifile,
) -> Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>> {
    let scalar_type = chconf.scalar_type.clone();
    let shape = chconf.shape.clone();
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
    chconf: ChConf,
    ncc: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    // sf-databuffer type backends identify channels by their (backend, name) only.
    let channel = evq.channel().clone();
    let range = evq.range().clone();
    let x = crate::channelconfig::channel_config_best_match(evq.range().try_into()?, channel, ncc).await;
    let channel_config = match x {
        Ok(Some(x)) => x,
        Ok(None) => {
            error!("make_event_pipe  can not find config");
            return Err(Error::with_msg_no_trace("make_event_pipe  can not find config"));
        }
        Err(e) => {
            error!("make_event_pipe  can not find config");
            if e.msg().contains("ErrorKind::NotFound") {
                warn!("{e}");
                let s = futures_util::stream::empty();
                return Ok(Box::pin(s));
            } else {
                return Err(e);
            }
        }
    };
    info!(
        "make_event_pipe  need_expand {need_expand}  {evq:?}",
        need_expand = evq.one_before_range()
    );
    let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
    // TODO should not need this for correctness.
    // Should limit based on return size and latency.
    let out_max_len = if ncc.node_config.cluster.is_central_storage {
        128
    } else {
        128
    };
    let event_blobs = EventChunkerMultifile::new(
        (&range).try_into()?,
        channel_config.clone(),
        ncc.node.clone(),
        ncc.ix,
        DiskIoTune::default(),
        event_chunker_conf,
        evq.one_before_range(),
        true,
        out_max_len,
    );
    error!("TODO replace AggKind in the called code");
    let pipe = make_num_pipeline_stream_evs(chconf, AggKind::TimeWeightedScalar, event_blobs);
    Ok(pipe)
}

pub fn make_local_event_blobs_stream(
    range: NanoRange,
    channel: SfDbChannel,
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
    let channel_config = SfDbChConf {
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
    channel: SfDbChannel,
    entry: &ConfigEntry,
    expand: bool,
    do_decompress: bool,
    event_chunker_conf: EventChunkerConf,
    disk_io_tune: DiskIoTune,
    node_config: &NodeConfigCached,
) -> Result<impl Stream<Item = Sitemty<EventFull>>, Error> {
    debug!("make_remote_event_blobs_stream");
    let shape = match entry.to_shape() {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    let channel_config = SfDbChConf {
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

pub async fn make_event_blobs_pipe_real(
    evq: &PlainEventsQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    if false {
        match dbconn::channel_exists(evq.channel(), &node_config).await {
            Ok(_) => (),
            Err(e) => return Err(e)?,
        }
    }
    let expand = evq.one_before_range();
    let range = evq.range();
    let entry = match config_entry_best_match(&evq.range().try_into()?, evq.channel().clone(), node_config).await {
        Ok(Some(x)) => x,
        Ok(None) => {
            let e = Error::with_msg_no_trace("no config entry found");
            error!("{e}");
            return Err(e);
        }
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
        /*
        type ItemType = Sitemty<EventFull>;
        let s = event_blobs.map(|item: ItemType| Box::new(item) as Box<dyn Framable + Send>);
        //let s = tracing_futures::Instrumented::instrument(s, tracing::info_span!("make_event_blobs_pipe"));
        let pipe: Pin<Box<dyn Stream<Item = Box<dyn Framable + Send>> + Send>>;
        pipe = Box::pin(s);
        pipe*/
        Box::pin(event_blobs) as _
    };
    Ok(pipe)
}

pub async fn make_event_blobs_pipe_test(
    evq: &PlainEventsQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    warn!("GENERATE INMEM TEST DATA");
    let node_count = node_config.node_config.cluster.nodes.len() as u64;
    let node_ix = node_config.ix as u64;
    let chn = evq.channel().name();
    let range = evq.range().clone();
    if chn == "test-gen-i32-dim0-v00" {
        Ok(Box::pin(EventBlobsGeneratorI32Test00::new(node_ix, node_count, range)))
    } else if chn == "test-gen-i32-dim0-v01" {
        Ok(Box::pin(EventBlobsGeneratorI32Test01::new(node_ix, node_count, range)))
    } else {
        let na: Vec<_> = chn.split("-").collect();
        if na.len() != 3 {
            Err(Error::with_msg_no_trace(format!(
                "can not understand test channel name: {chn:?}"
            )))
        } else {
            if na[0] != "inmem" {
                Err(Error::with_msg_no_trace(format!(
                    "can not understand test channel name: {chn:?}"
                )))
            } else {
                if na[1] == "d0" {
                    if na[2] == "i32" {
                        Ok(Box::pin(EventBlobsGeneratorI32Test00::new(node_ix, node_count, range)))
                    } else {
                        Err(Error::with_msg_no_trace(format!(
                            "can not understand test channel name: {chn:?}"
                        )))
                    }
                } else {
                    Err(Error::with_msg_no_trace(format!(
                        "can not understand test channel name: {chn:?}"
                    )))
                }
            }
        }
    }
}

pub async fn make_event_blobs_pipe(
    evq: &PlainEventsQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    debug!("make_event_blobs_pipe {evq:?}");
    if evq.channel().backend() == TEST_BACKEND {
        make_event_blobs_pipe_test(evq, node_config).await
    } else {
        make_event_blobs_pipe_real(evq, node_config).await
    }
}
