use crate::eventchunker::EventChunkerConf;
use crate::eventchunkermultifile::EventChunkerMultifile;
use crate::raw::generated::EventBlobsGeneratorI32Test00;
use crate::raw::generated::EventBlobsGeneratorI32Test01;
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
use netpod::DiskIoTune;
use netpod::NodeConfigCached;
use netpod::ReqCtxArc;
use netpod::SfChFetchInfo;
use query::api4::events::EventsSubQuery;
use std::pin::Pin;

const TEST_BACKEND: &str = "testbackend-00";

fn make_num_pipeline_stream_evs(
    fetch_info: SfChFetchInfo,
    agg_kind: AggKind,
    event_blobs: EventChunkerMultifile,
) -> Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>> {
    let scalar_type = fetch_info.scalar_type().clone();
    let shape = fetch_info.shape().clone();
    let event_blobs = Box::pin(event_blobs);
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
    evq: EventsSubQuery,
    fetch_info: SfChFetchInfo,
    reqctx: ReqCtxArc,
    ncc: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    // sf-databuffer type backends identify channels by their (backend, name) only.
    let range = evq.range().clone();
    let one_before = evq.transform().need_one_before_range();
    info!(
        "make_event_pipe  need_expand {need_expand}  {evq:?}",
        need_expand = one_before
    );
    let event_chunker_conf = EventChunkerConf::new(ByteSize::from_kb(1024));
    // TODO should not need this for correctness.
    // Should limit based on return size and latency.
    let out_max_len = if ncc.node_config.cluster.is_central_storage {
        128
    } else {
        128
    };
    let event_blobs = EventChunkerMultifile::new(
        (&range).try_into()?,
        fetch_info.clone(),
        ncc.node.clone(),
        ncc.ix,
        evq.disk_io_tune(),
        event_chunker_conf,
        one_before,
        out_max_len,
        reqctx,
    );
    error!("TODO replace AggKind in the called code");
    let pipe = make_num_pipeline_stream_evs(fetch_info, AggKind::TimeWeightedScalar, event_blobs);
    Ok(pipe)
}

pub fn make_event_blobs_stream(
    range: NanoRange,
    fetch_info: SfChFetchInfo,
    expand: bool,
    event_chunker_conf: EventChunkerConf,
    disk_io_tune: DiskIoTune,
    reqctx: ReqCtxArc,
    node_config: &NodeConfigCached,
) -> Result<EventChunkerMultifile, Error> {
    debug!("make_local_event_blobs_stream  {fetch_info:?}  disk_io_tune {disk_io_tune:?}");
    // TODO should not need this for correctness.
    // Should limit based on return size and latency.
    let out_max_len = if node_config.node_config.cluster.is_central_storage {
        128
    } else {
        128
    };
    let event_blobs = EventChunkerMultifile::new(
        range,
        fetch_info.clone(),
        node_config.node.clone(),
        node_config.ix,
        disk_io_tune,
        event_chunker_conf,
        expand,
        out_max_len,
        reqctx,
    );
    Ok(event_blobs)
}

pub fn make_event_blobs_pipe_real(
    subq: &EventsSubQuery,
    fetch_info: &SfChFetchInfo,
    reqctx: ReqCtxArc,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    let expand = subq.transform().need_one_before_range();
    let range = subq.range();
    let event_chunker_conf = EventChunkerConf::new(ByteSize::from_kb(1024));
    let event_blobs = make_event_blobs_stream(
        range.try_into()?,
        fetch_info.clone(),
        expand,
        event_chunker_conf,
        subq.disk_io_tune(),
        reqctx,
        node_config,
    )?;
    let pipe = Box::pin(event_blobs) as _;
    Ok(pipe)
}

pub fn make_event_blobs_pipe_test(
    subq: &EventsSubQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    warn!("GENERATE INMEM TEST DATA");
    let node_count = node_config.node_config.cluster.nodes.len() as u64;
    let node_ix = node_config.ix as u64;
    let chn = subq.name();
    let range = subq.range().clone();
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

pub fn make_event_blobs_pipe(
    subq: &EventsSubQuery,
    fetch_info: &SfChFetchInfo,
    reqctx: ReqCtxArc,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    debug!("make_event_blobs_pipe {subq:?}");
    if subq.backend() == TEST_BACKEND {
        make_event_blobs_pipe_test(subq, node_config)
    } else {
        make_event_blobs_pipe_real(subq, fetch_info, reqctx, node_config)
    }
}
