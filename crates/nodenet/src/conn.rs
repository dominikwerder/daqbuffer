use crate::scylla::scylla_channel_event_stream;
use bytes::Bytes;
use err::thiserror;
use err::Error;
use err::ThisError;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::on_sitemty_data;
use items_0::streamitem::sitem_data;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::streamitem::EVENT_QUERY_JSON_STRING_FRAME;
use items_2::channelevents::ChannelEvents;
use items_2::empty::empty_events_dyn_ev;
use items_2::framable::EventQueryJsonStringFrame;
use items_2::framable::Framable;
use items_2::frame::decode_frame;
use items_2::frame::make_term_frame;
use items_2::inmem::InMemoryFrame;
use netpod::histo::HistoLog2;
use netpod::log::*;
use netpod::NodeConfigCached;
use netpod::ReqCtxArc;
use query::api4::events::EventsSubQuery;
use query::api4::events::Frame1Parts;
use std::net::SocketAddr;
use std::pin::Pin;
use streams::frames::inmem::BoxedBytesStream;
use streams::frames::inmem::InMemoryFrameStream;
use streams::frames::inmem::TcpReadAsBytes;
use streams::tcprawclient::TEST_BACKEND;
use streams::transform::build_event_transform;
use taskrun::tokio;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tracing::Instrument;

#[cfg(test)]
mod test;

#[derive(Debug, ThisError)]
pub enum NodeNetError {}

pub async fn events_service(node_config: NodeConfigCached) -> Result<(), Error> {
    let addr = format!("{}:{}", node_config.node.listen(), node_config.node.port_raw);
    let lis = tokio::net::TcpListener::bind(addr).await?;
    loop {
        match lis.accept().await {
            Ok((stream, addr)) => {
                taskrun::spawn(events_conn_handler(stream, addr, node_config.clone()));
            }
            Err(e) => Err(e)?,
        }
    }
}

struct ConnErr {
    err: Error,
    #[allow(dead_code)]
    netout: OwnedWriteHalf,
}

impl<E: Into<Error>> From<(E, OwnedWriteHalf)> for ConnErr {
    fn from((err, netout): (E, OwnedWriteHalf)) -> Self {
        Self {
            err: err.into(),
            netout,
        }
    }
}

async fn make_channel_events_stream_data(
    subq: EventsSubQuery,
    reqctx: ReqCtxArc,
    ncc: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    if subq.backend() == TEST_BACKEND {
        let node_count = ncc.node_config.cluster.nodes.len() as u64;
        let node_ix = ncc.ix as u64;
        streams::generators::make_test_channel_events_stream_data(subq, node_count, node_ix)
    } else if let Some(scyconf) = &ncc.node_config.cluster.scylla {
        let cfg = subq.ch_conf().to_scylla()?;
        scylla_channel_event_stream(subq, cfg, scyconf, ncc).await
    } else if let Some(_) = &ncc.node.channel_archiver {
        let e = Error::with_msg_no_trace("archapp not built");
        Err(e)
    } else if let Some(_) = &ncc.node.archiver_appliance {
        let e = Error::with_msg_no_trace("archapp not built");
        Err(e)
    } else {
        let cfg = subq.ch_conf().to_sf_databuffer()?;
        Ok(disk::raw::conn::make_event_pipe(subq, cfg, reqctx, ncc).await?)
    }
}

async fn make_channel_events_stream(
    subq: EventsSubQuery,
    reqctx: ReqCtxArc,
    ncc: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    let empty = empty_events_dyn_ev(subq.ch_conf().scalar_type(), subq.ch_conf().shape())?;
    let empty = sitem_data(ChannelEvents::Events(empty));
    let stream = make_channel_events_stream_data(subq, reqctx, ncc).await?;
    let ret = futures_util::stream::iter([empty]).chain(stream);
    let ret = Box::pin(ret);
    Ok(ret)
}

pub async fn create_response_bytes_stream(
    evq: EventsSubQuery,
    ncc: &NodeConfigCached,
) -> Result<BoxedBytesStream, Error> {
    debug!(
        "create_response_bytes_stream  {:?}  {:?}",
        evq.ch_conf().scalar_type(),
        evq.ch_conf().shape(),
    );
    debug!("wasm1 {:?}", evq.wasm1());
    let reqctx = netpod::ReqCtx::new_from_single_reqid(evq.reqid().into()).into();
    if evq.create_errors_contains("nodenet_parse_query") {
        let e = Error::with_msg_no_trace("produced error on request nodenet_parse_query");
        return Err(e);
    }
    if evq.is_event_blobs() {
        // TODO support event blobs as transform
        let fetch_info = evq.ch_conf().to_sf_databuffer()?;
        let stream = disk::raw::conn::make_event_blobs_pipe(&evq, &fetch_info, reqctx, ncc)?;
        // let stream = stream.map(|x| Box::new(x) as _);
        let stream = stream.map(|x| x.make_frame().map(|x| x.freeze()));
        let ret = Box::pin(stream);
        Ok(ret)
    } else {
        let mut tr = build_event_transform(evq.transform())?;
        let stream = make_channel_events_stream(evq, reqctx, ncc).await?;
        let stream = stream.map(move |x| {
            on_sitemty_data!(x, |x: ChannelEvents| {
                match x {
                    ChannelEvents::Events(evs) => {
                        let evs = tr.0.transform(evs);
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(ChannelEvents::Events(
                            evs,
                        ))))
                    }
                    ChannelEvents::Status(x) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                        ChannelEvents::Status(x),
                    ))),
                }
            })
        });
        // let stream = stream.map(move |x| Box::new(x) as Box<dyn Framable + Send>);
        let stream = stream.map(|x| x.make_frame().map(|x| x.freeze()));
        let ret = Box::pin(stream);
        Ok(ret)
    }
}

async fn events_conn_handler_with_reqid(
    mut netout: OwnedWriteHalf,
    evq: EventsSubQuery,
    ncc: &NodeConfigCached,
) -> Result<(), ConnErr> {
    let mut stream = match create_response_bytes_stream(evq, ncc).await {
        Ok(x) => x,
        Err(e) => return Err((e, netout))?,
    };
    let mut buf_len_histo = HistoLog2::new(5);
    while let Some(item) = stream.next().await {
        match item {
            Ok(buf) => {
                buf_len_histo.ingest(buf.len() as u32);
                match netout.write_all(&buf).await {
                    Ok(()) => {
                        // TODO collect timing information and send as summary in a stats item.
                        // TODO especially collect a distribution over the buf lengths that were send.
                        // TODO we want to see a reasonable batch size.
                    }
                    Err(e) => return Err((e, netout))?,
                }
            }
            Err(e) => {
                error!("events_conn_handler_inner_try sees error in stream: {e:?}");
                return Err((e, netout))?;
            }
        }
    }
    {
        let item = LogItem {
            node_ix: ncc.ix as _,
            level: Level::DEBUG,
            msg: format!("buf_len_histo: {:?}", buf_len_histo),
        };
        let item: Sitemty<ChannelEvents> = Ok(StreamItem::Log(item));
        let buf = match item.make_frame() {
            Ok(k) => k,
            Err(e) => return Err((e, netout))?,
        };
        match netout.write_all(&buf).await {
            Ok(()) => (),
            Err(e) => return Err((e, netout))?,
        }
    }
    let buf = match make_term_frame() {
        Ok(k) => k,
        Err(e) => return Err((e, netout))?,
    };
    match netout.write_all(&buf).await {
        Ok(()) => (),
        Err(e) => return Err((e, netout))?,
    }
    match netout.flush().await {
        Ok(()) => (),
        Err(e) => return Err((e, netout))?,
    }
    Ok(())
}

pub async fn events_get_input_frames<INP>(netin: INP) -> Result<Vec<InMemoryFrame>, Error>
where
    INP: Stream<Item = Result<Bytes, Error>> + Unpin,
{
    let mut h = InMemoryFrameStream::new(netin, netpod::ByteSize::from_kb(8));
    let mut frames = Vec::new();
    while let Some(k) = h
        .next()
        .instrument(span!(Level::INFO, "events_conn_handler/query-input"))
        .await
    {
        match k {
            Ok(StreamItem::DataItem(item)) => {
                frames.push(item);
            }
            Ok(item) => {
                debug!("ignored incoming frame {:?}", item);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(frames)
}

pub fn events_parse_input_query(frames: Vec<InMemoryFrame>) -> Result<(EventsSubQuery,), Error> {
    if frames.len() != 1 {
        error!("{:?}", frames);
        error!("missing command frame  len {}", frames.len());
        let e = Error::with_msg("missing command frame");
        return Err(e);
    }
    let query_frame = &frames[0];
    if query_frame.tyid() != EVENT_QUERY_JSON_STRING_FRAME {
        return Err(Error::with_msg("query frame wrong type"));
    }
    // TODO this does not need all variants of Sitemty.
    let qitem = match decode_frame::<Sitemty<EventQueryJsonStringFrame>>(query_frame) {
        Ok(k) => match k {
            Ok(k) => match k {
                StreamItem::DataItem(k) => match k {
                    RangeCompletableItem::Data(k) => k,
                    RangeCompletableItem::RangeComplete => return Err(Error::with_msg("bad query item")),
                },
                _ => return Err(Error::with_msg("bad query item")),
            },
            Err(e) => return Err(e),
        },
        Err(e) => return Err(e),
    };
    let frame1: Frame1Parts = serde_json::from_str(&qitem.str()).map_err(|e| {
        let e = Error::with_msg_no_trace(format!("json parse error: {}  inp {}", e, qitem.str()));
        error!("{e}");
        error!("input was {}", qitem.str());
        e
    })?;
    Ok(frame1.parts())
}

async fn events_conn_handler_inner_try<INP>(
    netin: INP,
    netout: OwnedWriteHalf,
    addr: SocketAddr,
    ncc: &NodeConfigCached,
) -> Result<(), ConnErr>
where
    INP: Stream<Item = Result<Bytes, Error>> + Unpin,
{
    let _ = addr;
    let frames = match events_get_input_frames(netin).await {
        Ok(x) => x,
        Err(e) => return Err((e, netout).into()),
    };
    let (evq,) = match events_parse_input_query(frames) {
        Ok(x) => x,
        Err(e) => return Err((e, netout).into()),
    };
    debug!("events_conn_handler sees:  {evq:?}");
    let reqid = evq.reqid();
    let span = tracing::info_span!("subreq", reqid = reqid);
    events_conn_handler_with_reqid(netout, evq, ncc).instrument(span).await
}

async fn events_conn_handler_inner<INP>(
    netin: INP,
    netout: OwnedWriteHalf,
    addr: SocketAddr,
    node_config: &NodeConfigCached,
) -> Result<(), Error>
where
    INP: Stream<Item = Result<Bytes, Error>> + Unpin,
{
    match events_conn_handler_inner_try(netin, netout, addr, node_config).await {
        Ok(_) => (),
        Err(ce) => {
            let mut out = ce.netout;
            let item: Sitemty<ChannelEvents> = Err(ce.err);
            let buf = Framable::make_frame(&item)?;
            out.write_all(&buf).await?;
        }
    }
    Ok(())
}

async fn events_conn_handler(stream: TcpStream, addr: SocketAddr, node_config: NodeConfigCached) -> Result<(), Error> {
    let (netin, netout) = stream.into_split();
    let inp = Box::new(TcpReadAsBytes::new(netin));
    let span1 = span!(Level::INFO, "events_conn_handler");
    let r = events_conn_handler_inner(inp, netout, addr, &node_config)
        .instrument(span1)
        .await;
    match r {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("events_conn_handler sees error: {:?}", e);
            Err(e)
        }
    }
}
