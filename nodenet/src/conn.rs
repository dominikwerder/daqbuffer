use crate::scylla::scylla_channel_event_stream;
use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::on_sitemty_data;
use items_0::streamitem::sitem_data;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::streamitem::EVENT_QUERY_JSON_STRING_FRAME;
use items_0::Events;
use items_2::channelevents::ChannelEvents;
use items_2::empty::empty_events_dyn_ev;
use items_2::framable::EventQueryJsonStringFrame;
use items_2::framable::Framable;
use items_2::frame::decode_frame;
use items_2::frame::make_term_frame;
use items_2::inmem::InMemoryFrame;
use netpod::histo::HistoLog2;
use netpod::log::*;
use netpod::ChConf;
use netpod::NodeConfigCached;
use netpod::PerfOpts;
use query::api4::events::PlainEventsQuery;
use std::net::SocketAddr;
use std::pin::Pin;
use streams::frames::inmem::InMemoryFrameAsyncReadStream;
use streams::generators::GenerateF64V00;
use streams::generators::GenerateI32V00;
use streams::generators::GenerateI32V01;
use streams::transform::build_event_transform;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tracing::Instrument;

const TEST_BACKEND: &str = "testbackend-00";

#[cfg(test)]
mod test;

pub async fn events_service(node_config: NodeConfigCached) -> Result<(), Error> {
    let addr = format!("{}:{}", node_config.node.listen, node_config.node.port_raw);
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
    evq: PlainEventsQuery,
    chconf: ChConf,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    if evq.channel().backend() == TEST_BACKEND {
        debug!("use test backend data  {}", TEST_BACKEND);
        let node_count = node_config.node_config.cluster.nodes.len() as u64;
        let node_ix = node_config.ix as u64;
        let chn = evq.channel().name();
        let range = evq.range().clone();
        if chn == "test-gen-i32-dim0-v00" {
            Ok(Box::pin(GenerateI32V00::new(
                node_ix,
                node_count,
                range,
                evq.one_before_range(),
            )))
        } else if chn == "test-gen-i32-dim0-v01" {
            Ok(Box::pin(GenerateI32V01::new(
                node_ix,
                node_count,
                range,
                evq.one_before_range(),
            )))
        } else if chn == "test-gen-f64-dim1-v00" {
            Ok(Box::pin(GenerateF64V00::new(
                node_ix,
                node_count,
                range,
                evq.one_before_range(),
            )))
        } else {
            let na: Vec<_> = chn.split("-").collect();
            if na.len() != 3 {
                Err(Error::with_msg_no_trace(format!(
                    "make_channel_events_stream_data can not understand test channel name: {chn:?}"
                )))
            } else {
                if na[0] != "inmem" {
                    Err(Error::with_msg_no_trace(format!(
                        "make_channel_events_stream_data can not understand test channel name: {chn:?}"
                    )))
                } else {
                    let range = evq.range().clone();
                    if na[1] == "d0" {
                        if na[2] == "i32" {
                            //generator::generate_i32(node_ix, node_count, range)
                            panic!()
                        } else if na[2] == "f32" {
                            //generator::generate_f32(node_ix, node_count, range)
                            panic!()
                        } else {
                            Err(Error::with_msg_no_trace(format!(
                                "make_channel_events_stream_data can not understand test channel name: {chn:?}"
                            )))
                        }
                    } else {
                        Err(Error::with_msg_no_trace(format!(
                            "make_channel_events_stream_data can not understand test channel name: {chn:?}"
                        )))
                    }
                }
            }
        }
    } else if let Some(scyconf) = &node_config.node_config.cluster.scylla {
        scylla_channel_event_stream(evq, chconf, scyconf, node_config).await
    } else if let Some(_) = &node_config.node.channel_archiver {
        let e = Error::with_msg_no_trace("archapp not built");
        Err(e)
    } else if let Some(_) = &node_config.node.archiver_appliance {
        let e = Error::with_msg_no_trace("archapp not built");
        Err(e)
    } else {
        Ok(disk::raw::conn::make_event_pipe(&evq, chconf, node_config).await?)
    }
}

async fn make_channel_events_stream(
    evq: PlainEventsQuery,
    chconf: ChConf,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    let empty = empty_events_dyn_ev(&chconf.scalar_type, &chconf.shape)?;
    let empty = sitem_data(ChannelEvents::Events(empty));
    let stream = make_channel_events_stream_data(evq, chconf, node_config).await?;
    let ret = futures_util::stream::iter([empty]).chain(stream);
    let ret = Box::pin(ret);
    Ok(ret)
}

async fn events_get_input_frames(netin: OwnedReadHalf) -> Result<Vec<InMemoryFrame>, Error> {
    let perf_opts = PerfOpts::default();
    let mut h = InMemoryFrameAsyncReadStream::new(netin, perf_opts.inmem_bufcap);
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

async fn events_parse_input_query(
    frames: Vec<InMemoryFrame>,
    ncc: &NodeConfigCached,
) -> Result<(PlainEventsQuery, ChConf), Error> {
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
    let res: Result<PlainEventsQuery, _> = serde_json::from_str(&qitem.0);
    let evq = match res {
        Ok(k) => k,
        Err(e) => {
            let e = Error::with_msg_no_trace(format!("json parse error: {e}"));
            error!("{e}");
            return Err(e);
        }
    };
    debug!("events_parse_input_query  {:?}", evq);
    let chconf = crate::channelconfig::channel_config(evq.range().try_into()?, evq.channel().clone(), ncc)
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    Ok((evq, chconf))
}

async fn events_conn_handler_inner_try(
    stream: TcpStream,
    addr: SocketAddr,
    node_config: &NodeConfigCached,
) -> Result<(), ConnErr> {
    let _ = addr;
    let (netin, mut netout) = stream.into_split();
    let frames = match events_get_input_frames(netin).await {
        Ok(x) => x,
        Err(e) => return Err((e, netout).into()),
    };
    let (evq, chconf) = match events_parse_input_query(frames, node_config).await {
        Ok(x) => x,
        Err(e) => return Err((e, netout).into()),
    };
    let mut stream: Pin<Box<dyn Stream<Item = Box<dyn Framable + Send>> + Send>> = if evq.is_event_blobs() {
        // TODO support event blobs as transform
        match disk::raw::conn::make_event_blobs_pipe(&evq, node_config).await {
            Ok(stream) => {
                let stream = stream.map(|x| Box::new(x) as _);
                Box::pin(stream)
            }
            Err(e) => {
                return Err((e, netout).into());
            }
        }
    } else {
        match make_channel_events_stream(evq.clone(), chconf, node_config).await {
            Ok(stream) => {
                if false {
                    // TODO wasm example
                    use wasmer::Value;
                    let wasm = b"";
                    let mut store = wasmer::Store::default();
                    let module = wasmer::Module::new(&store, wasm).unwrap();
                    let import_object = wasmer::imports! {};
                    let instance = wasmer::Instance::new(&mut store, &module, &import_object).unwrap();
                    let add_one = instance.exports.get_function("event_transform").unwrap();
                    let result = add_one.call(&mut store, &[Value::I32(42)]).unwrap();
                    assert_eq!(result[0], Value::I32(43));
                }
                let mut tr = match build_event_transform(evq.transform()) {
                    Ok(x) => x,
                    Err(e) => {
                        return Err((e, netout).into());
                    }
                };
                let stream = stream.map(move |x| {
                    let item = on_sitemty_data!(x, |x| {
                        let x: Box<dyn Events> = Box::new(x);
                        let x = tr.0.transform(x);
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))
                    });
                    Box::new(item) as Box<dyn Framable + Send>
                });
                Box::pin(stream)
            }
            Err(e) => {
                return Err((e, netout).into());
            }
        }
    };

    let mut buf_len_histo = HistoLog2::new(5);
    while let Some(item) = stream.next().await {
        let item = item.make_frame();
        match item {
            Ok(buf) => {
                buf_len_histo.ingest(buf.len() as u32);
                match netout.write_all(&buf).await {
                    Ok(_) => {}
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
            node_ix: node_config.ix as _,
            level: Level::INFO,
            msg: format!("buf_len_histo: {:?}", buf_len_histo),
        };
        let item: Sitemty<ChannelEvents> = Ok(StreamItem::Log(item));
        let buf = match item.make_frame() {
            Ok(k) => k,
            Err(e) => return Err((e, netout))?,
        };
        match netout.write_all(&buf).await {
            Ok(_) => (),
            Err(e) => return Err((e, netout))?,
        }
    }
    let buf = match make_term_frame() {
        Ok(k) => k,
        Err(e) => return Err((e, netout))?,
    };
    match netout.write_all(&buf).await {
        Ok(_) => (),
        Err(e) => return Err((e, netout))?,
    }
    match netout.flush().await {
        Ok(_) => (),
        Err(e) => return Err((e, netout))?,
    }
    Ok(())
}

async fn events_conn_handler_inner(
    stream: TcpStream,
    addr: SocketAddr,
    node_config: &NodeConfigCached,
) -> Result<(), Error> {
    match events_conn_handler_inner_try(stream, addr, node_config).await {
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
    let span1 = span!(Level::INFO, "events_conn_handler");
    let r = events_conn_handler_inner(stream, addr, &node_config)
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
