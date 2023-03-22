use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::on_sitemty_data;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::streamitem::EVENT_QUERY_JSON_STRING_FRAME;
use items_0::Appendable;
use items_0::Empty;
use items_2::channelevents::ChannelEvents;
use items_2::framable::EventQueryJsonStringFrame;
use items_2::framable::Framable;
use items_2::frame::decode_frame;
use items_2::frame::make_term_frame;
use netpod::histo::HistoLog2;
use netpod::log::*;
use netpod::AggKind;
use netpod::NodeConfigCached;
use netpod::PerfOpts;
use query::api4::events::PlainEventsQuery;
use std::net::SocketAddr;
use std::pin::Pin;
use streams::frames::inmem::InMemoryFrameAsyncReadStream;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tracing::Instrument;

use crate::scylla::scylla_channel_event_stream;

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

async fn make_channel_events_stream(
    evq: PlainEventsQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    info!("nodenet::conn::make_channel_events_stream");
    if evq.channel().backend() == "test-inmem" {
        warn!("TEST BACKEND DATA");
        use netpod::timeunits::MS;
        let node_count = node_config.node_config.cluster.nodes.len();
        let node_ix = node_config.ix;
        if evq.channel().name() == "inmem-d0-i32" {
            let mut item = items_2::eventsdim0::EventsDim0::<i32>::empty();
            let td = MS * 10;
            for i in 0..20 {
                let ts = MS * 17 + td * node_ix as u64 + td * node_count as u64 * i;
                let pulse = 1 + node_ix as u64 + node_count as u64 * i;
                item.push(ts, pulse, pulse as _);
            }
            let item = ChannelEvents::Events(Box::new(item) as _);
            let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
            let stream = futures_util::stream::iter([item]);
            Ok(Box::pin(stream))
        } else if evq.channel().name() == "inmem-d0-f32" {
            let mut item = items_2::eventsdim0::EventsDim0::<f32>::empty();
            let td = MS * 10;
            for i in 0..20 {
                let ts = MS * 17 + td * node_ix as u64 + td * node_count as u64 * i;
                let pulse = 1 + node_ix as u64 + node_count as u64 * i;
                item.push(ts, pulse, ts as _);
            }
            let item = ChannelEvents::Events(Box::new(item) as _);
            let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
            let stream = futures_util::stream::iter([item]);
            Ok(Box::pin(stream))
        } else {
            let stream = futures_util::stream::empty();
            Ok(Box::pin(stream))
        }
    } else if let Some(scyconf) = &node_config.node_config.cluster.scylla {
        scylla_channel_event_stream(evq, scyconf, node_config).await
    } else if let Some(_) = &node_config.node.channel_archiver {
        let e = Error::with_msg_no_trace("archapp not built");
        Err(e)
    } else if let Some(_) = &node_config.node.archiver_appliance {
        let e = Error::with_msg_no_trace("archapp not built");
        Err(e)
    } else {
        Ok(disk::raw::conn::make_event_pipe(&evq, node_config).await?)
    }
}

async fn events_conn_handler_inner_try(
    stream: TcpStream,
    addr: SocketAddr,
    node_config: &NodeConfigCached,
) -> Result<(), ConnErr> {
    let _ = addr;
    let (netin, mut netout) = stream.into_split();
    warn!("fix magic inmem_bufcap option");
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
                return Err((e, netout).into());
            }
        }
    }
    debug!("events_conn_handler input frames received");
    if frames.len() != 1 {
        error!("{:?}", frames);
        error!("missing command frame  len {}", frames.len());
        return Err((Error::with_msg("missing command frame"), netout).into());
    }
    let query_frame = &frames[0];
    if query_frame.tyid() != EVENT_QUERY_JSON_STRING_FRAME {
        return Err((Error::with_msg("query frame wrong type"), netout).into());
    }
    // TODO this does not need all variants of Sitemty.
    let qitem = match decode_frame::<Sitemty<EventQueryJsonStringFrame>>(query_frame) {
        Ok(k) => match k {
            Ok(k) => match k {
                StreamItem::DataItem(k) => match k {
                    RangeCompletableItem::Data(k) => k,
                    RangeCompletableItem::RangeComplete => {
                        return Err((Error::with_msg("bad query item"), netout).into())
                    }
                },
                _ => return Err((Error::with_msg("bad query item"), netout).into()),
            },
            Err(e) => return Err((e, netout).into()),
        },
        Err(e) => return Err((e, netout).into()),
    };
    let res: Result<PlainEventsQuery, _> = serde_json::from_str(&qitem.0);
    let evq = match res {
        Ok(k) => k,
        Err(e) => {
            error!("json parse error: {:?}", e);
            return Err((Error::with_msg("json parse error"), netout).into());
        }
    };
    info!("events_conn_handler_inner_try  evq {:?}", evq);

    if evq.channel().name() == "test-do-trigger-main-error" {
        let e = Error::with_msg(format!("Test error private message."))
            .add_public_msg(format!("Test error PUBLIC message."));
        return Err((e, netout).into());
    }

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
        match make_channel_events_stream(evq.clone(), node_config).await {
            Ok(stream) => {
                let stream = stream
                    .map({
                        use items_0::transform::EventTransform;
                        let mut tf = items_0::transform::IdentityTransform::default();
                        move |item| {
                            if false {
                                on_sitemty_data!(item, |item4| {
                                    match item4 {
                                        ChannelEvents::Events(item5) => {
                                            let a = tf.transform(item5);
                                            let x = ChannelEvents::Events(a);
                                            Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))
                                        }
                                        x => Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))),
                                    }
                                })
                            } else {
                                item
                            }
                        }
                    })
                    .map(|x| Box::new(x) as _);
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
