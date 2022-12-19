use err::Error;
use futures_util::{Stream, StreamExt};
use items::frame::{decode_frame, make_term_frame};
use items::{EventQueryJsonStringFrame, Framable, RangeCompletableItem, Sitemty, StreamItem};
use items_0::Empty;
use items_2::channelevents::ChannelEvents;
use netpod::histo::HistoLog2;
use netpod::log::*;
use netpod::query::PlainEventsQuery;
use netpod::AggKind;
use netpod::{NodeConfigCached, PerfOpts};
use std::net::SocketAddr;
use std::pin::Pin;
use streams::frames::inmem::InMemoryFrameAsyncReadStream;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tracing::Instrument;

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

async fn events_conn_handler_inner_try(
    stream: TcpStream,
    addr: SocketAddr,
    node_config: &NodeConfigCached,
) -> Result<(), ConnErr> {
    let _ = addr;
    let (netin, mut netout) = stream.into_split();
    let perf_opts = PerfOpts { inmem_bufcap: 512 };
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
    if query_frame.tyid() != items::EVENT_QUERY_JSON_STRING_FRAME {
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

    let p1: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>> = if evq.channel().backend() == "test-inmem" {
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
            Box::pin(stream)
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
            Box::pin(stream)
        } else {
            let stream = futures_util::stream::empty();
            Box::pin(stream)
        }
    } else if let Some(conf) = &node_config.node_config.cluster.scylla {
        // TODO depends in general on the query
        // TODO why both in PlainEventsQuery and as separate parameter? Check other usages.
        let do_one_before_range = false;
        // TODO use better builder pattern with shortcuts for production and dev defaults
        let f = dbconn::channelconfig::chconf_from_database(evq.channel(), node_config)
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")));
        let f = match f {
            Ok(k) => k,
            Err(e) => return Err((e, netout))?,
        };
        let scyco = conf;
        let scy = match scyllaconn::create_scy_session(scyco).await {
            Ok(k) => k,
            Err(e) => return Err((e, netout))?,
        };
        let series = f.series;
        let scalar_type = f.scalar_type;
        let shape = f.shape;
        let do_test_stream_error = false;
        debug!("Make EventsStreamScylla for {series} {scalar_type:?} {shape:?}");
        let stream = scyllaconn::events::EventsStreamScylla::new(
            series,
            evq.range().clone(),
            do_one_before_range,
            scalar_type,
            shape,
            scy,
            do_test_stream_error,
        );
        let stream = stream
            .map(|item| match &item {
                Ok(k) => match k {
                    ChannelEvents::Events(k) => {
                        let n = k.len();
                        let d = evq.event_delay();
                        (item, n, d.clone())
                    }
                    ChannelEvents::Status(_) => (item, 1, None),
                },
                Err(_) => (item, 1, None),
            })
            .then(|(item, n, d)| async move {
                if let Some(d) = d {
                    debug!("sleep {} times {:?}", n, d);
                    tokio::time::sleep(d).await;
                }
                item
            })
            .map(|item| {
                let item = match item {
                    Ok(item) => match item {
                        ChannelEvents::Events(item) => {
                            let item = ChannelEvents::Events(item);
                            let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
                            item
                        }
                        ChannelEvents::Status(item) => {
                            let item = ChannelEvents::Status(item);
                            let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
                            item
                        }
                    },
                    Err(e) => Err(e),
                };
                item
            });
        Box::pin(stream)
    } else if let Some(_) = &node_config.node.channel_archiver {
        let e = Error::with_msg_no_trace("archapp not built");
        return Err((e, netout))?;
    } else if let Some(_) = &node_config.node.archiver_appliance {
        let e = Error::with_msg_no_trace("archapp not built");
        return Err((e, netout))?;
    } else {
        let stream = match evq.agg_kind() {
            AggKind::EventBlobs => match disk::raw::conn::make_event_blobs_pipe(&evq, node_config).await {
                Ok(_stream) => {
                    let e = Error::with_msg_no_trace("TODO make_event_blobs_pipe");
                    return Err((e, netout))?;
                }
                Err(e) => return Err((e, netout))?,
            },
            _ => match disk::raw::conn::make_event_pipe(&evq, node_config).await {
                Ok(j) => j,
                Err(e) => return Err((e, netout))?,
            },
        };
        stream
    };

    let p1 = p1.inspect(|x| {
        items::on_sitemty_range_complete!(x, warn!("GOOD  -----------   SEE RangeComplete in conn.rs"));
    });

    let mut p1 = p1;
    let mut buf_len_histo = HistoLog2::new(5);
    while let Some(item) = p1.next().await {
        let item = item.make_frame();
        match item {
            Ok(buf) => {
                info!("write {} bytes", buf.len());
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
    debug!("events_conn_handler_inner_try  buf_len_histo: {:?}", buf_len_histo);
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
            error!("events_conn_handler_inner sees error {:?}", ce.err);
            // Try to pass the error over the network.
            // If that fails, give error to the caller.
            let mut out = ce.netout;
            let e = ce.err;
            let buf = items::frame::make_error_frame(&e)?;
            //type T = StreamItem<items::RangeCompletableItem<items::scalarevents::ScalarEvents<u32>>>;
            //let buf = Err::<T, _>(e).make_frame()?;
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
