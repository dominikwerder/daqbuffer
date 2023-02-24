use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items::frame::decode_frame;
use items::frame::make_term_frame;
use items::EventQueryJsonStringFrame;
use items::Framable;
use items::RangeCompletableItem;
use items::Sitemty;
use items::StreamItem;
use items_0::Empty;
use items_2::channelevents::ChannelEvents;
use netpod::histo::HistoLog2;
use netpod::log::*;
use netpod::query::PlainEventsQuery;
use netpod::AggKind;
use netpod::NodeConfigCached;
use netpod::PerfOpts;
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

async fn make_channel_events_stream(
    evq: PlainEventsQuery,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
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
    } else if let Some(conf) = &node_config.node_config.cluster.scylla {
        // TODO depends in general on the query
        // TODO why both in PlainEventsQuery and as separate parameter? Check other usages.
        let do_one_before_range = false;
        // TODO use better builder pattern with shortcuts for production and dev defaults
        let f = crate::channelconfig::channel_config(evq.range().clone(), evq.channel().clone(), node_config)
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
        let scyco = conf;
        let scy = scyllaconn::create_scy_session(scyco).await?;
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
            .map({
                let agg_kind = evq.agg_kind_value();
                move |item| match item {
                    Ok(item) => {
                        let x = if let AggKind::PulseIdDiff = agg_kind {
                            let x = match item {
                                ChannelEvents::Events(item) => {
                                    let (tss, pulses) = items_0::EventsNonObj::into_tss_pulses(item);
                                    let mut item = items_2::eventsdim0::EventsDim0::empty();
                                    let mut pulse_last = pulses.front().map_or(0, |&x| x);
                                    for (ts, pulse) in tss.into_iter().zip(pulses) {
                                        let value = pulse as i64 - pulse_last as i64;
                                        item.push(ts, pulse, value);
                                        pulse_last = pulse;
                                    }
                                    ChannelEvents::Events(Box::new(item))
                                }
                                ChannelEvents::Status(x) => ChannelEvents::Status(x),
                            };
                            x
                        } else {
                            item
                        };
                        Ok(x)
                    }
                    Err(e) => Err(e),
                }
            })
            .map(move |item| match &item {
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
                    warn!("sleep {} times {:?}", n, d);
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
        Ok(Box::pin(stream))
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

    let mut stream: Pin<Box<dyn Stream<Item = Box<dyn Framable + Send>> + Send>> =
        if let AggKind::EventBlobs = evq.agg_kind_value() {
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
            match make_channel_events_stream(evq, node_config).await {
                Ok(stream) => {
                    let stream = stream
                        .map({
                            use items_2::eventtransform::EventTransform;
                            let mut tf = items_2::eventtransform::IdentityTransform::default();
                            move |item| match item {
                                Ok(item2) => match item2 {
                                    StreamItem::DataItem(item3) => match item3 {
                                        RangeCompletableItem::Data(item4) => match item4 {
                                            ChannelEvents::Events(item5) => {
                                                let a = tf.transform(item5);
                                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                                                    ChannelEvents::Events(a),
                                                )))
                                            }
                                            x => Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))),
                                        },
                                        x => Ok(StreamItem::DataItem(x)),
                                    },
                                    x => Ok(x),
                                },
                                _ => item,
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
                if buf.len() > 1024 * 64 {
                    warn!("emit buf len {}", buf.len());
                } else {
                    info!("emit buf len {}", buf.len());
                }
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
