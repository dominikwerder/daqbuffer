#[cfg(test)]
mod test;

use dbconn::events_scylla::make_scylla_stream;
use disk::frame::inmem::InMemoryFrameAsyncReadStream;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::frame::{decode_frame, make_term_frame};
use items::{Framable, StreamItem};
use netpod::histo::HistoLog2;
use netpod::log::*;
use netpod::query::RawEventsQuery;
use netpod::AggKind;
use netpod::{EventQueryJsonStringFrame, NodeConfigCached, PerfOpts};
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tracing::Instrument;

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
    let mut frames = vec![];
    while let Some(k) = h
        .next()
        .instrument(span!(Level::INFO, "events_conn_handler  INPUT STREAM READ"))
        .await
    {
        match k {
            Ok(StreamItem::DataItem(item)) => {
                frames.push(item);
            }
            Ok(_) => {}
            Err(e) => {
                return Err((e, netout))?;
            }
        }
    }
    if frames.len() != 1 {
        error!("missing command frame");
        return Err((Error::with_msg("missing command frame"), netout))?;
    }
    let qitem: EventQueryJsonStringFrame = match decode_frame(&frames[0]) {
        Ok(k) => k,
        Err(e) => return Err((e, netout).into()),
    };
    let res: Result<RawEventsQuery, _> = serde_json::from_str(&qitem.0);
    let evq = match res {
        Ok(k) => k,
        Err(e) => {
            error!("json parse error: {:?}", e);
            return Err((Error::with_msg("json parse error"), netout))?;
        }
    };
    info!("events_conn_handler_inner_try  evq {:?}", evq);

    if evq.do_test_main_error {
        let e = Error::with_msg(format!("Test error private message."))
            .add_public_msg(format!("Test error PUBLIC message."));
        return Err((e, netout).into());
    }

    let mut p1: Pin<Box<dyn Stream<Item = Box<dyn Framable + Send>> + Send>> =
        if let Some(conf) = &node_config.node_config.cluster.scylla {
            let scyco = conf;
            let dbconf = node_config.node_config.cluster.database.clone();
            match make_scylla_stream(&evq, scyco, dbconf, evq.do_test_stream_error).await {
                Ok(s) => {
                    //
                    let s = s.map(|item| {
                        //
                        /*match item {
                            Ok(StreamItem::Data(RangeCompletableItem::Data(k))) => {
                                let b = Box::new(b);
                                Ok(StreamItem::Data(RangeCompletableItem::Data(b)))
                            }
                            Ok(StreamItem::Data(RangeCompletableItem::Complete)) => {
                                Ok(StreamItem::Data(RangeCompletableItem::Complete))
                            }
                            Ok(StreamItem::Log(k)) => Ok(StreamItem::Log(k)),
                            Ok(StreamItem::Stats(k)) => Ok(StreamItem::Stats(k)),
                            Err(e) => Err(e),
                        }*/
                        Box::new(item) as Box<dyn Framable + Send>
                    });
                    Box::pin(s)
                }
                Err(e) => return Err((e, netout))?,
            }
        } else if let Some(_) = &node_config.node.channel_archiver {
            let e = Error::with_msg_no_trace("archapp not built");
            return Err((e, netout))?;
        } else if let Some(_) = &node_config.node.archiver_appliance {
            let e = Error::with_msg_no_trace("archapp not built");
            return Err((e, netout))?;
        } else {
            match evq.agg_kind {
                AggKind::EventBlobs => match disk::raw::conn::make_event_blobs_pipe(&evq, node_config).await {
                    Ok(j) => j,
                    Err(e) => return Err((e, netout))?,
                },
                _ => match disk::raw::conn::make_event_pipe(&evq, node_config).await {
                    Ok(j) => j,
                    Err(e) => return Err((e, netout))?,
                },
            }
        };
    let mut buf_len_histo = HistoLog2::new(5);
    while let Some(item) = p1.next().await {
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
                return Err((e, netout))?;
            }
        }
    }
    let buf = make_term_frame();
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
