// TODO move these frame-related things out of crate disk. Probably better into `nodenet`
use disk::frame::inmem::InMemoryFrameAsyncReadStream;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::frame::{decode_frame, make_term_frame};
use items::{Framable, StreamItem};
use netpod::log::*;
use netpod::query::RawEventsQuery;
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
    //use tracing_futures::Instrument;
    let span1 = span!(Level::INFO, "raw::raw_conn_handler");
    let r = events_conn_handler_inner(stream, addr, &node_config)
        .instrument(span1)
        .await;
    match r {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("raw_conn_handler sees error: {:?}", e);
            Err(e)
        }
    }
}

async fn events_conn_handler_inner(
    stream: TcpStream,
    addr: SocketAddr,
    node_config: &NodeConfigCached,
) -> Result<(), Error> {
    match events_conn_handler_inner_try(stream, addr, node_config).await {
        Ok(_) => (),
        Err(ce) => {
            // TODO pass errors over network.
            error!("events_conn_handler_inner: {:?}", ce.err);
        }
    }
    Ok(())
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
        .instrument(span!(Level::INFO, "raw_conn_handler  INPUT STREAM READ"))
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
    info!("---------------------------------------------------\nevq {:?}", evq);

    let mut p1: Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>> =
        if let Some(aa) = &node_config.node.archiver_appliance {
            match archapp_wrap::make_event_pipe(&evq, aa).await {
                Ok(j) => j,
                Err(e) => return Err((e, netout))?,
            }
        } else {
            match disk::raw::conn::make_event_pipe(&evq, node_config).await {
                Ok(j) => j,
                Err(e) => return Err((e, netout))?,
            }
        };

    while let Some(item) = p1.next().await {
        //info!("conn.rs  encode frame typeid {:x}", item.typeid());
        let item = item.make_frame();
        match item {
            Ok(buf) => match netout.write_all(&buf).await {
                Ok(_) => {}
                Err(e) => return Err((e, netout))?,
            },
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
    Ok(())
}
