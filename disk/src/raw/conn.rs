use crate::agg::{IntoBinnedXBins1, IntoDim1F32Stream, MinMaxAvgScalarEventBatch};
use crate::eventblobs::EventBlobsComplete;
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::{make_frame, make_term_frame};
use crate::raw::{EventQueryJsonStringFrame, EventsQuery};
use err::Error;
use futures_util::StreamExt;
#[allow(unused_imports)]
use netpod::log::*;
use netpod::timeunits::DAY;
use netpod::{NodeConfig, ScalarType, Shape};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tracing::Instrument;

pub async fn raw_service(node_config: Arc<NodeConfig>) -> Result<(), Error> {
    let addr = format!("{}:{}", node_config.node.listen, node_config.node.port_raw);
    let lis = tokio::net::TcpListener::bind(addr).await?;
    loop {
        match lis.accept().await {
            Ok((stream, addr)) => {
                taskrun::spawn(raw_conn_handler(stream, addr, node_config.clone()));
            }
            Err(e) => Err(e)?,
        }
    }
}

async fn raw_conn_handler(stream: TcpStream, addr: SocketAddr, node_config: Arc<NodeConfig>) -> Result<(), Error> {
    //use tracing_futures::Instrument;
    let span1 = span!(Level::INFO, "raw::raw_conn_handler");
    let r = raw_conn_handler_inner(stream, addr, node_config)
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

pub type RawConnOut = Result<MinMaxAvgScalarEventBatch, Error>;

async fn raw_conn_handler_inner(
    stream: TcpStream,
    addr: SocketAddr,
    node_config: Arc<NodeConfig>,
) -> Result<(), Error> {
    match raw_conn_handler_inner_try(stream, addr, node_config).await {
        Ok(_) => (),
        Err(mut ce) => {
            /*error!(
                "raw_conn_handler_inner  CAUGHT ERROR AND TRY TO SEND OVER TCP {:?}",
                ce.err
            );*/
            let buf = make_frame::<RawConnOut>(&Err(ce.err))?;
            match ce.netout.write(&buf).await {
                Ok(_) => (),
                Err(e) => return Err(e)?,
            }
        }
    }
    Ok(())
}

struct ConnErr {
    err: Error,
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

async fn raw_conn_handler_inner_try(
    stream: TcpStream,
    addr: SocketAddr,
    node_config: Arc<NodeConfig>,
) -> Result<(), ConnErr> {
    info!("raw_conn_handler   SPAWNED   for {:?}", addr);
    let (netin, mut netout) = stream.into_split();
    let mut h = InMemoryFrameAsyncReadStream::new(netin);
    let mut frames = vec![];
    while let Some(k) = h
        .next()
        .instrument(span!(Level::INFO, "raw_conn_handler  INPUT STREAM READ"))
        .await
    {
        match k {
            Ok(k) => {
                info!(". . . . . . . . . . . . . . . . . . . . . . . . . .   raw_conn_handler  FRAME RECV");
                frames.push(k);
            }
            Err(e) => {
                return Err((e, netout))?;
            }
        }
    }
    if frames.len() != 1 {
        error!("expect a command frame");
        return Err((Error::with_msg("expect a command frame"), netout))?;
    }
    let qitem = match bincode::deserialize::<EventQueryJsonStringFrame>(frames[0].buf()) {
        Ok(k) => k,
        Err(e) => return Err((e, netout))?,
    };
    trace!("json: {}", qitem.0);
    let res: Result<EventsQuery, _> = serde_json::from_str(&qitem.0);
    let evq = match res {
        Ok(k) => k,
        Err(e) => {
            error!("can not parse json {:?}", e);
            return Err((Error::with_msg("can not parse request json"), netout))?;
        }
    };
    error!(
        "TODO decide on response content based on the parsed json query\n{:?}",
        evq
    );
    let query = netpod::AggQuerySingleChannel {
        channel_config: netpod::ChannelConfig {
            channel: netpod::Channel {
                backend: "test1".into(),
                name: "wave1".into(),
            },
            keyspace: 3,
            time_bin_size: DAY,
            shape: Shape::Wave(17),
            scalar_type: ScalarType::F64,
            big_endian: true,
            array: true,
            compression: true,
        },
        // TODO use a NanoRange and search for matching files
        timebin: 0,
        tb_file_count: 1,
        // TODO use the requested buffer size
        buffer_size: 1024 * 4,
    };
    let mut s1 = EventBlobsComplete::new(&query, query.channel_config.clone(), node_config.node.clone())
        .into_dim_1_f32_stream()
        .take(10)
        .into_binned_x_bins_1();
    while let Some(item) = s1.next().await {
        if let Ok(k) = &item {
            trace!(
                "emit items {}  {:?}  {:?}",
                k.tss.len(),
                k.tss.first().map(|k| k / 1000000000),
                k.tss.last().map(|k| k / 1000000000)
            );
        }
        match make_frame::<RawConnOut>(&item) {
            Ok(buf) => match netout.write(&buf).await {
                Ok(_) => {}
                Err(e) => return Err((e, netout))?,
            },
            Err(e) => {
                return Err((e, netout))?;
            }
        }
    }
    if false {
        // Manual test batch.
        let mut batch = MinMaxAvgScalarEventBatch::empty();
        batch.tss.push(42);
        batch.tss.push(43);
        batch.mins.push(7.1);
        batch.mins.push(7.2);
        batch.maxs.push(8.3);
        batch.maxs.push(8.4);
        batch.avgs.push(9.5);
        batch.avgs.push(9.6);
        let mut s1 = futures_util::stream::iter(vec![batch]).map(Result::Ok);
        while let Some(item) = s1.next().await {
            match make_frame::<RawConnOut>(&item) {
                Ok(buf) => match netout.write(&buf).await {
                    Ok(_) => {}
                    Err(e) => return Err((e, netout))?,
                },
                Err(e) => {
                    return Err((e, netout))?;
                }
            }
        }
    }
    let buf = make_term_frame();
    match netout.write(&buf).await {
        Ok(_) => (),
        Err(e) => return Err((e, netout))?,
    }
    match netout.flush().await {
        Ok(_) => (),
        Err(e) => return Err((e, netout))?,
    }
    Ok(())
}
