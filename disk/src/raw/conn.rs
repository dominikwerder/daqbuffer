use crate::agg::binnedx::IntoBinnedXBins1;
use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::IntoDim1F32Stream;
use crate::channelconfig::{extract_matching_config_entry, read_local_config};
use crate::eventblobs::EventBlobsComplete;
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::{decode_frame, make_frame, make_term_frame};
use crate::raw::{EventQueryJsonStringFrame, EventsQuery};
use err::Error;
use futures_util::StreamExt;
#[allow(unused_imports)]
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{NodeConfigCached, Shape};
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tracing::Instrument;

pub async fn raw_service(node_config: NodeConfigCached) -> Result<(), Error> {
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

async fn raw_conn_handler(stream: TcpStream, addr: SocketAddr, node_config: NodeConfigCached) -> Result<(), Error> {
    //use tracing_futures::Instrument;
    let span1 = span!(Level::INFO, "raw::raw_conn_handler");
    let r = raw_conn_handler_inner(stream, addr, &node_config)
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
    node_config: &NodeConfigCached,
) -> Result<(), Error> {
    match raw_conn_handler_inner_try(stream, addr, node_config).await {
        Ok(_) => (),
        Err(mut ce) => {
            /*error!(
                "raw_conn_handler_inner  CAUGHT ERROR AND TRY TO SEND OVER TCP {:?}",
                ce.err
            );*/
            let buf = make_frame::<RawConnOut>(&Err(ce.err))?;
            match ce.netout.write_all(&buf).await {
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
    node_config: &NodeConfigCached,
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
    let qitem: EventQueryJsonStringFrame = match decode_frame(&frames[0]) {
        Ok(k) => k,
        Err(e) => return Err((e, netout).into()),
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
    match dbconn::channel_exists(&evq.channel, &node_config).await {
        Ok(_) => (),
        Err(e) => return Err((e, netout))?,
    }
    debug!("REQUEST  {:?}", evq);
    let range = &evq.range;
    let channel_config = match read_local_config(&evq.channel, &node_config.node).await {
        Ok(k) => k,
        Err(e) => return Err((e, netout))?,
    };
    let entry = match extract_matching_config_entry(range, &channel_config) {
        Ok(k) => k,
        Err(e) => return Err((e, netout))?,
    };
    info!("found config entry {:?}", entry);

    let shape = match &entry.shape {
        Some(lens) => {
            if lens.len() == 1 {
                Shape::Wave(lens[0])
            } else {
                return Err((
                    Error::with_msg(format!("Channel config unsupported shape {:?}", entry)),
                    netout,
                ))?;
            }
        }
        None => Shape::Scalar,
    };
    let query = netpod::AggQuerySingleChannel {
        channel_config: netpod::ChannelConfig {
            channel: evq.channel.clone(),
            keyspace: entry.ks as u8,
            time_bin_size: entry.bs,
            shape: shape,
            scalar_type: entry.scalar_type.clone(),
            big_endian: entry.is_big_endian,
            array: entry.is_array,
            compression: entry.is_compressed,
        },
        // TODO use a NanoRange and search for matching files
        timebin: 0,
        tb_file_count: 1,
        // TODO use the requested buffer size
        buffer_size: 1024 * 4,
    };
    let buffer_size = 1024 * 4;
    let mut s1 = EventBlobsComplete::new(
        range.clone(),
        query.channel_config.clone(),
        node_config.node.clone(),
        buffer_size,
    )
    .into_dim_1_f32_stream()
    .into_binned_x_bins_1();
    let mut e = 0;
    while let Some(item) = s1.next().await {
        if let Ok(k) = &item {
            e += 1;
            if false {
                trace!(
                    "emit items  sp {:2}  e {:3}  len {:3}  {:10?}  {:10?}",
                    node_config.node.split,
                    e,
                    k.tss.len(),
                    k.tss.first().map(|k| k / SEC),
                    k.tss.last().map(|k| k / SEC),
                );
            }
        }
        match make_frame::<RawConnOut>(&item) {
            Ok(buf) => match netout.write_all(&buf).await {
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
                Ok(buf) => match netout.write_all(&buf).await {
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
