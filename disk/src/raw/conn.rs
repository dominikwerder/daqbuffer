use crate::agg::binnedx::IntoBinnedXBins1;
use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::streams::StreamItem;
use crate::agg::IntoDim1F32Stream;
use crate::binned::{BinnedStreamKind, BinnedStreamKindScalar, RangeCompletableItem};
use crate::eventblobs::EventBlobsComplete;
use crate::eventchunker::EventChunkerConf;
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::{decode_frame, make_frame, make_term_frame, FrameType};
use crate::raw::{EventQueryJsonStringFrame, EventsQuery};
use err::Error;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{AggKind, ByteSize, NodeConfigCached, PerfOpts};
use parse::channelconfig::{extract_matching_config_entry, read_local_config, MatchingConfigEntry};
use std::io;
use std::net::SocketAddr;
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
        Err(mut ce) => {
            // TODO is it guaranteed to be compatible to serialize this way?
            let buf =
                make_frame::<Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarEventBatch>>, Error>>(&Err(ce.err))?;
            match ce.netout.write_all(&buf).await {
                Ok(_) => (),
                Err(e) => match e.kind() {
                    io::ErrorKind::BrokenPipe => {}
                    _ => {
                        error!("events_conn_handler_inner sees: {:?}", e);
                        return Err(e)?;
                    }
                },
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
    let frame_type = <EventQueryJsonStringFrame as FrameType>::FRAME_TYPE_ID;
    let qitem: EventQueryJsonStringFrame = match decode_frame(&frames[0], frame_type) {
        Ok(k) => k,
        Err(e) => return Err((e, netout).into()),
    };
    let res: Result<EventsQuery, _> = serde_json::from_str(&qitem.0);
    let evq = match res {
        Ok(k) => k,
        Err(e) => {
            error!("json parse error: {:?}", e);
            return Err((Error::with_msg("json parse error"), netout))?;
        }
    };
    match dbconn::channel_exists(&evq.channel, &node_config).await {
        Ok(_) => (),
        Err(e) => return Err((e, netout))?,
    }
    let range = &evq.range;
    let channel_config = match read_local_config(&evq.channel, &node_config.node).await {
        Ok(k) => k,
        Err(e) => return Err((e, netout))?,
    };
    let entry_res = match extract_matching_config_entry(range, &channel_config) {
        Ok(k) => k,
        Err(e) => return Err((e, netout))?,
    };
    let entry = match entry_res {
        MatchingConfigEntry::None => return Err((Error::with_msg("no config entry found"), netout))?,
        MatchingConfigEntry::Multiple => return Err((Error::with_msg("multiple config entries found"), netout))?,
        MatchingConfigEntry::Entry(entry) => entry,
    };
    let shape = match entry.to_shape() {
        Ok(k) => k,
        Err(e) => return Err((e, netout))?,
    };
    let channel_config = netpod::ChannelConfig {
        channel: evq.channel.clone(),
        keyspace: entry.ks as u8,
        time_bin_size: entry.bs,
        shape: shape,
        scalar_type: entry.scalar_type.clone(),
        big_endian: entry.is_big_endian,
        array: entry.is_array,
        compression: entry.is_compressed,
    };
    // TODO use a requested buffer size
    let buffer_size = 1024 * 4;
    let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
    let s1 = EventBlobsComplete::new(
        range.clone(),
        channel_config.clone(),
        node_config.node.clone(),
        node_config.ix,
        buffer_size,
        event_chunker_conf,
    )
    .into_dim_1_f32_stream();
    // TODO need to decide already here on the type I want to use.
    let mut s1 = IntoBinnedXBins1::<_, BinnedStreamKindScalar>::into_binned_x_bins_1(s1);
    let mut e = 0;
    while let Some(item) = s1.next().await {
        match &item {
            Ok(StreamItem::DataItem(_)) => {
                e += 1;
            }
            Ok(_) => {}
            Err(_) => {}
        }
        match evq.agg_kind {
            AggKind::DimXBins1 => {
                match make_frame::<
                    Result<
                        StreamItem<RangeCompletableItem<<BinnedStreamKindScalar as BinnedStreamKind>::XBinnedEvents>>,
                        Error,
                    >,
                >(&item)
                {
                    Ok(buf) => match netout.write_all(&buf).await {
                        Ok(_) => {}
                        Err(e) => return Err((e, netout))?,
                    },
                    Err(e) => {
                        return Err((e, netout))?;
                    }
                }
            }
            // TODO define this case:
            AggKind::DimXBinsN(_xbincount) => match make_frame::<
                Result<
                    StreamItem<RangeCompletableItem<<BinnedStreamKindScalar as BinnedStreamKind>::XBinnedEvents>>,
                    Error,
                >,
            >(err::todoval())
            {
                Ok(buf) => match netout.write_all(&buf).await {
                    Ok(_) => {}
                    Err(e) => return Err((e, netout))?,
                },
                Err(e) => {
                    return Err((e, netout))?;
                }
            },
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
    let _total_written_value_items = e;
    Ok(())
}
