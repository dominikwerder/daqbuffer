use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::streams::StreamItem;
use crate::binned::{EventsNodeProcessor, NumOps, RangeCompletableItem};
use crate::decode::{
    BigEndian, Endianness, EventValueFromBytes, EventValueShape, EventValuesDim0Case, EventValuesDim1Case,
    EventsDecodedStream, LittleEndian, NumFromBytes,
};
use crate::eventblobs::EventChunkerMultifile;
use crate::eventchunker::EventChunkerConf;
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::{decode_frame, make_frame, make_term_frame, Framable};
use crate::raw::{EventQueryJsonStringFrame, RawEventsQuery};
use crate::Sitemty;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{AggKind, BoolNum, ByteOrder, ByteSize, NodeConfigCached, PerfOpts, ScalarType, Shape};
use parse::channelconfig::{extract_matching_config_entry, read_local_config, MatchingConfigEntry};
use std::io;
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
        Err(mut ce) => {
            error!("events_conn_handler_inner: {:?}", ce.err);
            if false {
                let buf = make_frame::<Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarEventBatch>>, Error>>(
                    &Err(ce.err),
                )?;
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

fn make_num_pipeline_stream_evs<NTY, END, EVS, ENP>(
    event_value_shape: EVS,
    events_node_proc: ENP,
    event_blobs: EventChunkerMultifile,
) -> Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>
where
    NTY: NumOps + NumFromBytes<NTY, END> + 'static,
    END: Endianness + 'static,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
    ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Output> + 'static,
    Sitemty<<ENP as EventsNodeProcessor>::Output>: Framable + 'static,
    <ENP as EventsNodeProcessor>::Output: 'static,
{
    let decs = EventsDecodedStream::<NTY, END, EVS>::new(event_value_shape, event_blobs);
    let s2 = StreamExt::map(decs, move |item| match item {
        Ok(item) => match item {
            StreamItem::DataItem(item) => match item {
                RangeCompletableItem::Data(item) => {
                    let item = events_node_proc.process(item);
                    Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                }
                RangeCompletableItem::RangeComplete => Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)),
            },
            StreamItem::Log(item) => Ok(StreamItem::Log(item)),
            StreamItem::Stats(item) => Ok(StreamItem::Stats(item)),
        },
        Err(e) => Err(e),
    })
    .map(|item| Box::new(item) as Box<dyn Framable>);
    Box::pin(s2)
}

macro_rules! pipe4 {
    ($nty:ident, $end:ident, $shape:expr, $evs:ident, $evsv:expr, $agg_kind:expr, $event_blobs:expr) => {
        match $agg_kind {
            AggKind::DimXBins1 => make_num_pipeline_stream_evs::<$nty, $end, $evs<$nty>, _>(
                $evsv,
                <$evs<$nty> as EventValueShape<$nty, $end>>::NumXAggToSingleBin::create($shape, $agg_kind),
                $event_blobs,
            ),
            AggKind::DimXBinsN(_) => make_num_pipeline_stream_evs::<$nty, $end, $evs<$nty>, _>(
                $evsv,
                <$evs<$nty> as EventValueShape<$nty, $end>>::NumXAggToNBins::create($shape, $agg_kind),
                $event_blobs,
            ),
            AggKind::Plain => make_num_pipeline_stream_evs::<$nty, $end, $evs<$nty>, _>(
                $evsv,
                <$evs<$nty> as EventValueShape<$nty, $end>>::NumXAggPlain::create($shape, $agg_kind),
                $event_blobs,
            ),
        }
    };
}

macro_rules! pipe3 {
    ($nty:ident, $end:ident, $shape:expr, $agg_kind:expr, $event_blobs:expr) => {
        match $shape {
            Shape::Scalar => {
                pipe4!(
                    $nty,
                    $end,
                    $shape,
                    EventValuesDim0Case,
                    EventValuesDim0Case::<$nty>::new(),
                    $agg_kind,
                    $event_blobs
                )
            }
            Shape::Wave(n) => {
                pipe4!(
                    $nty,
                    $end,
                    $shape,
                    EventValuesDim1Case,
                    EventValuesDim1Case::<$nty>::new(n),
                    $agg_kind,
                    $event_blobs
                )
            }
        }
    };
}

macro_rules! pipe2 {
    ($nty:ident, $end:expr, $shape:expr, $agg_kind:expr, $event_blobs:expr) => {
        match $end {
            ByteOrder::LE => pipe3!($nty, LittleEndian, $shape, $agg_kind, $event_blobs),
            ByteOrder::BE => pipe3!($nty, BigEndian, $shape, $agg_kind, $event_blobs),
        }
    };
}

macro_rules! pipe1 {
    ($nty:expr, $end:expr, $shape:expr, $agg_kind:expr, $event_blobs:expr) => {
        match $nty {
            ScalarType::U8 => pipe2!(u8, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::U16 => pipe2!(u16, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::U32 => pipe2!(u32, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::U64 => pipe2!(u64, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::I8 => pipe2!(i8, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::I16 => pipe2!(i16, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::I32 => pipe2!(i32, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::I64 => pipe2!(i64, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::F32 => pipe2!(f32, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::F64 => pipe2!(f64, $end, $shape, $agg_kind, $event_blobs),
            ScalarType::BOOL => pipe2!(BoolNum, $end, $shape, $agg_kind, $event_blobs),
        }
    };
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
    match dbconn::channel_exists(&evq.channel, &node_config).await {
        Ok(_) => (),
        Err(e) => return Err((e, netout))?,
    }
    let range = &evq.range;
    let channel_config = match read_local_config(&evq.channel, &node_config.node).await {
        Ok(k) => k,
        Err(e) => {
            if e.msg().contains("ErrorKind::NotFound") {
                return Ok(());
            } else {
                return Err((e, netout))?;
            }
        }
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
        byte_order: entry.byte_order.clone(),
        array: entry.is_array,
        compression: entry.is_compressed,
    };
    let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
    let event_blobs = EventChunkerMultifile::new(
        range.clone(),
        channel_config.clone(),
        node_config.node.clone(),
        node_config.ix,
        evq.disk_io_buffer_size,
        event_chunker_conf,
    );
    let shape = entry.to_shape().unwrap();
    let mut p1 = pipe1!(entry.scalar_type, entry.byte_order, shape, evq.agg_kind, event_blobs);
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
