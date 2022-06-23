use crate::err::ArchError;
use crate::generated::EPICSEvent::PayloadType;
use crate::parse::multi::parse_all_ts;
use crate::parse::PbFileReader;
use crate::storagemerge::StorageMerge;
use chrono::{TimeZone, Utc};
use err::{ErrStr, Error};
use futures_core::Stream;
use futures_util::StreamExt;
use items::binnedevents::{MultiBinWaveEvents, SingleBinWaveEvents, XBinnedEvents};
use items::eventsitem::EventsItem;
use items::plainevents::{PlainEvents, ScalarPlainEvents, WavePlainEvents};
use items::scalarevents::ScalarEvents;
use items::waveevents::WaveEvents;
use items::xbinnedscalarevents::XBinnedScalarEvents;
use items::xbinnedwaveevents::XBinnedWaveEvents;
use items::{Framable, RangeCompletableItem, Sitemty, SitemtyFrameType, StreamItem, WithLen, WithTimestamps};
use netpod::log::*;
use netpod::query::RawEventsQuery;
use netpod::timeunits::{DAY, SEC};
use netpod::{AggKind, ArchiverAppliance, Channel, ChannelInfo, HasScalarType, HasShape, NanoRange, ScalarType, Shape};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::fs::{read_dir, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[derive(Debug)]
pub struct DataFilename {
    pub year: u32,
    pub month: u32,
}

pub fn parse_data_filename(s: &str) -> Result<DataFilename, Error> {
    if !s.ends_with(".pb") {
        return Err(Error::with_msg_no_trace("not a .pb file"));
    }
    if s.len() < 12 {
        return Err(Error::with_msg_no_trace("filename too short"));
    }
    let j = &s[s.len() - 11..];
    if &j[0..1] != ":" {
        return Err(Error::with_msg_no_trace("no colon"));
    }
    if &j[5..6] != "_" {
        return Err(Error::with_msg_no_trace("no underscore"));
    }
    let year: u32 = j[1..5].parse()?;
    let month: u32 = j[6..8].parse()?;
    let ret = DataFilename { year, month };
    Ok(ret)
}

// TODO do we need Send here?
pub trait FrameMakerTrait: Send {
    fn make_frame(&mut self, ei: Sitemty<EventsItem>) -> Box<dyn Framable + Send>;
}

pub struct FrameMaker {
    scalar_type: ScalarType,
    shape: Shape,
    agg_kind: AggKind,
}

impl FrameMaker {
    #[allow(dead_code)]
    fn make_frame_gen<T>(_item: Sitemty<EventsItem>) -> Box<dyn Framable>
    where
        T: SitemtyFrameType + Serialize + Send + 'static,
    {
        err::todoval()
    }

    pub fn with_item_type(scalar_type: ScalarType, shape: Shape, agg_kind: AggKind) -> Self {
        Self {
            scalar_type: scalar_type,
            shape: shape,
            agg_kind: agg_kind,
        }
    }
}

#[allow(unused_macros)]
macro_rules! events_item_to_sitemty {
    ($ei:expr, $t1:ident, $t2:ident, $t3:ident) => {{
        let combo = format!("t1 {}  t2 {}  t3 {}", stringify!($t1), stringify!($t2), stringify!($t3));
        let ret = match $ei {
            Ok(k) => match k {
                StreamItem::DataItem(k) => match k {
                    RangeCompletableItem::Data(k) => {
                        //
                        match k {
                            EventsItem::Plain(h) => {
                                //
                                match h {
                                    PlainEvents::$t1(h) => {
                                        //
                                        match h {
                                            $t2::$t3(h) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(h))),
                                            _ => {
                                                warn!("case AA {}", combo);
                                                panic!()
                                            }
                                        }
                                    }
                                    _ => {
                                        warn!("case BB {}", combo);
                                        panic!()
                                    }
                                }
                            }
                            _ => {
                                warn!("case CC {}", combo);
                                panic!()
                            }
                        }
                    }
                    RangeCompletableItem::RangeComplete => {
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                    }
                },
                StreamItem::Log(j) => Ok(StreamItem::Log(j)),
                StreamItem::Stats(j) => Ok(StreamItem::Stats(j)),
            },
            Err(e) => Err(e),
        };
        Box::new(ret)
    }};
}

macro_rules! arm2 {
    ($item:expr, $t1:ident, $t2:ident, $t3:ident, $t4:ident, $t5:ident, $sty1:ident, $sty2:ident) => {{
        type T1 = $t1<$sty1>;
        let combo = format!(
            "t1 {}  t2 {}  t3 {}  t4 {}  t5 {}  sty1 {}  sty2 {}",
            stringify!($t1),
            stringify!($t2),
            stringify!($t3),
            stringify!($t4),
            stringify!($t5),
            stringify!($sty1),
            stringify!($sty2)
        );
        let ret: Sitemty<T1> = match $item {
            Ok(k) => match k {
                StreamItem::DataItem(k) => match k {
                    RangeCompletableItem::RangeComplete => {
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                    }
                    RangeCompletableItem::Data(k) => match k {
                        EventsItem::$t2(k) => match k {
                            $t3::$t4(k) => match k {
                                $t5::$sty2(k) => {
                                    //
                                    Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
                                }
                                _ => {
                                    warn!("unclear what to do A {}", combo);
                                    err::todoval()
                                }
                            },
                            _ => {
                                warn!("unclear what to do B {}", combo);
                                err::todoval()
                            }
                        },
                        _ => {
                            error!("unexpected arm2 case {}", combo);
                            err::todoval()
                        }
                    },
                },
                StreamItem::Log(k) => Ok(StreamItem::Log(k)),
                StreamItem::Stats(k) => Ok(StreamItem::Stats(k)),
            },
            Err(e) => Err(e),
        };
        Box::new(ret) as Box<dyn Framable + Send>
    }};
}

macro_rules! arm1 {
    ($item:expr, $sty1:ident, $sty2:ident, $shape:expr, $ak:expr) => {{
        if let AggKind::Stats1 = $ak {
            err::todo();
            return arm2!(
                $item,
                ScalarEvents,
                Plain,
                PlainEvents,
                Scalar,
                ScalarPlainEvents,
                $sty1,
                $sty2
            );
        }
        match $shape {
            Shape::Scalar => match $ak {
                AggKind::EventBlobs => {
                    warn!("arm1 unhandled AggKind::EventBlobs");
                    panic!()
                }
                AggKind::Plain => arm2!(
                    $item,
                    ScalarEvents,
                    Plain,
                    PlainEvents,
                    Scalar,
                    ScalarPlainEvents,
                    $sty1,
                    $sty2
                ),
                AggKind::TimeWeightedScalar => arm2!(
                    $item,
                    ScalarEvents,
                    XBinnedEvents,
                    XBinnedEvents,
                    Scalar,
                    ScalarPlainEvents,
                    $sty1,
                    $sty2
                ),
                AggKind::DimXBins1 => arm2!(
                    $item,
                    ScalarEvents,
                    XBinnedEvents,
                    XBinnedEvents,
                    Scalar,
                    ScalarPlainEvents,
                    $sty1,
                    $sty2
                ),
                AggKind::DimXBinsN(_) => arm2!(
                    $item,
                    ScalarEvents,
                    XBinnedEvents,
                    XBinnedEvents,
                    Scalar,
                    ScalarPlainEvents,
                    $sty1,
                    $sty2
                ),
                // Handled above..
                AggKind::Stats1 => panic!(),
            },
            Shape::Wave(_) => match $ak {
                AggKind::EventBlobs => {
                    warn!("arm1 unhandled EventBlobs");
                    panic!()
                }
                AggKind::Plain => arm2!(
                    $item,
                    WaveEvents,
                    Plain,
                    PlainEvents,
                    Wave,
                    WavePlainEvents,
                    $sty1,
                    $sty2
                ),
                AggKind::TimeWeightedScalar => arm2!(
                    $item,
                    XBinnedScalarEvents,
                    XBinnedEvents,
                    XBinnedEvents,
                    SingleBinWave,
                    SingleBinWaveEvents,
                    $sty1,
                    $sty2
                ),
                AggKind::DimXBins1 => arm2!(
                    $item,
                    XBinnedScalarEvents,
                    XBinnedEvents,
                    XBinnedEvents,
                    SingleBinWave,
                    SingleBinWaveEvents,
                    $sty1,
                    $sty2
                ),
                AggKind::DimXBinsN(_) => arm2!(
                    $item,
                    XBinnedWaveEvents,
                    XBinnedEvents,
                    XBinnedEvents,
                    MultiBinWave,
                    MultiBinWaveEvents,
                    $sty1,
                    $sty2
                ),
                // Handled above..
                AggKind::Stats1 => panic!(),
            },
            Shape::Image(..) => {
                // There should be no images on archiver.
                warn!("TODO for {:?}", $shape);
                err::todoval()
            }
        }
    }};
}

impl FrameMakerTrait for FrameMaker {
    fn make_frame(&mut self, item: Sitemty<EventsItem>) -> Box<dyn Framable + Send> {
        let scalar_type = &self.scalar_type;
        let shape = &self.shape;
        let agg_kind = &self.agg_kind;
        match scalar_type {
            ScalarType::I8 => arm1!(item, i8, I8, shape, agg_kind),
            ScalarType::I16 => arm1!(item, i16, I16, shape, agg_kind),
            ScalarType::I32 => arm1!(item, i32, I32, shape, agg_kind),
            ScalarType::F32 => arm1!(item, f32, F32, shape, agg_kind),
            ScalarType::F64 => arm1!(item, f64, F64, shape, agg_kind),
            _ => {
                warn!("TODO for scalar_type {:?}", scalar_type);
                err::todoval()
            }
        }
    }
}

pub async fn make_event_pipe(
    evq: &RawEventsQuery,
    aa: &ArchiverAppliance,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable + Send>> + Send>>, Error> {
    let ci = channel_info(&evq.channel, aa).await?;
    let mut inps = vec![];
    let mut names = vec![];
    for p1 in &aa.data_base_paths {
        let p2 = p1.clone();
        let p3 = make_single_event_pipe(evq, p2).await?;
        inps.push(p3);
        names.push(p1.to_str().unwrap().into());
    }
    let sm = StorageMerge::new(inps, names, evq.range.clone());
    let mut frame_maker = Box::new(FrameMaker::with_item_type(
        ci.scalar_type.clone(),
        ci.shape.clone(),
        evq.agg_kind.clone(),
    )) as Box<dyn FrameMakerTrait>;
    let ret = sm.map(move |j| frame_maker.make_frame(j));
    Ok(Box::pin(ret))
}

pub async fn make_single_event_pipe(
    evq: &RawEventsQuery,
    base_path: PathBuf,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventsItem>> + Send>>, Error> {
    // TODO must apply the proper x-binning depending on the requested AggKind.
    debug!("make_single_event_pipe  {:?}", evq);
    let evq = evq.clone();
    let DirAndPrefix { dir, prefix } = directory_for_channel_files(&evq.channel, &base_path)?;
    //let dtbeg = Utc.timestamp((evq.range.beg / 1000000000) as i64, (evq.range.beg % 1000000000) as u32);
    let (tx, rx) = async_channel::bounded(16);
    let block1 = async move {
        debug!("start read of {:?}", dir);

        // TODO first collect all matching filenames, then sort, then open files.
        // TODO if dir does not exist, should notify client but not log as error.
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(k) => k,
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    warn!("does not exist: {:?}", dir);
                    return Ok(());
                }
                _ => return Err(e)?,
            },
        };
        while let Some(de) = rd.next_entry().await? {
            let s = de.file_name().to_string_lossy().into_owned();
            if s.starts_with(&prefix) && s.ends_with(".pb") {
                match parse_data_filename(&s) {
                    Ok(df) => {
                        debug!("parse went ok: {} {}", df.year, df.month);
                        let ts0 = Utc.ymd(df.year as i32, df.month, 1).and_hms(0, 0, 0);
                        let ts1 = ts0.timestamp() as u64 * SEC + ts0.timestamp_subsec_nanos() as u64;
                        debug!("file    {}   {}", ts1, ts1 + DAY * 27);
                        debug!("range   {}   {}", evq.range.beg, evq.range.end);
                        if evq.range.beg < ts1 + DAY * 27 && evq.range.end > ts1 {
                            debug!("••••••••••••••••••••••••••  file matches requested range");
                            let f1 = File::open(de.path()).await?;
                            info!("opened {:?}", de.path());
                            let mut z = position_file_for_evq(f1, evq.clone(), df.year).await?;
                            let mut pbr = if let PositionState::Positioned(pos) = z.state {
                                z.pbr.reset_io(pos).await?;
                                z.pbr
                            } else {
                                continue;
                            };
                            let mut i1 = 0;
                            'evread: loop {
                                match pbr.read_msg().await {
                                    Ok(Some(ei)) => {
                                        let ei = ei.item;
                                        let tslast = if ei.len() > 0 { Some(ei.ts(ei.len() - 1)) } else { None };
                                        i1 += 1;
                                        if i1 % 1000 == 0 {
                                            info!("read msg from file {}", i1);
                                        }
                                        let ei2 = ei.x_aggregate(&evq.agg_kind);
                                        let g = Ok(StreamItem::DataItem(RangeCompletableItem::Data(ei2)));
                                        tx.send(g).await.errstr()?;
                                        if let Some(t) = tslast {
                                            if t >= evq.range.end {
                                                info!("after requested range, break");
                                                break 'evread;
                                            }
                                        }
                                    }
                                    Ok(None) => {
                                        debug!("reached end of file");
                                        break;
                                    }
                                    Err(e) => {
                                        error!("error while reading msg  {:?}", e);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("bad filename parse {:?}", e);
                    }
                }
            } else {
                debug!("prefix {}  s {}", prefix, s);
            }
        }
        Ok::<_, Error>(())
    };
    let block2 = async move {
        match block1.await {
            Ok(_) => {}
            Err(e) => {
                error!("{:?}", e);
            }
        }
    };
    tokio::task::spawn(block2);
    Ok(Box::pin(rx))
}

pub enum PositionState {
    NothingFound,
    Positioned(u64),
}

pub struct PositionResult {
    pub pbr: PbFileReader,
    pub state: PositionState,
}

pub async fn position_file_for_evq(mut file: File, evq: RawEventsQuery, year: u32) -> Result<PositionResult, Error> {
    trace!("--------------  position_file_for_evq");
    let flen = file.seek(SeekFrom::End(0)).await?;
    file.seek(SeekFrom::Start(0)).await?;
    if true || flen < 1024 * 512 {
        position_file_for_evq_linear(file, evq, year).await
    } else {
        position_file_for_evq_binary(file, evq, year).await
    }
}

async fn position_file_for_evq_linear(file: File, evq: RawEventsQuery, _year: u32) -> Result<PositionResult, Error> {
    // TODO make read of header part of init:
    let mut pbr = PbFileReader::new(file).await?;
    let mut curpos;
    loop {
        // TODO
        // Issue is that I always read more than the actual packet.
        // Is protobuf length-framed?
        // Otherwise: read_header must return the number of bytes that were read.
        curpos = pbr.abspos();
        trace!("position_file_for_evq_linear  save curpos {}", curpos);
        let res = pbr.read_msg().await?;
        match res {
            Some(res) => {
                trace!(
                    "position_file_for_evq_linear  read_msg  pos {}  len {}",
                    res.pos,
                    res.item.len()
                );
                if res.item.len() < 1 {
                    return Err(Error::with_msg_no_trace("no event read from file"));
                }
                let tslast = res.item.ts(res.item.len() - 1);
                let diff = tslast as i64 - evq.range.beg as i64;
                trace!("position_file_for_evq_linear  tslast {}   diff {}", tslast, diff);
                if tslast >= evq.range.beg {
                    debug!("position_file_for_evq_linear  Positioned  curpos {}", curpos);
                    pbr.reset_io(curpos).await?;
                    let ret = PositionResult {
                        state: PositionState::Positioned(curpos),
                        pbr,
                    };
                    return Ok(ret);
                }
            }
            None => {
                debug!("position_file_for_evq_linear  NothingFound");
                pbr.reset_io(0).await?;
                let ret = PositionResult {
                    state: PositionState::NothingFound,
                    pbr,
                };
                return Ok(ret);
            }
        }
    }
}

async fn position_file_for_evq_binary(mut file: File, evq: RawEventsQuery, year: u32) -> Result<PositionResult, Error> {
    debug!("position_file_for_evq_binary");
    let flen = file.seek(SeekFrom::End(0)).await?;
    file.seek(SeekFrom::Start(0)).await?;
    // TODO make read of header part of init:
    let mut pbr = PbFileReader::new(file).await?;
    let payload_type = pbr.payload_type().clone();
    let res = pbr.read_msg().await?;
    //let mut file = pbr.into_file();
    let file = pbr.file();
    let res = if let Some(res) = res {
        res
    } else {
        return Err(Error::with_msg_no_trace("no event read from file"));
    };
    if res.item.len() < 1 {
        return Err(Error::with_msg_no_trace("no event read from file"));
    }
    let events_begin_pos = res.pos;

    // * the search invariant is that the ts1 < beg and ts2 >= end
    // * read some data from the end.
    // * read some data from the begin.
    // * extract events from begin and end.
    // * check if the binary search invariant is already violated, in that case return.
    // * otherwise, choose some spot in the middle, read there the next chunk.
    //   Then use the actual position of the found item!
    let mut buf1 = vec![0; 1024 * 16];
    let mut buf2 = vec![0; 1024 * 16];
    let mut buf3 = vec![0; 1024 * 16];

    let mut p1 = events_begin_pos;
    let mut p2 = flen - buf2.len() as u64;

    file.seek(SeekFrom::Start(p1 - 1)).await?;
    file.read_exact(&mut buf1).await?;
    file.seek(SeekFrom::Start(p2)).await?;
    file.read_exact(&mut buf2).await?;

    let evs1 = parse_all_ts(p1 - 1, &buf1, payload_type.clone(), year)?;
    let evs2 = parse_all_ts(p2, &buf2, payload_type.clone(), year)?;

    debug!("...............................................................");
    debug!("evs1.len() {:?}", evs1.len());
    debug!("evs2.len() {:?}", evs2.len());
    debug!("p1: {}", p1);
    debug!("p2: {}", p2);

    let tgt = evq.range.beg;

    {
        let ev = evs1.first().unwrap();
        if ev.ts >= tgt {
            pbr.reset_io(ev.pos).await?;
            let ret = PositionResult {
                state: PositionState::Positioned(ev.pos),
                pbr,
            };
            return Ok(ret);
        }
    }
    {
        let ev = evs2.last().unwrap();
        if ev.ts < tgt {
            pbr.reset_io(0).await?;
            let ret = PositionResult {
                state: PositionState::NothingFound,
                pbr,
            };
            return Ok(ret);
        }
    }

    p2 = evs2.last().unwrap().pos;

    // TODO make sure that NL-delimited chunks have a max size.
    loop {
        info!("bsearch loop  p1 {}  p2 {}", p1, p2);
        if p2 - p1 < 1024 * 128 {
            // TODO switch here to linear search...
            info!("switch to linear search in pos {}..{}", p1, p2);
            return linear_search_2(pbr, evq, year, p1, p2, payload_type).await;
        }
        let p3 = (p2 + p1) / 2;
        file.seek(SeekFrom::Start(p3)).await?;
        file.read_exact(&mut buf3).await?;
        let evs3 = parse_all_ts(p3, &buf3, payload_type.clone(), year)?;
        let ev = evs3.first().unwrap();
        if ev.ts < tgt {
            info!("p3 {}   ts: {}  pos: {}  branch A", p3, ev.ts, ev.pos);
            p1 = ev.pos;
        } else {
            info!("p3 {}   ts: {}  pos: {}  branch B", p3, ev.ts, ev.pos);
            p2 = ev.pos;
        }
    }
}

async fn linear_search_2(
    mut pbr: PbFileReader,
    evq: RawEventsQuery,
    year: u32,
    p1: u64,
    p2: u64,
    payload_type: PayloadType,
) -> Result<PositionResult, Error> {
    debug!("linear_search_2  begin");
    // TODO improve.. either use additional file handle, or keep pbr in consistent state.
    let file = pbr.file();
    file.seek(SeekFrom::Start(p1 - 1)).await?;
    let mut buf = vec![0; (p2 - p1) as usize];
    file.read_exact(&mut buf).await?;
    let evs1 = parse_all_ts(p1 - 1, &buf, payload_type.clone(), year)?;
    for ev in evs1 {
        if ev.ts >= evq.range.beg {
            debug!("linear_search_2  Positioned {:?}", ev);
            pbr.reset_io(ev.pos).await?;
            let ret = PositionResult {
                state: PositionState::Positioned(ev.pos),
                pbr,
            };
            return Ok(ret);
        }
    }
    Err(Error::with_msg_no_trace("linear_search_2 failed"))
}

#[allow(unused)]
fn events_item_to_framable(ei: EventsItem) -> Result<Box<dyn Framable + Send>, Error> {
    match ei {
        EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::I32(h))) => {
            let range: NanoRange = err::todoval();
            let (tss, pulses, values) = h
                .tss
                .into_iter()
                .zip(h.pulses.into_iter())
                .zip(h.values.into_iter())
                .filter_map(|((t, p), v)| {
                    if t < range.beg || t >= range.end {
                        None
                    } else {
                        Some((t, p, v))
                    }
                })
                .fold((vec![], vec![], vec![]), |(mut a, mut b, mut c), (j, k, l)| {
                    a.push(j);
                    b.push(k);
                    c.push(l);
                    (a, b, c)
                });
            let b = ScalarEvents { tss, pulses, values };
            let b = Ok(StreamItem::DataItem(RangeCompletableItem::Data(b)));
            let ret = Box::new(b);
            Ok(ret)
        }
        _ => {
            error!("case not covered");
            Err(Error::with_msg_no_trace("todo"))
        }
    }
}

#[derive(Debug)]
pub struct DirAndPrefix {
    dir: PathBuf,
    prefix: String,
}

pub fn directory_for_channel_files(channel: &Channel, base_path: &PathBuf) -> Result<DirAndPrefix, ArchError> {
    // SARUN11/CVME/DBLM546/IOC_CPU_LOAD
    // SARUN11-CVME-DBLM546:IOC_CPU_LOAD
    let a: Vec<_> = channel.name.split("-").map(|s| s.split(":")).flatten().collect();
    let path = base_path;
    let path = a.iter().take(a.len() - 1).fold(path.clone(), |a, &x| a.join(x));
    let ret = DirAndPrefix {
        dir: path,
        prefix: a
            .last()
            .ok_or_else(|| ArchError::with_msg_no_trace("no prefix in file"))?
            .to_string(),
    };
    Ok(ret)
}

// The same channel-name in different data directories like "lts", "mts", .. are considered different channels.
pub async fn find_files_for_channel(base_path: &PathBuf, channel: &Channel) -> Result<Vec<PathBuf>, ArchError> {
    let mut ret = vec![];
    let chandir = directory_for_channel_files(channel, base_path)?;
    let mut rd = read_dir(&chandir.dir).await?;
    while let Some(en) = rd.next_entry().await? {
        let fns = en.file_name().to_string_lossy().into_owned();
        if fns.starts_with(&format!("{}:20", chandir.prefix)) && fns.ends_with(".pb") {
            ret.push(en.path());
        }
    }
    ret.sort_unstable();
    Ok(ret)
}

pub async fn channel_info(channel: &Channel, aa: &ArchiverAppliance) -> Result<ChannelInfo, Error> {
    let DirAndPrefix { dir, prefix } = directory_for_channel_files(channel, aa.data_base_paths.last().unwrap())?;
    let mut msgs = vec![];
    msgs.push(format!("path: {}", dir.to_string_lossy()));
    let mut scalar_type = None;
    let mut shape = None;
    let mut rd = read_dir(&dir)
        .await
        .map_err(|e| Error::with_msg(format!("Can not open directory {dir:?}  {e:?}")))?;
    while let Some(de) = rd.next_entry().await? {
        let s = de.file_name().to_string_lossy().into_owned();
        if s.starts_with(&prefix) && s.ends_with(".pb") {
            msgs.push(s);
            let f1 = File::open(de.path()).await?;
            let mut pbr = PbFileReader::new(f1).await?;
            msgs.push(format!("got header {}", pbr.channel_name()));
            let ev = pbr.read_msg().await;
            match ev {
                Ok(Some(item)) => {
                    let item = item.item;
                    msgs.push(format!("got event {:?}", item));
                    shape = Some(item.shape());
                    // These type mappings are defined by the protobuffer schema.
                    scalar_type = Some(item.scalar_type());
                    break;
                }
                Ok(None) => {
                    msgs.push(format!("can not read event"));
                }
                Err(e) => {
                    msgs.push(format!("can not read event {:?}", e));
                }
            }
            msgs.push(format!("got header {}", pbr.channel_name()));
        }
    }
    let shape = shape.ok_or_else(|| Error::with_msg(format!("could not determine shape  {:?}", msgs)))?;
    let scalar_type =
        scalar_type.ok_or_else(|| Error::with_msg(format!("could not determine scalar_type  {:?}", msgs)))?;
    let ret = ChannelInfo {
        scalar_type,
        byte_order: None,
        shape,
        msg: JsonValue::Array(msgs.into_iter().map(JsonValue::String).collect()),
    };
    Ok(ret)
}
