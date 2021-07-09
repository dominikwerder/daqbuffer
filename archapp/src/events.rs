use crate::parse::PbFileReader;
use crate::EventsItem;
use chrono::{TimeZone, Utc};
use err::Error;
use futures_core::Stream;
use items::eventvalues::EventValues;
use items::{Framable, RangeCompletableItem, StreamItem};
use netpod::log::*;
use netpod::query::RawEventsQuery;
use netpod::timeunits::DAY;
use netpod::{ArchiverAppliance, Channel, ChannelInfo, ScalarType, Shape};
use serde_json::Value as JsonValue;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::fs::{read_dir, File};

struct DataFilename {
    year: u32,
    month: u32,
}

fn parse_data_filename(s: &str) -> Result<DataFilename, Error> {
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

pub async fn make_event_pipe(
    evq: &RawEventsQuery,
    aa: &ArchiverAppliance,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error> {
    info!("make_event_pipe  {:?}", evq);
    let evq = evq.clone();
    let DirAndPrefix { dir, prefix } = directory_for_channel_files(&evq.channel, aa)?;
    let channel_info = channel_info(&evq.channel, aa).await?;
    //let dtbeg = Utc.timestamp((evq.range.beg / 1000000000) as i64, (evq.range.beg % 1000000000) as u32);
    let (tx, rx) = async_channel::bounded(16);
    let block1 = async move {
        let mut rd = tokio::fs::read_dir(&dir).await?;
        while let Some(de) = rd.next_entry().await? {
            let s = de.file_name().to_string_lossy().into_owned();
            if s.starts_with(&prefix) && s.ends_with(".pb") {
                match parse_data_filename(&s) {
                    Ok(df) => {
                        let ts0 = Utc.ymd(df.year as i32, df.month, 0).and_hms(0, 0, 0);
                        let ts1 = ts0.timestamp() as u64 * 1000000000 + ts0.timestamp_subsec_nanos() as u64;
                        if evq.range.beg < ts1 + DAY * 32 && evq.range.end > ts1 {
                            let f1 = File::open(de.path()).await?;
                            info!("opened {:?}", de.path());
                            let mut pbr = PbFileReader::new(f1).await;
                            pbr.read_header().await?;
                            loop {
                                match pbr.read_msg().await {
                                    Ok(ev) => match ev {
                                        EventsItem::ScalarDouble(h) => {
                                            //
                                            let (x, y) = h
                                                .tss
                                                .into_iter()
                                                .zip(h.values.into_iter())
                                                .filter_map(|(j, k)| {
                                                    if j < evq.range.beg || j >= evq.range.end {
                                                        None
                                                    } else {
                                                        Some((j, k))
                                                    }
                                                })
                                                .fold((vec![], vec![]), |(mut a, mut b), (j, k)| {
                                                    a.push(j);
                                                    b.push(k);
                                                    (a, b)
                                                });
                                            let b = EventValues { tss: x, values: y };
                                            let b = Ok(StreamItem::DataItem(RangeCompletableItem::Data(b)));
                                            tx.send(Box::new(b) as Box<dyn Framable>).await?;
                                        }
                                        _ => {
                                            //
                                            error!("case not covered");
                                            return Err(Error::with_msg_no_trace("todo"));
                                        }
                                    },
                                    Err(e) => {}
                                }
                            }
                        }
                    }
                    Err(e) => {}
                }
            }
        }
        Ok::<_, Error>(())
    };
    let block2 = async move {
        match block1.await {
            Ok(_) => {
                info!("block1 done ok");
            }
            Err(e) => {
                error!("{:?}", e);
            }
        }
    };
    tokio::task::spawn(block2);
    Ok(Box::pin(rx))
}

struct DirAndPrefix {
    dir: PathBuf,
    prefix: String,
}

fn directory_for_channel_files(channel: &Channel, aa: &ArchiverAppliance) -> Result<DirAndPrefix, Error> {
    // SARUN11/CVME/DBLM546/IOC_CPU_LOAD
    // SARUN11-CVME-DBLM546:IOC_CPU_LOAD
    let a: Vec<_> = channel.name.split("-").map(|s| s.split(":")).flatten().collect();
    let path = aa.data_base_path.clone();
    let path = a.iter().take(a.len() - 1).fold(path, |a, &x| a.join(x));
    let ret = DirAndPrefix {
        dir: path,
        prefix: a
            .last()
            .ok_or_else(|| Error::with_msg_no_trace("no prefix in file"))?
            .to_string(),
    };
    Ok(ret)
}

pub async fn channel_info(channel: &Channel, aa: &ArchiverAppliance) -> Result<ChannelInfo, Error> {
    let DirAndPrefix { dir, prefix } = directory_for_channel_files(channel, aa)?;
    let mut msgs = vec![];
    msgs.push(format!("path: {}", dir.to_string_lossy()));
    let mut scalar_type = None;
    let mut shape = None;
    let mut rd = read_dir(&dir).await?;
    while let Some(de) = rd.next_entry().await? {
        let s = de.file_name().to_string_lossy().into_owned();
        if s.starts_with(&prefix) && s.ends_with(".pb") {
            msgs.push(s);
            let f1 = File::open(de.path()).await?;
            let mut pbr = PbFileReader::new(f1).await;
            pbr.read_header().await?;
            msgs.push(format!("got header {}", pbr.channel_name()));
            let ev = pbr.read_msg().await;
            match ev {
                Ok(item) => {
                    msgs.push(format!("got event {:?}", item));
                    shape = Some(match &item {
                        EventsItem::ScalarByte(_) => Shape::Scalar,
                        EventsItem::ScalarShort(_) => Shape::Scalar,
                        EventsItem::ScalarInt(_) => Shape::Scalar,
                        EventsItem::ScalarFloat(_) => Shape::Scalar,
                        EventsItem::ScalarDouble(_) => Shape::Scalar,
                        EventsItem::WaveByte(item) => Shape::Wave(
                            item.vals
                                .first()
                                .ok_or_else(|| Error::with_msg_no_trace("empty event batch"))?
                                .len() as u32,
                        ),
                        EventsItem::WaveShort(item) => Shape::Wave(
                            item.vals
                                .first()
                                .ok_or_else(|| Error::with_msg_no_trace("empty event batch"))?
                                .len() as u32,
                        ),
                        EventsItem::WaveInt(item) => Shape::Wave(
                            item.vals
                                .first()
                                .ok_or_else(|| Error::with_msg_no_trace("empty event batch"))?
                                .len() as u32,
                        ),
                        EventsItem::WaveFloat(item) => Shape::Wave(
                            item.vals
                                .first()
                                .ok_or_else(|| Error::with_msg_no_trace("empty event batch"))?
                                .len() as u32,
                        ),
                        EventsItem::WaveDouble(item) => Shape::Wave(
                            item.vals
                                .first()
                                .ok_or_else(|| Error::with_msg_no_trace("empty event batch"))?
                                .len() as u32,
                        ),
                    });
                    // These type mappings are defined by the protobuffer schema.
                    scalar_type = Some(match item {
                        EventsItem::ScalarByte(_) => ScalarType::U8,
                        EventsItem::ScalarShort(_) => ScalarType::I32,
                        EventsItem::ScalarInt(_) => ScalarType::I32,
                        EventsItem::ScalarFloat(_) => ScalarType::F32,
                        EventsItem::ScalarDouble(_) => ScalarType::F64,
                        EventsItem::WaveByte(_) => ScalarType::U8,
                        EventsItem::WaveShort(_) => ScalarType::I32,
                        EventsItem::WaveInt(_) => ScalarType::I32,
                        EventsItem::WaveFloat(_) => ScalarType::F32,
                        EventsItem::WaveDouble(_) => ScalarType::F64,
                    });
                    break;
                }
                Err(e) => {
                    msgs.push(format!("can not read event! {:?}", e));
                }
            }
            msgs.push(format!("got header {}", pbr.channel_name()));
        }
    }
    let shape = shape.ok_or_else(|| Error::with_msg("could not determine shape"))?;
    let scalar_type = scalar_type.ok_or_else(|| Error::with_msg("could not determine scalar_type"))?;
    let ret = ChannelInfo {
        scalar_type,
        byte_order: None,
        shape,
        msg: JsonValue::Array(msgs.into_iter().map(JsonValue::String).collect()),
    };
    Ok(ret)
}
