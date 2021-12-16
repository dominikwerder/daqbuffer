use archapp::events::PositionState;
use archapp::parse::PbFileReader;
use bytes::BufMut;
use chrono::{TimeZone, Utc};
use err::Error;
use items::plainevents::{PlainEvents, ScalarPlainEvents, WavePlainEvents};
use netpod::log::*;
use netpod::query::RawEventsQuery;
use netpod::timeunits::*;
use netpod::{AggKind, Channel, HasScalarType, HasShape, NanoRange, Nanos, ScalarType, Shape};
use parse::channelconfig::Config;
use std::fmt;
use std::io::SeekFrom;
use std::mem::take;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

trait WritableValue: fmt::Debug {
    fn put_value(&self, buf: &mut Vec<u8>);
}

impl WritableValue for u32 {
    fn put_value(&self, buf: &mut Vec<u8>) {
        buf.put_u32_le(*self);
    }
}

impl WritableValue for i8 {
    fn put_value(&self, buf: &mut Vec<u8>) {
        buf.put_i8(*self);
    }
}

impl WritableValue for i16 {
    fn put_value(&self, buf: &mut Vec<u8>) {
        buf.put_i16_le(*self);
    }
}

impl WritableValue for i32 {
    fn put_value(&self, buf: &mut Vec<u8>) {
        buf.put_i32_le(*self);
    }
}

impl WritableValue for f32 {
    fn put_value(&self, buf: &mut Vec<u8>) {
        buf.put_f32_le(*self);
    }
}

impl WritableValue for f64 {
    fn put_value(&self, buf: &mut Vec<u8>) {
        buf.put_f64_le(*self);
    }
}

struct DataWriter {
    output_dir: PathBuf,
    kspre: String,
    channel: Channel,
    bs: Nanos,
    tb: u64,
    datafile: Option<File>,
    indexfile: Option<File>,
    wpos: u64,
    buf1: Vec<u8>,
}

impl DataWriter {
    async fn new(output_dir: PathBuf, kspre: String, channel: Channel, bs: Nanos) -> Result<Self, Error> {
        let ret = Self {
            output_dir,
            kspre,
            channel,
            bs,
            tb: u64::MAX,
            datafile: None,
            indexfile: None,
            wpos: 0,
            buf1: vec![0; 1024 * 1024],
        };
        Ok(ret)
    }

    async fn write_item(&mut self, item: &PlainEvents) -> Result<(), Error> {
        match item {
            PlainEvents::Scalar(item) => match item {
                ScalarPlainEvents::U32(events) => {
                    self.write_events(2, ScalarType::U32, &events.tss, &events.values)
                        .await?;
                }
                ScalarPlainEvents::I8(events) => {
                    self.write_events(2, ScalarType::I8, &events.tss, &events.values)
                        .await?;
                }
                ScalarPlainEvents::I16(events) => {
                    self.write_events(2, ScalarType::I16, &events.tss, &events.values)
                        .await?;
                }
                ScalarPlainEvents::I32(events) => {
                    self.write_events(2, ScalarType::I32, &events.tss, &events.values)
                        .await?;
                }
                ScalarPlainEvents::F32(events) => {
                    self.write_events(2, ScalarType::F32, &events.tss, &events.values)
                        .await?;
                }
                ScalarPlainEvents::F64(events) => {
                    self.write_events(2, ScalarType::F64, &events.tss, &events.values)
                        .await?;
                }
                _ => todo!(),
            },
            PlainEvents::Wave(item) => match item {
                WavePlainEvents::F64(_events) => {
                    todo!()
                }
                _ => todo!(),
            },
        }
        Ok(())
    }

    async fn write_events<T: WritableValue>(
        &mut self,
        ks: u32,
        scalar_type: ScalarType,
        tss: &Vec<u64>,
        vals: &Vec<T>,
    ) -> Result<(), Error> {
        let split = 0;
        assert_eq!(tss.len(), vals.len());
        for i in 0..tss.len() {
            let ts = tss[i];
            let tb = ts / self.bs.ns;
            if tb != self.tb {
                let tbdate = chrono::Utc.timestamp((tb * (self.bs.ns / SEC)) as i64, 0);
                eprintln!("Create directory for timebin {}", tbdate);
                let p1 = self.output_dir.join(format!("{}_{}", self.kspre, ks));
                let p2 = p1.join(self.channel.name());
                let p3 = p2.join(format!("{:019}", tb));
                let p4 = p3.join(format!("{:010}", split));
                let p5 = p4.join(format!("{:019}_00000_Data", self.bs.ns / MS));
                let p6 = p4.join(format!("{:019}_00000_Data_Index", self.bs.ns / MS));
                tokio::fs::create_dir_all(&p4).await.map_err(|e| {
                    error!("Can not create {:?}", p4);
                    e
                })?;
                let mut file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&p5)
                    .await
                    .map_err(|e| {
                        error!("can not create new file {:?}", p5);
                        e
                    })?;
                file.write_all(&0u16.to_be_bytes()).await?;
                let chs = self.channel.name().as_bytes();
                let len1 = (chs.len() + 8) as u32;
                file.write_all(&len1.to_be_bytes()).await?;
                file.write_all(chs).await?;
                file.write_all(&len1.to_be_bytes()).await?;
                self.wpos = 10 + chs.len() as u64;
                self.datafile = Some(file);
                if ks == 3 {
                    let mut file = OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(&p6)
                        .await
                        .map_err(|e| {
                            error!("can not create new file {:?}", p6);
                            e
                        })?;
                    file.write_all(&0u16.to_be_bytes()).await?;
                    self.indexfile = Some(file);
                }
                self.tb = tb;
            }
            let file = self.datafile.as_mut().unwrap();
            let mut buf = take(&mut self.buf1);
            buf.clear();
            buf.put_i32(0);
            buf.put_u64(0);
            buf.put_u64(ts);
            buf.put_u64(0);
            buf.put_u64(0);
            // Status, Severity
            buf.put_u8(0);
            buf.put_u8(0);
            buf.put_i32(-1);
            let flags = 0;
            buf.put_u8(flags);
            buf.put_u8(scalar_type.index());
            vals[i].put_value(&mut buf);
            buf.put_i32(0);
            let len1 = buf.len();
            buf[0..4].as_mut().put_u32(len1 as u32);
            buf[len1 - 4..len1].as_mut().put_u32(len1 as u32);
            file.write_all(&buf).await?;
            self.buf1 = buf;
            if ks == 3 {
                let file = self.indexfile.as_mut().unwrap();
                let mut buf = take(&mut self.buf1);
                buf.clear();
                buf.put_u64(ts);
                buf.put_u64(self.wpos);
                file.write_all(&buf).await?;
                self.buf1 = buf;
            }
            self.wpos += len1 as u64;
        }
        Ok(())
    }

    async fn write_config(&mut self, config: &Config) -> Result<(), Error> {
        eprintln!("Create directory for channel config");
        let p1 = self.output_dir.join("config").join(self.channel.name()).join("latest");
        tokio::fs::create_dir_all(&p1).await.map_err(|e| {
            error!("Can not create {:?}", p1);
            e
        })?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(p1.join("00000_Config"))
            .await
            .map_err(|e| {
                error!("can not create config file in {:?}", p1);
                e
            })?;
        let mut buf = take(&mut self.buf1);
        {
            buf.clear();
            buf.put_u16(0);
            file.write_all(&buf).await?;
        }
        {
            buf.clear();
            let chs = self.channel.name().as_bytes();
            let len1 = (chs.len() + 8) as u32;
            buf.put_u32(len1);
            buf.put_slice(chs);
            buf.put_u32(len1);
            //let len1 = buf.len();
            //buf[0..4].as_mut().put_u32(len1 as u32);
            //buf[len1 - 4..len1].as_mut().put_u32(len1 as u32);
            file.write_all(&buf).await?;
        }
        {
            let e = &config.entries[0];
            buf.clear();
            buf.put_u32(0);
            buf.put_u64(0);
            buf.put_u64(0);
            buf.put_i32(e.ks);
            buf.put_u64(e.bs.ns / MS);
            buf.put_i32(e.split_count);
            buf.put_i32(e.status);
            buf.put_i8(e.bb);
            buf.put_i32(e.modulo);
            buf.put_i32(e.offset);
            buf.put_i16(e.precision);
            let dtlen = 0;
            buf.put_i32(dtlen);
            let flags = 0;
            buf.put_u8(flags);
            buf.put_u8(e.scalar_type.index());
            if false {
                // is shaped?
                buf.put_u8(1);
                buf.put_u32(16);
            }
            buf.put_i32(-1);
            buf.put_i32(-1);
            buf.put_i32(-1);
            buf.put_i32(-1);
            buf.put_i32(-1);
            buf.put_u32(0);
            let len1 = buf.len();
            buf[0..4].as_mut().put_u32(len1 as u32);
            buf[len1 - 4..len1].as_mut().put_u32(len1 as u32);
            file.write_all(&buf).await?;
        }
        self.buf1 = buf;
        Ok(())
    }
}

impl Drop for DataWriter {
    fn drop(&mut self) {
        let indexfile = self.indexfile.take();
        let datafile = self.datafile.take();
        tokio::task::spawn(async move {
            match indexfile {
                Some(mut file) => {
                    let _ = file.flush().await;
                }
                None => {}
            }
            match datafile {
                Some(mut file) => {
                    let _ = file.flush().await;
                }
                None => {}
            }
        });
    }
}

#[derive(Clone, Debug)]
pub struct ConvertParams {
    pub keyspace_prefix: String,
    pub channel_name: String,
    pub input_dir: PathBuf,
    pub output_dir: PathBuf,
    pub time_bin_size: Nanos,
}

pub async fn convert(convert_params: ConvertParams) -> Result<(), Error> {
    let _ = tokio::fs::create_dir(&convert_params.output_dir).await;
    let meta = tokio::fs::metadata(&convert_params.output_dir).await?;
    if !meta.is_dir() {
        return Err(Error::from_string(format!(
            "Given output path is not a directory: {:?}",
            convert_params.output_dir
        )));
    }
    let bs = convert_params.time_bin_size.clone();
    let mut channel_config: Option<Config> = None;
    let channel = Channel {
        backend: String::new(),
        name: convert_params.channel_name.into(),
    };
    let mut data_writer = DataWriter::new(
        convert_params.output_dir,
        convert_params.keyspace_prefix.into(),
        channel.clone(),
        bs.clone(),
    )
    .await?;
    let chandir = archapp::events::directory_for_channel_files(&channel, &convert_params.input_dir)?;
    eprintln!("Looking for files in: {:?}", chandir);
    let files = archapp::events::find_files_for_channel(&convert_params.input_dir, &channel).await?;
    let mut evstot = 0;
    for file in files {
        eprintln!("Try to open {:?}", file);
        let fni = archapp::events::parse_data_filename(file.to_str().unwrap())?;
        debug!("fni: {:?}", fni);
        let ts0 = Utc.ymd(fni.year as i32, fni.month, 1).and_hms(0, 0, 0);
        let ts1 = ts0.timestamp() as u64 * SEC + ts0.timestamp_subsec_nanos() as u64;
        let _ = ts1;
        let mut f1 = File::open(&file).await?;
        let _flen = f1.seek(SeekFrom::End(0)).await?;
        f1.seek(SeekFrom::Start(0)).await?;
        let pbr = PbFileReader::new(f1).await?;
        eprintln!(
            "PBR file header channel name: {:?}  data type: {:?}",
            pbr.channel_name(),
            pbr.payload_type()
        );
        debug!("channel name in pbr file: {:?}", pbr.channel_name());
        debug!("data type in file: {:?}", pbr.payload_type());
        if pbr.channel_name() != channel.name() {
            return Err(Error::with_msg(format!(
                "channel name mismatch: {:?} vs {:?}",
                pbr.channel_name(),
                channel.name()
            )));
        }
        let evq = RawEventsQuery {
            channel: channel.clone(),
            range: NanoRange {
                beg: u64::MIN,
                end: u64::MAX,
            },
            agg_kind: AggKind::Plain,
            disk_io_buffer_size: 1024 * 4,
            do_decompress: true,
        };
        let f1 = pbr.into_file();
        // TODO can the positioning-logic maybe re-use the pbr?
        let z = archapp::events::position_file_for_evq(f1, evq.clone(), fni.year).await?;
        if let PositionState::Positioned(pos) = z.state {
            let mut pbr = z.pbr;
            assert_eq!(pos, pbr.abspos());
            let mut i1 = 0;
            let mut repnext = u64::MAX;
            loop {
                match pbr.read_msg().await {
                    Ok(Some(ei)) => {
                        use items::{WithLen, WithTimestamps};
                        let ei = ei.item;
                        if ei.is_wave() {
                            eprintln!("ERROR wave channels are not yet fully supported");
                            return Ok(());
                        }
                        if ei.len() > 0 {
                            let scalar_type = ei.scalar_type();
                            let shape = match &ei {
                                items::eventsitem::EventsItem::Plain(k) => match k.shape() {
                                    Shape::Scalar => None,
                                    Shape::Wave(n) => Some(vec![n]),
                                    Shape::Image(..) => panic!(),
                                },
                                items::eventsitem::EventsItem::XBinnedEvents(_) => panic!(),
                            };
                            if let Some(conf) = &channel_config {
                                if scalar_type != conf.entries[0].scalar_type {
                                    let msg = format!(
                                        "unexpected type: {:?} vs {:?}",
                                        scalar_type, conf.entries[0].scalar_type
                                    );
                                    return Err(Error::with_msg_no_trace(msg));
                                }
                                if shape != conf.entries[0].shape {
                                    let msg = format!("unexpected shape: {:?} vs {:?}", shape, conf.entries[0].shape);
                                    return Err(Error::with_msg_no_trace(msg));
                                }
                            }
                            if channel_config.is_none() {
                                let ks = if ei.is_wave() { 3 } else { 2 };
                                let e = parse::channelconfig::ConfigEntry {
                                    ts: 0,
                                    pulse: 0,
                                    ks,
                                    bs: bs.clone(),
                                    split_count: 1,
                                    status: 0,
                                    bb: 0,
                                    modulo: 0,
                                    offset: 0,
                                    precision: 0,
                                    scalar_type: scalar_type,
                                    is_compressed: false,
                                    is_shaped: false,
                                    is_array: false,
                                    byte_order: netpod::ByteOrder::LE,
                                    compression_method: None,
                                    shape,
                                    source_name: None,
                                    unit: None,
                                    description: None,
                                    optional_fields: None,
                                    value_converter: None,
                                };
                                let k = parse::channelconfig::Config {
                                    format_version: 0,
                                    channel_name: channel.name().into(),
                                    entries: vec![e],
                                };
                                channel_config = Some(k);
                            }
                            match &ei {
                                items::eventsitem::EventsItem::Plain(item) => {
                                    data_writer.write_item(item).await?;
                                }
                                items::eventsitem::EventsItem::XBinnedEvents(_) => {
                                    panic!()
                                }
                            }
                        }
                        let tslast = if ei.len() > 0 { Some(ei.ts(ei.len() - 1)) } else { None };
                        if i1 == repnext {
                            debug!("read msg from file {}  len {}  tslast {:?}", i1, ei.len(), tslast);
                            repnext = 1 + 4 * repnext / 3;
                        }
                        i1 += 1;
                        if false {
                            ei.x_aggregate(&evq.agg_kind);
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
            debug!("read total {} events from the last file", i1);
            evstot += i1;
        } else {
            error!("Position fail.");
        }
    }
    eprintln!("Total number of events converted: {}", evstot);
    data_writer.write_config(channel_config.as_ref().unwrap()).await?;
    Ok(())
}
