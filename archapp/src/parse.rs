pub mod multi;

use crate::events::parse_data_filename;
use crate::generated::EPICSEvent::PayloadType;
use crate::unescape_archapp_msg;
use archapp_xc::*;
use async_channel::{bounded, Receiver};
use chrono::{TimeZone, Utc};
use err::{ErrStr, Error};
use futures_util::StreamExt;
use items::eventsitem::EventsItem;
use items::plainevents::{PlainEvents, ScalarPlainEvents, WavePlainEvents};
use items::scalarevents::ScalarEvents;
use items::waveevents::WaveEvents;
use netpod::log::*;
use netpod::{ArchiverAppliance, ChannelConfigQuery, ChannelConfigResponse};
use netpod::{Database, ScalarType, Shape};
use protobuf::Message;
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::fs::FileType;
use std::io::SeekFrom;
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub struct PbFileReader {
    file: File,
    buf: Vec<u8>,
    escbuf: Vec<u8>,
    wp: usize,
    rp: usize,
    off: u64,
    channel_name: String,
    payload_type: PayloadType,
    year: u32,
}

fn parse_scalar_byte(m: &[u8], year: u32) -> Result<EventsItem, Error> {
    let msg = crate::generated::EPICSEvent::ScalarByte::parse_from_bytes(m)
        .map_err(|_| Error::with_msg(format!("can not parse pb-type {}", "ScalarByte")))?;
    let mut t = ScalarEvents::<i8> {
        tss: vec![],
        values: vec![],
    };
    let yd = Utc.ymd(year as i32, 1, 1).and_hms(0, 0, 0);
    let ts = yd.timestamp() as u64 * 1000000000 + msg.get_secondsintoyear() as u64 * 1000000000 + msg.get_nano() as u64;
    let v = msg.get_val().first().map_or(0, |k| *k as i8);
    t.tss.push(ts);
    t.values.push(v);
    Ok(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::I8(t))))
}

macro_rules! scalar_parse {
    ($m:expr, $year:expr, $pbt:ident, $eit:ident, $evty:ident) => {{
        let msg = crate::generated::EPICSEvent::$pbt::parse_from_bytes($m)
            .map_err(|e| Error::with_msg(format!("can not parse pb-type {}  {:?}", stringify!($pbt), e)))?;
        let mut t = ScalarEvents::<$evty> {
            tss: vec![],
            values: vec![],
        };
        let yd = Utc.ymd($year as i32, 1, 1).and_hms(0, 0, 0);
        let ts =
            yd.timestamp() as u64 * 1000000000 + msg.get_secondsintoyear() as u64 * 1000000000 + msg.get_nano() as u64;
        let v = msg.get_val();
        //eprintln!("ts {}   val {}", ts, v);
        t.tss.push(ts);
        t.values.push(v as $evty);
        EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::$eit(t)))
    }};
}

macro_rules! wave_parse {
    ($m:expr, $year:expr, $pbt:ident, $eit:ident, $evty:ident) => {{
        let msg = crate::generated::EPICSEvent::$pbt::parse_from_bytes($m)
            .map_err(|_| Error::with_msg(format!("can not parse pb-type {}", stringify!($pbt))))?;
        let mut t = WaveEvents::<$evty> {
            tss: vec![],
            vals: vec![],
        };
        let yd = Utc.ymd($year as i32, 1, 1).and_hms(0, 0, 0);
        let ts =
            yd.timestamp() as u64 * 1000000000 + msg.get_secondsintoyear() as u64 * 1000000000 + msg.get_nano() as u64;
        let v = msg.get_val();
        t.tss.push(ts);
        t.vals.push(v.into_iter().map(|&x| x as $evty).collect());
        EventsItem::Plain(PlainEvents::Wave(WavePlainEvents::$eit(t)))
    }};
}

const MIN_BUF_FILL: usize = 1024 * 64;

pub struct ReadMessageResult {
    pub pos: u64,
    pub item: EventsItem,
}

impl PbFileReader {
    pub async fn new(file: File) -> Result<Self, Error> {
        let mut ret = Self {
            file,
            buf: vec![0; MIN_BUF_FILL * 4],
            escbuf: vec![],
            wp: 0,
            rp: 0,
            // TODO check usage of `off`.
            // It should represent the absolute position where the 1st byte of `buf` is located
            // in the file, independent of `wp` or `rp`.
            off: 0,
            channel_name: String::new(),
            payload_type: PayloadType::V4_GENERIC_BYTES,
            year: 0,
        };
        ret.read_header().await?;
        Ok(ret)
    }

    pub fn into_file(self) -> File {
        self.file
    }

    pub fn file(&mut self) -> &mut File {
        &mut self.file
    }

    pub async fn reset_io(&mut self, off: u64) -> Result<(), Error> {
        self.file().seek(SeekFrom::Start(off)).await?;
        self.wp = 0;
        self.rp = 0;
        self.off = off;
        Ok(())
    }

    pub async fn read_header(&mut self) -> Result<(), Error> {
        self.fill_buf().await?;
        let k = self.find_next_nl()?;
        trace!("read_header  abspos {}  packet len {}", self.abspos(), k + 1 - self.rp);
        let buf = &mut self.buf;
        let m = unescape_archapp_msg(&buf[self.rp..k], mem::replace(&mut self.escbuf, vec![]))?;
        self.escbuf = m;
        let payload_info = crate::generated::EPICSEvent::PayloadInfo::parse_from_bytes(&self.escbuf)
            .map_err(|_| Error::with_msg("can not parse PayloadInfo"))?;
        self.channel_name = payload_info.get_pvname().into();
        self.payload_type = payload_info.get_field_type();
        self.year = payload_info.get_year() as u32;
        self.rp = k + 1;
        Ok(())
    }

    pub async fn read_msg(&mut self) -> Result<Option<ReadMessageResult>, Error> {
        self.fill_buf().await?;
        let k = if let Ok(k) = self.find_next_nl() {
            k
        } else {
            debug!("Can not find a next NL");
            return Ok(None);
        };
        //info!("read_msg  abspos {}  packet len {}", self.abspos(), k + 1 - self.rp);
        let buf = &mut self.buf;
        let m = mem::replace(&mut self.escbuf, vec![]);
        let m = unescape_archapp_msg(&buf[self.rp..k], m)?;
        self.escbuf = m;
        let ei = Self::parse_buffer(&self.escbuf, self.payload_type.clone(), self.year)?;
        let ret = ReadMessageResult {
            pos: self.off + self.rp as u64,
            item: ei,
        };
        self.rp = k + 1;
        Ok(Some(ret))
    }

    pub fn parse_buffer(m: &[u8], payload_type: PayloadType, year: u32) -> Result<EventsItem, Error> {
        use PayloadType::*;
        let ei = match payload_type {
            SCALAR_BYTE => parse_scalar_byte(m, year)?,
            SCALAR_ENUM => {
                scalar_parse!(m, year, ScalarEnum, I32, i32)
            }
            SCALAR_SHORT => {
                scalar_parse!(m, year, ScalarShort, I16, i16)
            }
            SCALAR_INT => {
                scalar_parse!(m, year, ScalarInt, I32, i32)
            }
            SCALAR_FLOAT => {
                scalar_parse!(m, year, ScalarFloat, F32, f32)
            }
            SCALAR_DOUBLE => {
                scalar_parse!(m, year, ScalarDouble, F64, f64)
            }
            WAVEFORM_BYTE => {
                wave_parse!(m, year, VectorChar, I8, i8)
            }
            WAVEFORM_SHORT => {
                wave_parse!(m, year, VectorShort, I16, i16)
            }
            WAVEFORM_ENUM => {
                wave_parse!(m, year, VectorEnum, I32, i32)
            }
            WAVEFORM_INT => {
                wave_parse!(m, year, VectorInt, I32, i32)
            }
            WAVEFORM_FLOAT => {
                wave_parse!(m, year, VectorFloat, F32, f32)
            }
            WAVEFORM_DOUBLE => {
                wave_parse!(m, year, VectorDouble, F64, f64)
            }
            SCALAR_STRING | WAVEFORM_STRING | V4_GENERIC_BYTES => {
                return Err(Error::with_msg_no_trace(format!("not supported: {:?}", payload_type)));
            }
        };
        Ok(ei)
    }

    pub fn abspos(&self) -> u64 {
        self.off + self.rp as u64
    }

    async fn fill_buf(&mut self) -> Result<(), Error> {
        if self.wp - self.rp >= MIN_BUF_FILL {
            return Ok(());
        }
        if self.rp + MIN_BUF_FILL >= self.buf.len() {
            let n = self.wp - self.rp;
            self.buf.copy_within(self.rp..self.rp + n, 0);
            self.off += self.rp as u64;
            self.rp = 0;
            self.wp = n;
        }
        let buf = &mut self.buf;
        loop {
            let sl = &mut buf[self.wp..];
            if sl.len() == 0 {
                break;
            }
            let n = self.file.read(sl).await?;
            if n == 0 {
                break;
            } else {
                self.wp += n;
            }
        }
        Ok(())
    }

    fn find_next_nl(&self) -> Result<usize, Error> {
        let buf = &self.buf;
        let mut k = self.rp;
        while k < self.wp && buf[k] != 0xa {
            k += 1;
        }
        if k == self.wp {
            // TODO test whether with_msg_no_trace makes difference.
            return Err(Error::with_msg("no nl in pb file"));
        }
        Ok(k)
    }

    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }

    pub fn payload_type(&self) -> &PayloadType {
        &self.payload_type
    }
}

#[derive(Serialize)]
pub struct EpicsEventPayloadInfo {
    headers: Vec<(String, String)>,
    year: i32,
    pvname: String,
    datatype: String,
    ts0: u32,
    val0: f32,
}

// TODO remove in favor of PbFileRead
async fn _read_pb_file(mut f1: File) -> Result<(EpicsEventPayloadInfo, File), Error> {
    let mut buf = vec![0; 1024 * 4];
    {
        let mut i1 = 0;
        loop {
            let n = f1.read(&mut buf[i1..]).await?;
            if n == 0 {
                break;
            }
            i1 += n;
            if i1 >= buf.len() {
                break;
            }
        }
    }
    let mut j1 = 0;
    let mut payload_info = crate::generated::EPICSEvent::PayloadInfo::new();
    let mut z = EpicsEventPayloadInfo {
        pvname: String::new(),
        headers: vec![],
        year: 0,
        datatype: String::new(),
        ts0: 0,
        val0: 0.0,
    };
    loop {
        let mut i2 = usize::MAX;
        for (i1, &k) in buf[j1..].iter().enumerate() {
            if k == 0xa {
                i2 = j1 + i1;
                break;
            }
        }
        if i2 != usize::MAX {
            //info!("got NL  {} .. {}", j1, i2);
            let m = unescape_archapp_msg(&buf[j1..i2], vec![])?;
            if j1 == 0 {
                payload_info = crate::generated::EPICSEvent::PayloadInfo::parse_from_bytes(&m)
                    .map_err(|_| Error::with_msg("can not parse PayloadInfo"))?;
                //info!("got payload_info: {:?}", payload_info);
                z = EpicsEventPayloadInfo {
                    headers: payload_info
                        .get_headers()
                        .iter()
                        .map(|j| (j.get_name().to_string(), j.get_val().to_string()))
                        .collect(),
                    year: payload_info.get_year(),
                    pvname: payload_info.get_pvname().into(),
                    datatype: String::new(),
                    ts0: 0,
                    val0: 0.0,
                };
            } else {
                let ft = payload_info.get_field_type();
                {
                    use crate::generated::EPICSEvent::PayloadType::*;
                    let (ts, val) = match ft {
                        SCALAR_BYTE => {
                            let d = crate::generated::EPICSEvent::ScalarByte::parse_from_bytes(&m)
                                .map_err(|_| Error::with_msg("can not parse EPICSEvent::ScalarByte"))?;
                            (d.get_secondsintoyear(), d.get_val()[0] as f32)
                        }
                        SCALAR_SHORT => {
                            let d = crate::generated::EPICSEvent::ScalarShort::parse_from_bytes(&m)
                                .map_err(|_| Error::with_msg("can not parse EPICSEvent::ScalarShort"))?;
                            (d.get_secondsintoyear(), d.get_val() as f32)
                        }
                        SCALAR_INT => {
                            let d = crate::generated::EPICSEvent::ScalarInt::parse_from_bytes(&m)
                                .map_err(|_| Error::with_msg("can not parse EPICSEvent::ScalarInt"))?;
                            (d.get_secondsintoyear(), d.get_val() as f32)
                        }
                        SCALAR_FLOAT => {
                            let d = crate::generated::EPICSEvent::ScalarFloat::parse_from_bytes(&m)
                                .map_err(|_| Error::with_msg("can not parse EPICSEvent::ScalarFloat"))?;
                            (d.get_secondsintoyear(), d.get_val() as f32)
                        }
                        SCALAR_DOUBLE => {
                            let d = crate::generated::EPICSEvent::ScalarDouble::parse_from_bytes(&m)
                                .map_err(|_| Error::with_msg("can not parse EPICSEvent::ScalarDouble"))?;
                            (d.get_secondsintoyear(), d.get_val() as f32)
                        }
                        WAVEFORM_FLOAT => {
                            let d = crate::generated::EPICSEvent::VectorFloat::parse_from_bytes(&m)
                                .map_err(|_| Error::with_msg("can not parse EPICSEvent::VectorFloat"))?;
                            (d.get_secondsintoyear(), d.get_val()[0] as f32)
                        }
                        WAVEFORM_DOUBLE => {
                            let d = crate::generated::EPICSEvent::VectorDouble::parse_from_bytes(&m)
                                .map_err(|_| Error::with_msg("can not parse EPICSEvent::VectorDouble"))?;
                            (d.get_secondsintoyear(), d.get_val()[0] as f32)
                        }
                        _ => (0, 0.0),
                    };
                    z.datatype = format!("{:?}", ft);
                    z.ts0 = ts;
                    z.val0 = val;
                    return Ok((z, f1));
                }
            }
        } else {
            //info!("no more packets");
            break;
        }
        j1 = i2 + 1;
    }
    Err(Error::with_msg(format!("no data found in file")))
}

struct LruCache {
    map: BTreeMap<String, Instant>,
}

impl LruCache {
    fn new() -> Self {
        Self { map: BTreeMap::new() }
    }

    fn insert(&mut self, key: &str) {
        self.map.insert(key.into(), Instant::now());
        if self.map.len() > 2000 {
            let mut tss: Vec<Instant> = self.map.values().map(|j| j.clone()).collect();
            tss.sort_unstable();
            let thr = tss[1500];
            let m1 = std::mem::replace(&mut self.map, BTreeMap::new());
            self.map = m1.into_iter().filter(|(_j, k)| k > &thr).collect();
        }
    }

    fn query(&self, key: &str) -> bool {
        self.map.get(key).map_or(false, |_| true)
    }
}

#[derive(Debug)]
#[allow(unused)]
pub struct DiscoveredDatafile {
    path: PathBuf,
    channel_name: String,
    variant_name: String,
    scalar_type: ScalarType,
    shape: Shape,
}

pub async fn scan_files_inner(
    pairs: BTreeMap<String, String>,
    data_base_paths: Vec<PathBuf>,
) -> Result<Receiver<Result<DiscoveredDatafile, Error>>, Error> {
    let _ = pairs;
    let (tx, rx) = bounded(16);
    let tx = Arc::new(tx);
    let tx2 = tx.clone();
    let block1 = async move {
        let mut lru = LruCache::new();
        struct PE {
            path: PathBuf,
            fty: FileType,
        }
        let proot = data_base_paths.last().unwrap().clone();
        let proots = proot.to_str().unwrap().to_string();
        // TODO factor out to automatically add path to error.
        let meta = tokio::fs::metadata(&proot)
            .await
            .map_err(|e| Error::with_msg(format!("can not open {proot:?}  {e:?}")))?;
        let mut paths = VecDeque::new();
        paths.push_back(PE {
            path: proot,
            fty: meta.file_type(),
        });
        loop {
            if let Some(pe) = paths.pop_back() {
                if pe.fty.is_dir() {
                    // TODO factor out to automatically add path to error.
                    let mut rd = tokio::fs::read_dir(&pe.path)
                        .await
                        .map_err(|e| Error::with_msg(format!("can not open {:?}  {e:?}", pe.path)))?;
                    loop {
                        match rd.next_entry().await {
                            Ok(item) => match item {
                                Some(item) => {
                                    paths.push_back(PE {
                                        path: item.path(),
                                        fty: item.file_type().await?,
                                    });
                                }
                                None => {
                                    break;
                                }
                            },
                            Err(e) => {
                                tx.send(Err(e.into())).await.errstr()?;
                            }
                        }
                    }
                } else if pe.fty.is_file() {
                    //tx.send(Ok(Box::new(path.clone()) as RT1)).await?;
                    let fns = pe.path.to_str().ok_or_else(|| Error::with_msg("invalid path string"))?;
                    if let Ok(_fnp) = parse_data_filename(&fns) {
                        //tx.send(Ok(Box::new(serde_json::to_value(fns)?) as ItemSerBox)).await?;
                        let channel_path = &fns[proots.len() + 1..fns.len() - 11];
                        if !lru.query(channel_path) {
                            let f3 = tokio::fs::File::open(&pe.path)
                                .await
                                .map_err(|e| Error::with_msg(format!("can not open {:?}  {e:?}", pe.path)))?;
                            let mut pbr = PbFileReader::new(f3).await?;
                            let normalized_channel_name = {
                                let pvn = pbr.channel_name().replace("-", "/");
                                pvn.replace(":", "/")
                            };
                            if channel_path != normalized_channel_name {
                                let msg = format!("{} - {}", channel_path, normalized_channel_name);
                                warn!("{}", msg);
                                tx.send(Err(Error::with_msg(format!("MISMATCH --------------------"))))
                                    .await
                                    .errstr()?;
                            } else {
                                if let Ok(Some(msg)) = pbr.read_msg().await {
                                    lru.insert(channel_path);
                                    let item = DiscoveredDatafile {
                                        path: pe.path,
                                        channel_name: pbr.channel_name().into(),
                                        variant_name: msg.item.variant_name(),
                                        scalar_type: msg.item.type_info().0,
                                        shape: msg.item.type_info().1,
                                    };
                                    tx.send(Ok(item)).await.errstr()?;
                                }
                            }
                        }
                    }
                }
            } else {
                break;
            }
        }
        Ok::<_, Error>(())
    };
    let block2 = async move {
        match block1.await {
            Ok(_) => {}
            Err(e) => match tx2.send(Err(e)).await {
                Ok(_) => {}
                Err(e) => {
                    error!("can not deliver error through channel: {:?}", e);
                }
            },
        }
    };
    tokio::spawn(block2);
    Ok(rx)
}

pub async fn scan_files_msgs(
    pairs: BTreeMap<String, String>,
    data_base_paths: Vec<PathBuf>,
) -> Result<Receiver<Result<ItemSerBox, Error>>, Error> {
    let (tx, rx) = bounded(16);
    let tx = Arc::new(tx);
    let tx2 = tx.clone();
    let block1 = async move {
        let mut inp = scan_files_inner(pairs, data_base_paths).await?;
        while let Some(item) = inp.next().await {
            let item = item?;
            let msg = format!("{item:?}");
            tx.send(Ok(Box::new(serde_json::to_value(msg)?) as ItemSerBox))
                .await
                .errstr()?;
        }
        Ok(())
    };
    let block2 = async move {
        match block1.await {
            Ok(_) => {}
            Err(e) => match tx2.send(Err(e)).await {
                Ok(_) => {}
                Err(e) => {
                    error!("can not deliver error through channel: {:?}", e);
                }
            },
        }
    };
    tokio::spawn(block2);
    Ok(rx)
}

pub async fn scan_files_to_database(
    pairs: BTreeMap<String, String>,
    data_base_paths: Vec<PathBuf>,
    database: Database,
) -> Result<Receiver<Result<ItemSerBox, Error>>, Error> {
    let (tx, rx) = bounded(16);
    let tx = Arc::new(tx);
    let tx2 = tx.clone();
    let block1 = async move {
        let dbc = dbconn::create_connection(&database).await?;
        let mut inp = scan_files_inner(pairs, data_base_paths).await?;
        while let Some(item) = inp.next().await {
            let item = item?;
            let sql = "select rowid from channels where name = $1";
            let rows = dbc.query(sql, &[&item.channel_name]).await.errstr()?;
            if rows.len() == 0 {
                let sql = "insert into channels (name, config) values ($1, $2) on conflict do nothing returning rowid";
                let cfg = serde_json::json!({
                    "scalarType":item.scalar_type.to_bsread_str(),
                    "shape":item.shape,
                });
                let rows = dbc.query(sql, &[&item.channel_name, &cfg]).await.errstr()?;
                if let Some(row) = rows.last() {
                    let rowid: i64 = row.get(0);
                    let msg = format!("insert done  rowid {rowid}  {item:?}");
                    let msg = Box::new(serde_json::to_value(msg)?) as ItemSerBox;
                    tx.send(Ok(msg)).await.errstr()?;
                }
            } else {
                // TODO update channel config if needed
            }
        }
        Ok(())
    };
    let block2 = async move {
        match block1.await {
            Ok(_) => {}
            Err(e) => match tx2.send(Err(e)).await {
                Ok(_) => {}
                Err(e) => {
                    error!("can not deliver error through channel: {:?}", e);
                }
            },
        }
    };
    tokio::spawn(block2);
    Ok(rx)
}

pub async fn channel_config(q: &ChannelConfigQuery, aa: &ArchiverAppliance) -> Result<ChannelConfigResponse, Error> {
    let ci = crate::events::channel_info(&q.channel, aa).await?;
    let ret = ChannelConfigResponse {
        channel: q.channel.clone(),
        scalar_type: ci.scalar_type,
        byte_order: ci.byte_order,
        shape: ci.shape,
    };
    Ok(ret)
}
