use crate::err::Error;
use crate::response;
use bytes::BufMut;
use bytes::{Buf, BytesMut};
use futures_util::stream::FuturesOrdered;
use futures_util::FutureExt;
use http::{Method, StatusCode, Uri};
use hyper::{Body, Request, Response};
use netpod::log::*;
use netpod::AppendToUrl;
use netpod::FromUrl;
use netpod::HasBackend;
use netpod::HasTimeout;
use netpod::NodeConfigCached;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use std::{io::SeekFrom, path::PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::task::JoinHandle;
use url::Url;

pub struct MapPulseHisto {
    _pulse: u64,
    _tss: Vec<u64>,
    _counts: Vec<u64>,
}

const MAP_INDEX_FULL_URL_PREFIX: &'static str = "/api/1/map/index/full/";
const _MAP_INDEX_FAST_URL_PREFIX: &'static str = "/api/1/map/index/fast/";
const MAP_PULSE_HISTO_URL_PREFIX: &'static str = "/api/1/map/pulse/histo/";
const MAP_PULSE_URL_PREFIX: &'static str = "/api/1/map/pulse/";
const MAP_PULSE_LOCAL_URL_PREFIX: &'static str = "/api/1/map/pulse/local/";
const MAP_PULSE_MARK_CLOSED_URL_PREFIX: &'static str = "/api/1/map/pulse/mark/closed/";
const API_4_MAP_PULSE_URL_PREFIX: &'static str = "/api/4/map/pulse/";

async fn make_tables(node_config: &NodeConfigCached) -> Result<(), Error> {
    let conn = dbconn::create_connection(&node_config.node_config.cluster.database).await?;
    let sql = "set client_min_messages = 'warning'";
    conn.execute(sql, &[]).await?;
    let sql = "create table if not exists map_pulse_channels (name text, tbmax int)";
    conn.execute(sql, &[]).await?;
    let sql = "create table if not exists map_pulse_files (channel text not null, split int not null, timebin int not null, closed int not null default 0, pulse_min int8 not null, pulse_max int8 not null)";
    conn.execute(sql, &[]).await?;
    let sql = "create unique index if not exists map_pulse_files_ix1 on map_pulse_files (channel, split, timebin)";
    conn.execute(sql, &[]).await?;
    let sql = "alter table map_pulse_files add if not exists upc1 int not null default 0";
    conn.execute(sql, &[]).await?;
    let sql = "alter table map_pulse_files add if not exists hostname text not null default ''";
    conn.execute(sql, &[]).await?;
    let sql = "create index if not exists map_pulse_files_ix2 on map_pulse_files (hostname)";
    conn.execute(sql, &[]).await?;
    let sql = "set client_min_messages = 'notice'";
    conn.execute(sql, &[]).await?;
    Ok(())
}

fn timer_channel_names() -> Vec<String> {
    let sections = vec!["SINEG01", "SINSB01", "SINSB02", "SINSB03", "SINSB04", "SINXB01"];
    let suffixes = vec!["MASTER"];
    let mut all: Vec<_> = sections
        .iter()
        .map(|sec| {
            suffixes
                .iter()
                .map(move |suf| format!("{}-RLLE-STA:{}-EVRPULSEID", sec, suf))
        })
        .flatten()
        .collect();
    all.push("SIN-CVME-TIFGUN-EVR0:RX-PULSEID".into());
    all
}

async fn datafiles_for_channel(name: String, node_config: &NodeConfigCached) -> Result<Vec<PathBuf>, Error> {
    let mut a = vec![];
    let sfc = node_config.node.sf_databuffer.as_ref().unwrap();
    let channel_path = sfc
        .data_base_path
        .join(format!("{}_2", sfc.ksprefix))
        .join("byTime")
        .join(&name);
    let mut rd = tokio::fs::read_dir(&channel_path).await?;
    while let Ok(Some(entry)) = rd.next_entry().await {
        let mut rd2 = tokio::fs::read_dir(entry.path()).await?;
        while let Ok(Some(e2)) = rd2.next_entry().await {
            let mut rd3 = tokio::fs::read_dir(e2.path()).await?;
            while let Ok(Some(e3)) = rd3.next_entry().await {
                if e3.file_name().to_string_lossy().ends_with("_00000_Data") {
                    //info!("path: {:?}", e3.path());
                    a.push(e3.path());
                }
            }
        }
    }
    Ok(a)
}

#[derive(Debug)]
struct ChunkInfo {
    pos: u64,
    len: u64,
    ts: u64,
    pulse: u64,
}

async fn read_buf_or_eof(file: &mut File, buf: &mut BytesMut) -> Result<usize, Error> {
    let mut m = 0;
    loop {
        if buf.has_remaining_mut() {
            let n = file.read_buf(buf).await?;
            if n == 0 {
                break;
            } else {
                m += n;
            }
        } else {
            break;
        }
    }
    Ok(m)
}

async fn read_first_chunk(mut file: File) -> Result<(Option<ChunkInfo>, File), Error> {
    file.seek(SeekFrom::Start(0)).await?;
    let mut buf = BytesMut::with_capacity(1024);
    let n1 = read_buf_or_eof(&mut file, &mut buf).await?;
    if n1 < 6 {
        let msg = format!("can not even read 6 bytes from datafile  n1 {}", n1);
        warn!("{msg}");
        return Ok((None, file));
    }
    let ver = buf.get_i16();
    if ver != 0 {
        return Err(Error::with_msg_no_trace(format!("unknown file version  ver {}", ver)));
    }
    let hlen = buf.get_u32() as u64;
    if n1 < 2 + hlen as usize + 4 + 3 * 8 {
        let msg = format!("did not read enough for first event  n1 {}", n1);
        warn!("{msg}");
        return Ok((None, file));
    }
    buf.advance(hlen as usize - 4);
    let clen = buf.get_u32() as u64;
    let _ttl = buf.get_u64();
    let ts = buf.get_u64();
    let pulse = buf.get_u64();
    let ret = ChunkInfo {
        pos: 2 + hlen,
        len: clen,
        ts,
        pulse,
    };
    Ok((Some(ret), file))
}

async fn read_last_chunk(mut file: File, pos_first: u64, chunk_len: u64) -> Result<(Option<ChunkInfo>, File), Error> {
    let flen = file.seek(SeekFrom::End(0)).await?;
    let c1 = (flen - pos_first) / chunk_len;
    if c1 == 0 {
        return Ok((None, file));
    }
    let p2 = pos_first + (c1 - 1) * chunk_len;
    file.seek(SeekFrom::Start(p2)).await?;
    let mut buf = BytesMut::with_capacity(1024);
    let n1 = read_buf_or_eof(&mut file, &mut buf).await?;
    if n1 < 4 + 3 * 8 {
        return Err(Error::with_msg_no_trace(format!(
            "can not read enough from datafile  flen {}  pos_first {}  chunk_len {}  c1 {}  n1 {}",
            flen, pos_first, chunk_len, c1, n1
        )));
    }
    let clen = buf.get_u32() as u64;
    if clen != chunk_len {
        return Err(Error::with_msg_no_trace(format!(
            "read_last_chunk  mismatch  flen {}  pos_first {}  chunk_len {}  clen {}  c1 {}  n1 {}",
            flen, pos_first, chunk_len, clen, c1, n1
        )));
    }
    let _ttl = buf.get_u64();
    let ts = buf.get_u64();
    let pulse = buf.get_u64();
    let ret = ChunkInfo {
        pos: p2,
        len: clen,
        ts,
        pulse,
    };
    Ok((Some(ret), file))
}

async fn read_chunk_at(mut file: File, pos: u64, chunk_len: u64) -> Result<(ChunkInfo, File), Error> {
    file.seek(SeekFrom::Start(pos)).await?;
    let mut buf = BytesMut::with_capacity(1024);
    let n1 = file.read_buf(&mut buf).await?;
    if n1 < 4 + 3 * 8 {
        return Err(Error::with_msg_no_trace(format!(
            "can not read enough from datafile  n1 {}",
            n1
        )));
    }
    let clen = buf.get_u32() as u64;
    if clen != chunk_len {
        return Err(Error::with_msg_no_trace(format!(
            "read_chunk_at  mismatch: pos {}  clen {}  chunk_len {}",
            pos, clen, chunk_len
        )));
    }
    let _ttl = buf.get_u64();
    let ts = buf.get_u64();
    let pulse = buf.get_u64();
    //info!("data chunk len {}  ts {}  pulse {}", clen, ts, pulse);
    let ret = ChunkInfo {
        pos,
        len: clen,
        ts,
        pulse,
    };
    Ok((ret, file))
}

pub struct IndexFullHttpFunction {}

impl IndexFullHttpFunction {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().starts_with(MAP_INDEX_FULL_URL_PREFIX) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        let ret = match Self::index(node_config).await {
            Ok(msg) => response(StatusCode::OK).body(Body::from(msg))?,
            Err(e) => response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from(format!("{:?}", e)))?,
        };
        Ok(ret)
    }

    pub async fn index_channel(
        channel_name: String,
        conn: &dbconn::pg::Client,
        node_config: &NodeConfigCached,
    ) -> Result<String, Error> {
        let mut msg = format!("Index channel {}", channel_name);
        let files = datafiles_for_channel(channel_name.clone(), node_config).await?;
        msg = format!("{}\n{:?}", msg, files);
        let mut latest_pair = (0, 0);
        for path in files {
            let splitted: Vec<_> = path.to_str().unwrap().split("/").collect();
            let timebin: u64 = splitted[splitted.len() - 3].parse()?;
            let split: u64 = splitted[splitted.len() - 2].parse()?;
            if false {
                info!(
                    "hostname {}  timebin {}  split {}",
                    node_config.node.host, timebin, split
                );
            }
            let file = tokio::fs::OpenOptions::new().read(true).open(&path).await?;
            let (r2, file) = read_first_chunk(file).await?;
            msg = format!("{}\n{:?}", msg, r2);
            if let Some(r2) = r2 {
                let (r3, _file) = read_last_chunk(file, r2.pos, r2.len).await?;
                msg = format!("{}\n{:?}", msg, r3);
                if let Some(r3) = r3 {
                    if r3.pulse > latest_pair.0 {
                        latest_pair = (r3.pulse, r3.ts);
                    }
                    // TODO remove update of static columns when older clients are removed.
                    let sql = "insert into map_pulse_files (channel, split, timebin, pulse_min, pulse_max, hostname) values ($1, $2, $3, $4, $5, $6) on conflict (channel, split, timebin) do update set pulse_min = $4, pulse_max = $5, upc1 = map_pulse_files.upc1 + 1, hostname = $6";
                    conn.execute(
                        sql,
                        &[
                            &channel_name,
                            &(split as i32),
                            &(timebin as i32),
                            &(r2.pulse as i64),
                            &(r3.pulse as i64),
                            &node_config.node.host,
                        ],
                    )
                    .await?;
                }
            } else {
                warn!("could not find first event chunk in {path:?}");
            }
        }
        info!("latest for {channel_name}  {latest_pair:?}");
        Ok(msg)
    }

    pub async fn index(node_config: &NodeConfigCached) -> Result<String, Error> {
        // TODO avoid double-insert on central storage.
        let mut msg = format!("LOG");
        make_tables(node_config).await?;
        let conn = dbconn::create_connection(&node_config.node_config.cluster.database).await?;
        let chs = timer_channel_names();
        for channel_name in &chs[..] {
            match Self::index_channel(channel_name.clone(), &conn, node_config).await {
                Ok(m) => {
                    msg.push_str("\n");
                    msg.push_str(&m);
                }
                Err(e) => {
                    error!("error while indexing {}  {:?}", channel_name, e);
                    //return Err(e);
                }
            }
        }
        Ok(msg)
    }
}

pub struct UpdateTaskGuard {
    do_abort: Arc<AtomicUsize>,
    // TODO allow user to explicitly stop and wait with timeout.
    // Protect against double-abort on Drop, or multiple calls.
    jh: Option<JoinHandle<Result<(), Error>>>,
}

impl UpdateTaskGuard {
    pub async fn abort_wait(&mut self) -> Result<(), Error> {
        if let Some(jh) = self.jh.take() {
            info!("UpdateTaskGuard::abort_wait");
            let fut = tokio::time::timeout(Duration::from_millis(6000), async { jh.await });
            Ok(fut.await???)
        } else {
            Ok(())
        }
    }
}

impl Drop for UpdateTaskGuard {
    fn drop(&mut self) {
        info!("impl Drop for UpdateTaskGuard");
        self.do_abort.fetch_add(1, Ordering::SeqCst);
    }
}

async fn update_task(do_abort: Arc<AtomicUsize>, node_config: NodeConfigCached) -> Result<(), Error> {
    loop {
        if do_abort.load(Ordering::SeqCst) != 0 {
            info!("update_task  break A");
            break;
        }
        tokio::time::sleep(Duration::from_millis(40000 + (0x3fff & commonio::tokio_rand().await?))).await;
        if do_abort.load(Ordering::SeqCst) != 0 {
            info!("update_task  break B");
            break;
        }
        let ts1 = Instant::now();
        match IndexFullHttpFunction::index(&node_config).await {
            Ok(_) => {}
            Err(e) => {
                error!("issue during last update task: {:?}", e);
                tokio::time::sleep(Duration::from_millis(5000)).await;
            }
        }
        let ts2 = Instant::now();
        let dt = ts2.duration_since(ts1).as_secs_f64() * 1e3;
        info!("Done update task  {:.0} ms", dt);
    }
    Ok(())
}

pub struct UpdateTask {
    do_abort: Arc<AtomicUsize>,
    fut: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>,
    complete: bool,
}

impl Future for UpdateTask {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll on complete")
            } else if self.do_abort.load(Ordering::SeqCst) != 0 {
                self.complete = true;
                Ready(Ok(()))
            } else {
                match self.fut.poll_unpin(cx) {
                    Ready(res) => {
                        self.complete = true;
                        Ready(res)
                    }
                    Pending => Pending,
                }
            };
        }
    }
}

impl UpdateTask {
    pub fn new(node_config: NodeConfigCached) -> UpdateTaskGuard {
        let do_abort = Arc::new(AtomicUsize::default());
        let task = Self {
            do_abort: do_abort.clone(),
            fut: Box::pin(update_task(do_abort.clone(), node_config)),
            complete: false,
        };
        let jh = tokio::spawn(task);
        let ret = UpdateTaskGuard { do_abort, jh: Some(jh) };
        ret
    }
}

async fn search_pulse(pulse: u64, path: &Path) -> Result<Option<u64>, Error> {
    let f1 = tokio::fs::OpenOptions::new().read(true).open(path).await?;
    let (ck1, f1) = read_first_chunk(f1).await?;
    if let Some(ck1) = ck1 {
        if ck1.pulse == pulse {
            return Ok(Some(ck1.ts));
        }
        if ck1.pulse > pulse {
            return Ok(None);
        }
        let (ck2, mut f1) = read_last_chunk(f1, ck1.pos, ck1.len).await?;
        if let Some(ck2) = ck2 {
            if ck2.pulse == pulse {
                return Ok(Some(ck2.ts));
            }
            if ck2.pulse < pulse {
                return Ok(None);
            }
            let chunk_len = ck1.len;
            //let flen = f1.seek(SeekFrom::End(0)).await?;
            //let chunk_count = (flen - ck1.pos) / ck1.len;
            let mut p1 = ck1.pos;
            let mut p2 = ck2.pos;
            loop {
                let d = p2 - p1;
                if 0 != d % chunk_len {
                    return Err(Error::with_msg_no_trace(format!("search_pulse  ")));
                }
                if d <= chunk_len {
                    return Ok(None);
                }
                let m = p1 + d / chunk_len / 2 * chunk_len;
                let (z, f2) = read_chunk_at(f1, m, chunk_len).await?;
                f1 = f2;
                if z.pulse == pulse {
                    return Ok(Some(z.ts));
                } else if z.pulse > pulse {
                    p2 = m;
                } else {
                    p1 = m;
                }
            }
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapPulseQuery {
    pub backend: String,
    pub pulse: u64,
}

impl HasBackend for MapPulseQuery {
    fn backend(&self) -> &str {
        &self.backend
    }
}

impl HasTimeout for MapPulseQuery {
    fn timeout(&self) -> Duration {
        Duration::from_millis(2000)
    }
}

impl FromUrl for MapPulseQuery {
    fn from_url(url: &url::Url) -> Result<Self, err::Error> {
        let mut pit = url
            .path_segments()
            .ok_or(Error::with_msg_no_trace("no path in url"))?
            .rev();
        let pulsestr = pit.next().ok_or(Error::with_msg_no_trace("no pulse in url path"))?;
        let backend = pit.next().unwrap_or("sf-databuffer").into();
        // TODO legacy: use a default backend if not specified.
        let backend = if backend == "pulse" {
            String::from("sf-databuffer")
        } else {
            backend
        };
        let pulse: u64 = pulsestr.parse()?;
        let ret = Self { backend, pulse };
        info!("FromUrl {ret:?}");
        Ok(ret)
    }

    fn from_pairs(_pairs: &BTreeMap<String, String>) -> Result<Self, err::Error> {
        Err(err::Error::with_msg_no_trace(format!(
            "can not only construct from pairs"
        )))
    }
}

impl AppendToUrl for MapPulseQuery {
    fn append_to_url(&self, _url: &mut Url) {}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalMap {
    pulse: u64,
    tss: Vec<u64>,
    channels: Vec<String>,
}

pub trait ErrConv<T> {
    fn err_conv(self) -> Result<T, err::Error>;
}

impl<T> ErrConv<T> for Result<T, scylla::transport::errors::NewSessionError> {
    fn err_conv(self) -> Result<T, err::Error> {
        self.map_err(|e| err::Error::with_msg_no_trace(format!("{e:?}")))
    }
}

impl<T> ErrConv<T> for Result<T, scylla::transport::errors::QueryError> {
    fn err_conv(self) -> Result<T, err::Error> {
        self.map_err(|e| err::Error::with_msg_no_trace(format!("{e:?}")))
    }
}

impl<T> ErrConv<T> for Result<T, scylla::transport::query_result::RowsExpectedError> {
    fn err_conv(self) -> Result<T, err::Error> {
        self.map_err(|e| err::Error::with_msg_no_trace(format!("{e:?}")))
    }
}

pub struct MapPulseScyllaHandler {}

impl MapPulseScyllaHandler {
    pub fn prefix() -> &'static str {
        "/api/4/scylla/map/pulse/"
    }

    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        let urls = format!("dummy://{}", req.uri());
        let url = url::Url::parse(&urls)?;
        let query = MapPulseQuery::from_url(&url)?;
        let pulse = query.pulse;
        let scyconf = if let Some(x) = node_config.node_config.cluster.scylla.as_ref() {
            x
        } else {
            return Err(Error::with_public_msg_no_trace("no scylla configured"));
        };
        let scy = scyllaconn::create_scy_session(&scyconf).await?;
        let pulse_a = (pulse >> 14) as i64;
        let pulse_b = (pulse & 0x3fff) as i32;
        let res = scy
            .query(
                "select ts_a, ts_b from pulse where pulse_a = ? and pulse_b = ?",
                (pulse_a, pulse_b),
            )
            .await
            .err_conv()?;
        let rows = res.rows().err_conv()?;
        let ch = "pulsemaptable";
        let mut tss = Vec::new();
        let mut channels = Vec::new();
        use scylla::frame::response::result::CqlValue;
        let ts_a_def = CqlValue::BigInt(0);
        let ts_b_def = CqlValue::Int(0);
        for row in rows {
            let ts_a = row.columns[0].as_ref().unwrap_or(&ts_a_def).as_bigint().unwrap_or(0) as u64;
            let ts_b = row.columns[1].as_ref().unwrap_or(&ts_b_def).as_int().unwrap_or(0) as u32 as u64;
            tss.push(ts_a * netpod::timeunits::SEC + ts_b);
            channels.push(ch.into());
        }
        let ret = LocalMap { pulse, tss, channels };
        Ok(response(StatusCode::OK).body(Body::from(serde_json::to_vec(&ret)?))?)
    }
}

pub struct MapPulseLocalHttpFunction {}

impl MapPulseLocalHttpFunction {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().starts_with(MAP_PULSE_LOCAL_URL_PREFIX) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        let urls = format!("{}", req.uri());
        let pulse: u64 = urls[MAP_PULSE_LOCAL_URL_PREFIX.len()..]
            .parse()
            .map_err(|_| Error::with_public_msg_no_trace(format!("can not understand pulse map url: {}", req.uri())))?;
        let conn = dbconn::create_connection(&node_config.node_config.cluster.database).await?;
        let sql = "select channel, hostname, timebin, split from map_pulse_files where hostname = $1 and pulse_min <= $2 and (pulse_max >= $2 or closed = 0)";
        let rows = conn.query(sql, &[&node_config.node.host, &(pulse as i64)]).await?;
        let cands: Vec<_> = rows
            .iter()
            .map(|r| {
                let channel: String = r.try_get(0).unwrap_or("nochannel".into());
                let hostname: String = r.try_get(1).unwrap_or("nohost".into());
                let timebin: i32 = r.try_get(2).unwrap_or(0);
                let split: i32 = r.try_get(3).unwrap_or(0);
                (channel, hostname, timebin as u32, split as u32)
            })
            .collect();
        //let mut msg = String::new();
        //use std::fmt::Write;
        //write!(&mut msg, "cands: {:?}\n", cands)?;
        let mut tss = Vec::new();
        let mut channels = Vec::new();
        for (ch, _, tb, sp) in cands {
            let ks = 2;
            match disk::paths::data_path_tb(ks, &ch, tb, 86400000, sp, &node_config.node) {
                Ok(path) => {
                    //write!(&mut msg, "data_path_tb:  {:?}\n", path)?;
                    match search_pulse(pulse, &path).await {
                        Ok(ts) => {
                            //write!(&mut msg, "SEARCH:  {:?}  for {}\n", ts, pulse)?;
                            if let Some(ts) = ts {
                                tss.push(ts);
                                channels.push(ch);
                            }
                        }
                        Err(e) => {
                            warn!("can not map pulse with {ch} {sp} {tb} {e}");
                        }
                    }
                }
                Err(e) => {
                    warn!("can not get path to files {ch} {e}");
                }
            }
        }
        let ret = LocalMap { pulse, tss, channels };
        Ok(response(StatusCode::OK).body(Body::from(serde_json::to_vec(&ret)?))?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsHisto {
    pulse: u64,
    tss: Vec<u64>,
    counts: Vec<u64>,
}

pub struct MapPulseHistoHttpFunction {}

impl MapPulseHistoHttpFunction {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().starts_with(MAP_PULSE_HISTO_URL_PREFIX) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        let urls = format!("{}", req.uri());
        let pulse: u64 = urls[MAP_PULSE_HISTO_URL_PREFIX.len()..].parse()?;
        let ret = Self::histo(pulse, node_config).await?;
        Ok(response(StatusCode::OK).body(Body::from(serde_json::to_vec(&ret)?))?)
    }

    pub async fn histo(pulse: u64, node_config: &NodeConfigCached) -> Result<TsHisto, Error> {
        let mut futs = FuturesOrdered::new();
        for node in &node_config.node_config.cluster.nodes {
            let s = format!("http://{}:{}/api/1/map/pulse/local/{}", node.host, node.port, pulse);
            let uri: Uri = s.parse()?;
            let fut = hyper::Client::new().get(uri);
            let fut = tokio::time::timeout(Duration::from_millis(1000), fut);
            futs.push_back(fut);
        }
        use futures_util::stream::StreamExt;
        let mut map = BTreeMap::new();
        while let Some(Ok(Ok(res))) = futs.next().await {
            if let Ok(b) = hyper::body::to_bytes(res.into_body()).await {
                if let Ok(lm) = serde_json::from_slice::<LocalMap>(&b) {
                    for ts in lm.tss {
                        let a = map.get(&ts);
                        if let Some(&j) = a {
                            map.insert(ts, j + 1);
                        } else {
                            map.insert(ts, 1);
                        }
                    }
                }
            }
        }
        let ret = TsHisto {
            pulse,
            tss: map.keys().map(|j| *j).collect(),
            counts: map.values().map(|j| *j).collect(),
        };
        Ok(ret)
    }
}

pub struct MapPulseHttpFunction {}

impl MapPulseHttpFunction {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().starts_with(MAP_PULSE_URL_PREFIX) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        info!("MapPulseHttpFunction  handle  uri: {:?}", req.uri());
        let urls = format!("{}", req.uri());
        let pulse: u64 = urls[MAP_PULSE_URL_PREFIX.len()..].parse()?;
        let histo = MapPulseHistoHttpFunction::histo(pulse, node_config).await?;
        let mut i1 = 0;
        let mut max = 0;
        for i2 in 0..histo.tss.len() {
            if histo.counts[i2] > max {
                max = histo.counts[i2];
                i1 = i2;
            }
        }
        if max > 0 {
            Ok(response(StatusCode::OK).body(Body::from(serde_json::to_vec(&histo.tss[i1])?))?)
        } else {
            Ok(response(StatusCode::NO_CONTENT).body(Body::empty())?)
        }
    }
}

pub struct Api4MapPulseHttpFunction {}

impl Api4MapPulseHttpFunction {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().starts_with(API_4_MAP_PULSE_URL_PREFIX) {
            Some(Self {})
        } else {
            None
        }
    }

    pub fn path_matches(path: &str) -> bool {
        path.starts_with(API_4_MAP_PULSE_URL_PREFIX)
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        info!("Api4MapPulseHttpFunction  handle  uri: {:?}", req.uri());
        let url = Url::parse(&format!("dummy:{}", req.uri()))?;
        let q = MapPulseQuery::from_url(&url)?;
        let histo = MapPulseHistoHttpFunction::histo(q.pulse, node_config).await?;
        let mut i1 = 0;
        let mut max = 0;
        for i2 in 0..histo.tss.len() {
            if histo.counts[i2] > max {
                max = histo.counts[i2];
                i1 = i2;
            }
        }
        if max > 0 {
            Ok(response(StatusCode::OK).body(Body::from(serde_json::to_vec(&histo.tss[i1])?))?)
        } else {
            Ok(response(StatusCode::NO_CONTENT).body(Body::empty())?)
        }
    }
}

pub struct MarkClosedHttpFunction {}

impl MarkClosedHttpFunction {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().starts_with(MAP_PULSE_MARK_CLOSED_URL_PREFIX) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        info!("MarkClosedHttpFunction  handle  uri: {:?}", req.uri());
        match MarkClosedHttpFunction::mark_closed(node_config).await {
            Ok(_) => {
                let ret = response(StatusCode::OK).body(Body::empty())?;
                Ok(ret)
            }
            Err(e) => {
                let msg = format!("{:?}", e);
                let ret = response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from(msg))?;
                Ok(ret)
            }
        }
    }

    pub async fn mark_closed(node_config: &NodeConfigCached) -> Result<(), Error> {
        let conn = dbconn::create_connection(&node_config.node_config.cluster.database).await?;
        let sql = "select distinct channel from map_pulse_files order by channel";
        let rows = conn.query(sql, &[]).await?;
        let chns: Vec<_> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        for chn in &chns {
            let sql = concat!(
                "with q1 as (select channel, split, timebin from map_pulse_files",
                " where channel = $1 and hostname = $2",
                " order by timebin desc offset 2)",
                " update map_pulse_files t2 set closed = 1 from q1",
                " where t2.channel = q1.channel",
                " and t2.closed = 0",
                " and t2.split = q1.split",
                " and t2.timebin = q1.timebin",
            );
            let nmod = conn.execute(sql, &[&chn, &node_config.node.host]).await?;
            info!(
                "mark files  mod {}  chn {:?}  host {:?}",
                nmod, chn, node_config.node.host
            );
        }
        Ok(())
    }
}
