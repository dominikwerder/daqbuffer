use crate::response;
use bytes::Buf;
use bytes::BytesMut;
use err::Error;
use http::{Method, StatusCode};
use hyper::{Body, Request, Response};
use netpod::log::*;
use netpod::NodeConfigCached;
use std::{io::SeekFrom, path::PathBuf};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

pub struct MapPulseHisto {
    _pulse: u64,
    _tss: Vec<u64>,
    _counts: Vec<u64>,
}

const MAP_INDEX_FULL_URL_PREFIX: &'static str = "/api/1/map/index/full/";
const _MAP_INDEX_FAST_URL_PREFIX: &'static str = "/api/1/map/index/fast/";
const MAP_PULSE_HISTO_URL_PREFIX: &'static str = "/api/1/map/pulse/histo/";
const MAP_PULSE_URL_PREFIX: &'static str = "/api/1/map/pulse/";

async fn make_tables(node_config: &NodeConfigCached) -> Result<(), Error> {
    let conn = dbconn::create_connection(&node_config.node_config.cluster.database).await?;
    let sql = "create table if not exists map_pulse_channels (name text, tbmax int)";
    conn.execute(sql, &[]).await?;
    let sql = "create table if not exists map_pulse_files (channel text not null, split int not null, timebin int not null, closed int not null default 0, pulse_min int8 not null, pulse_max int8 not null)";
    conn.execute(sql, &[]).await?;
    let sql = "create unique index if not exists map_pulse_files_ix1 on map_pulse_files (channel, split, timebin)";
    conn.execute(sql, &[]).await?;
    Ok(())
}

fn timer_channel_names() -> Vec<String> {
    let sections = vec!["SINEG01", "SINSB01", "SINSB02", "SINSB03", "SINSB04", "SINXB01"];
    let suffixes = vec!["MASTER"];
    let all: Vec<_> = sections
        .iter()
        .map(|sec| {
            suffixes
                .iter()
                .map(move |suf| format!("{}-RLLE-STA:{}-EVRPULSEID", sec, suf))
        })
        .flatten()
        .collect();
    all
}

async fn datafiles_for_channel(name: String, node_config: &NodeConfigCached) -> Result<Vec<PathBuf>, Error> {
    let mut a = vec![];
    let n = &node_config.node;
    let channel_path = n
        .data_base_path
        .join(format!("{}_2", n.ksprefix))
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

async fn read_first_chunk(mut file: File) -> Result<(ChunkInfo, File), Error> {
    file.seek(SeekFrom::Start(0)).await?;
    let mut buf = BytesMut::with_capacity(1024);
    let n1 = file.read_buf(&mut buf).await?;
    if n1 < 6 {
        return Err(Error::with_msg_no_trace(format!(
            "can not even read 6 bytes from datafile  n1 {}",
            n1
        )));
    }
    let ver = buf.get_i16();
    if ver != 0 {
        return Err(Error::with_msg_no_trace(format!("unknown file version  ver {}", ver)));
    }
    let hlen = buf.get_u32() as u64;
    if n1 < 2 + hlen as usize + 4 + 3 * 8 {
        return Err(Error::with_msg_no_trace(format!(
            "did not read enough for first event  n1 {}",
            n1
        )));
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
    Ok((ret, file))
}

async fn read_last_chunk(mut file: File, pos_first: u64, chunk_len: u64) -> Result<(ChunkInfo, File), Error> {
    let flen = file.seek(SeekFrom::End(0)).await?;
    let c1 = (flen - pos_first) / chunk_len;
    if c1 == 0 {
        return Err(Error::with_msg_no_trace("no chunks in this file"));
    }
    let p2 = pos_first + (c1 - 1) * chunk_len;
    file.seek(SeekFrom::Start(p2)).await?;
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
            "mismatch: clen {}  chunk_len {}",
            clen, chunk_len
        )));
    }
    let _ttl = buf.get_u64();
    let ts = buf.get_u64();
    let pulse = buf.get_u64();
    info!("data chunk len {}  ts {}  pulse {}", clen, ts, pulse);
    let ret = ChunkInfo {
        pos: p2,
        len: clen,
        ts,
        pulse,
    };
    Ok((ret, file))
}

pub struct IndexFullHttpFunction {}

impl IndexFullHttpFunction {
    pub fn path_matches(path: &str) -> bool {
        path.starts_with(MAP_INDEX_FULL_URL_PREFIX)
    }

    pub async fn handle(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        // For each timer channel, find all local data files (TODO what about central storage?)
        // When central storage, must assign a split-whitelist.
        // Scan each datafile:
        // findFirstChunk(path) read first kB, u16 version tag, u32 len of string header (incl itself), first event after that.
        // readFirstChunk(file, posFirst) read first chunk, extract len, ts, pulse.
        // readLastChunk(file, posFirst, chunkLen)
        // Collect info: Path, chunkLen, posFirst, tsFirst, pulseFirst, posLast, tsLast, pulseLast

        // Update database:
        //"insert into map_pulse_files (channel, split, timebin, pulse_min, pulse_max) values (?, ?, ?, ?, ?) on conflict (channel, split, timebin) do update set pulse_min=?, pulse_max=?"
        // TODO Mark files as "closed".

        make_tables(node_config).await?;
        let conn = dbconn::create_connection(&node_config.node_config.cluster.database).await?;

        let chs = timer_channel_names();
        for channel_name in &chs[0..2] {
            let files = datafiles_for_channel(channel_name.clone(), node_config).await?;
            let mut msg = format!("{:?}", files);
            for path in files {
                let splitted: Vec<_> = path.to_str().unwrap().split("/").collect();
                info!("splitted: {:?}", splitted);
                let timebin: u64 = splitted[splitted.len() - 3].parse()?;
                let split: u64 = splitted[splitted.len() - 2].parse()?;
                info!("timebin {}  split {}", timebin, split);
                let file = tokio::fs::OpenOptions::new().read(true).open(path).await?;
                let (r2, file) = read_first_chunk(file).await?;
                msg = format!("{}\n{:?}", msg, r2);
                let (r3, _file) = read_last_chunk(file, r2.pos, r2.len).await?;
                msg = format!("{}\n{:?}", msg, r3);
                let sql = "insert into map_pulse_files (channel, split, timebin, pulse_min, pulse_max) values ($1, $2, $3, $4, $5) on conflict (channel, split, timebin) do update set pulse_min=$4, pulse_max=$5";
                conn.execute(sql, &[])
            }
        }
        Ok(response(StatusCode::OK).body(Body::from(msg))?)
    }
}

pub struct MapPulseHistoHttpFunction {}

impl MapPulseHistoHttpFunction {
    pub fn path_matches(path: &str) -> bool {
        path.starts_with(MAP_PULSE_HISTO_URL_PREFIX)
    }

    pub fn handle(req: Request<Body>, _node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        let urls = format!("{}", req.uri());
        let _pulse: u64 = urls[MAP_PULSE_HISTO_URL_PREFIX.len()..].parse()?;
        Ok(response(StatusCode::NOT_IMPLEMENTED).body(Body::empty())?)
    }
}

pub struct MapPulseHttpFunction {}

impl MapPulseHttpFunction {
    pub fn path_matches(path: &str) -> bool {
        path.starts_with(MAP_PULSE_URL_PREFIX)
    }

    pub fn handle(req: Request<Body>, _node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        let urls = format!("{}", req.uri());
        let _pulse: u64 = urls[MAP_PULSE_URL_PREFIX.len()..].parse()?;
        Ok(response(StatusCode::NOT_IMPLEMENTED).body(Body::empty())?)
    }
}
