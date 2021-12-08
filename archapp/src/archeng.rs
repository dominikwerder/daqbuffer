pub mod backreadbuf;
pub mod blockrefstream;
pub mod blockstream;
pub mod bufminread;
pub mod configs;
pub mod datablock;
pub mod diskio;
pub mod indexfiles;
pub mod indextree;
pub mod pipe;

use self::indexfiles::{database_connect, list_index_files};
use self::indextree::channel_list;
use crate::timed::Timed;
use crate::wrap_task;
use async_channel::{Receiver, Sender};
use commonio::StatsChannel;
use err::{ErrStr, Error};
use futures_util::StreamExt;
use items::{StreamItem, WithLen};
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{ChannelArchiver, ChannelConfigQuery, ChannelConfigResponse};
use netpod::{ScalarType, Shape};
use serde::Serialize;
use serde_json::Value as JsVal;
use std::convert::TryInto;

const EPICS_EPOCH_OFFSET: u64 = 631152000 * SEC;

pub fn name_hash(s: &str, ht_len: u32) -> u32 {
    let mut h = 0;
    for ch in s.as_bytes() {
        h = (128 * h + *ch as u32) % ht_len;
    }
    h
}

fn format_hex_block(buf: &[u8], max: usize) -> String {
    use std::fmt::Write;
    const COLS: usize = 16;
    let buf = if buf.len() > max { &buf[0..max] } else { buf };
    let mut i1 = 0;
    let mut ret = String::new();
    while i1 < buf.len() {
        buf[i1..i1 + COLS].iter().for_each(|x| {
            write!(&mut ret, " {:02x}", *x).unwrap();
        });
        ret.push('\n');
        i1 += COLS;
    }
    ret
}

fn readu64(buf: &[u8], pos: usize) -> u64 {
    u64::from_be_bytes(buf.as_ref()[pos..pos + 8].try_into().unwrap())
}

fn readu32(buf: &[u8], pos: usize) -> u32 {
    u32::from_be_bytes(buf.as_ref()[pos..pos + 4].try_into().unwrap())
}

fn readu16(buf: &[u8], pos: usize) -> u16 {
    u16::from_be_bytes(buf.as_ref()[pos..pos + 2].try_into().unwrap())
}

fn readf64(buf: &[u8], pos: usize) -> f64 {
    f64::from_be_bytes(buf.as_ref()[pos..pos + 8].try_into().unwrap())
}

fn read_string(buf: &[u8]) -> Result<String, Error> {
    let imax = buf
        .iter()
        .map(|k| *k)
        .enumerate()
        .take_while(|&(_, k)| k != 0)
        .last()
        .map(|(i, _)| i);
    let ret = match imax {
        Some(imax) => String::from_utf8(buf[..imax + 1].to_vec())?,
        None => String::new(),
    };
    Ok(ret)
}

#[allow(dead_code)]
async fn datarange_stream_fill(_channel_name: &str, _tx: Sender<Datarange>) {
    // Search the first relevant leaf node.
    // Pipe all ranges from there, and continue with nodes.
    // Issue: can not stop because I don't look into the files.
}

// TODO
// Should contain enough information to allow one to open and position a relevant datafile.
pub struct Datarange {}

pub fn datarange_stream(_channel_name: &str) -> Result<Receiver<Datarange>, Error> {
    let (_tx, rx) = async_channel::bounded(4);
    let task = async {};
    taskrun::spawn(task);
    Ok(rx)
}

#[derive(Debug, Serialize)]
pub struct ListChannelItem {
    name: String,
    index_path: String,
    matches: bool,
}

pub fn list_all_channels(node: &ChannelArchiver) -> Receiver<Result<ListChannelItem, Error>> {
    let node = node.clone();
    let (tx, rx) = async_channel::bounded(4);
    let tx2 = tx.clone();
    let stats = {
        let (tx, rx) = async_channel::bounded(16);
        taskrun::spawn(async move {
            let mut rx = rx;
            while let Some(item) = rx.next().await {
                match item {
                    Ok(StreamItem::Stats(item)) => {
                        debug!("stats: {:?}", item);
                    }
                    _ => {}
                }
            }
        });
        StatsChannel::new(tx.clone())
    };
    let task = async move {
        let mut ixf = list_index_files(&node);
        while let Some(f) = ixf.next().await {
            let index_path = f?;
            //info!("try to read for {:?}", index_path);
            let channels = channel_list(index_path.clone(), &stats).await?;
            //info!("list_all_channels  emit {} channels", channels.len());
            for ch in channels {
                let mm = match ch.split("-").next() {
                    Some(k) => {
                        let dname = index_path.parent().unwrap().file_name().unwrap().to_str().unwrap();
                        if dname.starts_with(&format!("archive_{}", k)) {
                            true
                        } else {
                            false
                        }
                    }
                    None => false,
                };
                let item = ListChannelItem {
                    name: ch,
                    index_path: index_path.to_str().unwrap().into(),
                    matches: mm,
                };
                tx.send(Ok(item)).await.errstr()?;
                //info!("{:?}  parent {:?}  channel {}", index_path, index_path.parent(), ch);
                //break;
            }
        }
        Ok::<_, Error>(())
    };
    wrap_task(task, tx2);
    rx
}

struct ErrWrap(tokio_postgres::Error);

impl From<tokio_postgres::Error> for ErrWrap {
    fn from(x: tokio_postgres::Error) -> Self {
        Self(x)
    }
}

impl From<ErrWrap> for Error {
    fn from(_: ErrWrap) -> Self {
        todo!()
    }
}

pub async fn channel_config_from_db(
    q: &ChannelConfigQuery,
    conf: &ChannelArchiver,
) -> Result<ChannelConfigResponse, Error> {
    let dbc = database_connect(&conf.database).await?;
    let sql = "select config from channels where name = $1";
    let rows = dbc.query(sql, &[&q.channel.name()]).await.errstr()?;
    if let Some(row) = rows.first() {
        let cfg: JsVal = row.try_get(0).errstr()?;
        let val = cfg
            .get("shape")
            .ok_or_else(|| Error::with_msg_no_trace("shape not found on config"))?;
        let shape = Shape::from_db_jsval(val)?;
        let val = cfg
            .get("scalarType")
            .ok_or_else(|| Error::with_msg_no_trace("no scalarType in db"))?;
        let s = if let JsVal::String(s) = val {
            s
        } else {
            return Err(Error::with_msg_no_trace(format!(
                "channel_config_from_db bad scalar type {:?}",
                cfg
            )));
        };
        let scalar_type = ScalarType::from_archeng_db_str(s)?;
        let ret = ChannelConfigResponse {
            channel: q.channel.clone(),
            scalar_type,
            // TODO.. only binary endpoint would care.
            byte_order: None,
            shape,
        };
        Ok(ret)
    } else {
        Err(Error::with_msg_no_trace(format!(
            "can not find config for {}",
            q.channel.name()
        )))
    }
}

pub async fn channel_config(q: &ChannelConfigQuery, conf: &ChannelArchiver) -> Result<ChannelConfigResponse, Error> {
    let _timed = Timed::new("channel_config");
    let mut type_info = None;
    let ixpaths = indexfiles::index_file_path_list(q.channel.clone(), conf.database.clone()).await?;
    info!("got categorized ixpaths: {:?}", ixpaths);
    let ixpath = ixpaths.first().unwrap().clone();
    let stream = blockrefstream::blockref_stream(q.channel.clone(), q.range.clone(), q.expand, ixpath.clone());
    let stream = Box::pin(stream);
    let stream = blockstream::BlockStream::new(stream, q.range.clone(), 1);
    let mut stream = stream;
    while let Some(item) = stream.next().await {
        use blockstream::BlockItem::*;
        match item {
            Ok(k) => match k {
                EventsItem(item) => {
                    if item.len() > 0 {
                        type_info = Some(item.type_info());
                        break;
                    }
                }
                JsVal(jsval) => {
                    debug!("jsval: {}", serde_json::to_string(&jsval)?);
                }
            },
            Err(e) => {
                error!("{}", e);
                ()
            }
        }
    }
    if type_info.is_none() {
        let timed_normal = Timed::new("channel_config NORMAL");
        warn!("channel_config expand mode returned none");
        let stream = blockrefstream::blockref_stream(q.channel.clone(), q.range.clone(), q.expand, ixpath.clone());
        let stream = Box::pin(stream);
        let stream = blockstream::BlockStream::new(stream, q.range.clone(), 1);
        let mut stream = stream;
        while let Some(item) = stream.next().await {
            use blockstream::BlockItem::*;
            match item {
                Ok(k) => match k {
                    EventsItem(item) => {
                        if item.len() > 0 {
                            type_info = Some(item.type_info());
                            break;
                        }
                    }
                    JsVal(jsval) => {
                        debug!("jsval: {}", serde_json::to_string(&jsval)?);
                    }
                },
                Err(e) => {
                    error!("{}", e);
                    ()
                }
            }
        }
        drop(timed_normal);
    }
    if let Some(type_info) = type_info {
        let ret = ChannelConfigResponse {
            channel: q.channel.clone(),
            scalar_type: type_info.0,
            byte_order: None,
            shape: type_info.1,
        };
        Ok(ret)
    } else {
        Err(Error::with_msg_no_trace("can not get channel type info"))
    }
}

#[cfg(test)]
mod test {
    use crate::archeng::datablock::{read_data_1, read_datafile_header};
    use crate::archeng::indextree::{read_channel, read_datablockref, search_record};
    use crate::archeng::{StatsChannel, EPICS_EPOCH_OFFSET};
    use commonio::open_read;
    use err::Error;
    use items::{LogItem, Sitemty, StatsItem, StreamItem};
    use netpod::timeunits::*;
    use netpod::{log::*, RangeFilterStats};
    use netpod::{FilePos, NanoRange, Nanos};
    use serde::Serialize;
    use std::path::PathBuf;

    /*
    Root node: most left record ts1 965081099942616289, most right record ts2 1002441959876114632
    */
    const CHN_0_MASTER_INDEX: &str = "/data/daqbuffer-testdata/sls/gfa03/bl_arch/archive_X05DA_SH/index";

    #[test]
    fn search_record_data() -> Result<(), Error> {
        let fut = async {
            let stats = &StatsChannel::dummy();
            let index_path: PathBuf = CHN_0_MASTER_INDEX.into();
            let index_file = open_read(index_path.clone(), stats).await?;
            let mut file2 = open_read(index_path.clone(), stats).await?;
            let channel_name = "X05DA-FE-WI1:TC1";
            const T0: u64 = 1002000000 * SEC + EPICS_EPOCH_OFFSET;
            let beg = Nanos { ns: T0 };
            let range = NanoRange {
                beg: beg.ns,
                end: beg.ns + 20 * SEC,
            };
            let res = read_channel(index_path.clone(), index_file, channel_name, stats).await?;
            let cib = res.unwrap();
            let (res, _stats) = search_record(&mut file2, cib.rtree_m, cib.rtree_start_pos, beg, stats).await?;
            assert_eq!(res.is_some(), true);
            let res = res.unwrap();
            assert_eq!(res.node.is_leaf, true);
            assert_eq!(res.node.pos.pos, 1861178);
            assert_eq!(res.rix, 41);
            let rec = &res.node.records[res.rix];
            assert_eq!(rec.ts1.ns, 1001993759871202919 + EPICS_EPOCH_OFFSET);
            assert_eq!(rec.ts2.ns, 1002009299596362122 + EPICS_EPOCH_OFFSET);
            assert_eq!(rec.child_or_id, 2501903);
            let pos = FilePos { pos: rec.child_or_id };
            let datablock = read_datablockref(&mut file2, pos, cib.hver(), stats).await?;
            assert_eq!(datablock.data_header_pos().0, 9311367);
            assert_eq!(datablock.file_name(), "20211001/20211001");
            let data_path = index_path.parent().unwrap().join(datablock.file_name());
            let mut data_file = open_read(data_path, stats).await?;
            let datafile_header = read_datafile_header(&mut data_file, datablock.data_header_pos(), stats).await?;
            let events = read_data_1(&mut data_file, &datafile_header, range.clone(), false, stats).await?;
            debug!("read events: {:?}", events);
            // TODO assert more
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }

    #[test]
    fn test_bincode_rep_stats() {
        fn make_stats<T>() -> Vec<u8>
        where
            T: Serialize,
        {
            let stats = RangeFilterStats {
                events_pre: 626262,
                events_post: 929292,
                events_unordered: 131313,
            };
            let item = StreamItem::Stats(StatsItem::RangeFilterStats(stats));
            let item: Sitemty<T> = Ok(item);
            bincode::serialize(&item).unwrap()
        }
        let v1 = make_stats::<u8>();
        let v2 = make_stats::<f32>();
        let v3 = make_stats::<Vec<u32>>();
        let v4 = make_stats::<Vec<f64>>();
        assert_eq!(v1, v2);
        assert_eq!(v1, v3);
        assert_eq!(v1, v4);
    }

    #[test]
    fn test_bincode_rep_log() {
        fn make_log<T>() -> Vec<u8>
        where
            T: Serialize,
        {
            let item = StreamItem::Log(LogItem::quick(
                Level::DEBUG,
                format!("Some easy log message for testing purpose here."),
            ));
            let item: Sitemty<T> = Ok(item);
            bincode::serialize(&item).unwrap()
        }
        let v1 = make_log::<u8>();
        let v2 = make_log::<f32>();
        let v3 = make_log::<Vec<u32>>();
        let v4 = make_log::<Vec<f64>>();
        assert_eq!(v1, v2);
        assert_eq!(v1, v3);
        assert_eq!(v1, v4);
    }
}
