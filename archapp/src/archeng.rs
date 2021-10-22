pub mod datablockstream;
pub mod datastream;
pub mod pipe;

use crate::{EventsItem, PlainEvents, ScalarPlainEvents};
use async_channel::{Receiver, Sender};
use err::Error;
use futures_core::Future;
use futures_util::StreamExt;
use items::eventvalues::EventValues;
use items::{RangeCompletableItem, StreamItem};
use netpod::timeunits::SEC;
use netpod::{log::*, ChannelArchiver, ChannelConfigQuery, ChannelConfigResponse, DataHeaderPos, FilePos, Nanos};
use std::convert::TryInto;
use std::io::{self, SeekFrom};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::fs::{read_dir, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/*
struct ReadExactWrap<'a> {
    fut: &'a mut dyn Future<Output = io::Result<usize>>,
}

trait TimedIo {
    fn read_exact<'a, F>(&'a mut self, buf: &'a mut [u8]) -> ReadExactWrap
    where
        Self: Unpin;
}

impl TimedIo for File {
    fn read_exact<'a, F>(&'a mut self, buf: &'a mut [u8]) -> ReadExactWrap
    where
        Self: Unpin,
    {
        let fut = tokio::io::AsyncReadExt::read_exact(self, buf);
        ReadExactWrap { fut: Box::pin(fut) }
    }
}
*/

type Offset = u64;

const OFFSET_SIZE: usize = std::mem::size_of::<Offset>();
const EPICS_EPOCH_OFFSET: u64 = 631152000 * SEC;

pub async fn open_read(path: PathBuf) -> io::Result<File> {
    let ts1 = Instant::now();
    let res = OpenOptions::new().read(true).open(path).await;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1).as_secs_f64() * 1e3;
    if false {
        info!("timed open_read  dt: {:.3} ms", dt);
    }
    res
}

async fn seek(file: &mut File, pos: SeekFrom) -> io::Result<u64> {
    let ts1 = Instant::now();
    let res = file.seek(pos).await;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1).as_secs_f64() * 1e3;
    if false {
        info!("timed seek  dt: {:.3} ms", dt);
    }
    res
}

async fn read(file: &mut File, buf: &mut [u8]) -> io::Result<usize> {
    let ts1 = Instant::now();
    let res = file.read(buf).await;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1).as_secs_f64() * 1e3;
    if false {
        info!("timed read  dt: {:.3} ms  res: {:?}", dt, res);
    }
    res
}

async fn read_exact(file: &mut File, buf: &mut [u8]) -> io::Result<usize> {
    let ts1 = Instant::now();
    let res = file.read_exact(buf).await;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1).as_secs_f64() * 1e3;
    if false {
        info!("timed read_exact  dt: {:.3} ms  res: {:?}", dt, res);
    }
    res
}

#[derive(Debug)]
pub struct NamedHashTableEntry {
    named_hash_channel_entry_pos: u64,
}

#[derive(Debug)]
pub struct NamedHashChannelEntry {
    next: u64,
    id_rtree_pos: u64,
    channel_name: String,
    id_txt: String,
}

#[derive(Debug)]
pub struct IndexFileBasics {
    version: u8,
    name_hash_anchor_beg: u64,
    name_hash_anchor_len: u64,
    fa_used_list_beg: u64,
    fa_used_list_end: u64,
    fa_used_list_len: u64,
    fa_free_list_beg: u64,
    fa_free_list_end: u64,
    fa_free_list_len: u64,
    fa_header_prev: u64,
    fa_header_next: u64,
    fa_header_len: u64,
    name_hash_entries: Vec<NamedHashTableEntry>,
}

pub fn name_hash(s: &str, ht_len: u32) -> u32 {
    let mut h = 0;
    for ch in s.as_bytes() {
        h = (128 * h + *ch as u32) % ht_len;
    }
    h
}

pub struct RingBuf {
    buf: Vec<u8>,
    wp: usize,
    rp: usize,
}

impl RingBuf {
    pub fn new() -> Self {
        Self {
            buf: vec![0; 1024 * 8],
            wp: 0,
            rp: 0,
        }
    }

    pub fn reset(&mut self) {
        self.rp = 0;
        self.wp = 0;
    }

    pub fn len(&self) -> usize {
        self.wp - self.rp
    }

    pub fn adv(&mut self, n: usize) {
        self.rp += n;
    }

    pub fn data(&self) -> &[u8] {
        &self.buf[self.rp..self.wp]
    }

    pub async fn fill(&mut self, file: &mut File) -> Result<usize, Error> {
        if self.rp == self.wp {
            if self.rp != 0 {
                self.wp = 0;
                self.rp = 0;
            }
        } else {
            unsafe {
                std::ptr::copy::<u8>(&self.buf[self.rp], &mut self.buf[0], self.len());
                self.wp -= self.rp;
                self.rp = 0;
            }
        }
        let n = read(file, &mut self.buf[self.wp..]).await?;
        self.wp += n;
        return Ok(n);
    }

    pub async fn fill_if_low(&mut self, file: &mut File) -> Result<usize, Error> {
        let len = self.len();
        let cap = self.buf.len();
        while self.len() < cap / 6 {
            let n = self.fill(file).await?;
            if n == 0 {
                break;
            }
        }
        return Ok(self.len() - len);
    }

    pub async fn fill_min(&mut self, file: &mut File, min: usize) -> Result<usize, Error> {
        let len = self.len();
        while self.len() < min {
            let n = self.fill(file).await?;
            if n == 0 {
                break;
            }
        }
        return Ok(self.len() - len);
    }
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

fn readoffset(buf: &[u8], pos: usize) -> Offset {
    u64::from_be_bytes(buf.as_ref()[pos..pos + OFFSET_SIZE].try_into().unwrap())
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

pub async fn read_file_basics(file: &mut File) -> Result<IndexFileBasics, Error> {
    let mut buf = vec![0; 128];
    read_exact(file, &mut buf[0..4]).await?;
    let version = String::from_utf8(buf[3..4].to_vec())?.parse()?;
    if version == 3 {
        read_exact(file, &mut buf[4..88]).await?;
    } else if version == 2 {
        read_exact(file, &mut buf[4..48]).await?;
    } else {
        panic!();
    }
    //info!("\nread_file_basics\n{}", format_hex_block(&buf, 160));
    let b = &buf;
    if false {
        let s: String = b.iter().map(|x| format!(" {:02x}", *x)).collect();
        info!("\n\n{}", s);
    }
    if false {
        let mut i1 = 0x58 + 0x10 * 8;
        while i1 < 0x58 + 0x15 * 8 {
            let s: String = b[i1..i1 + 8].iter().map(|x| format!(" {:02x}", *x)).collect();
            info!("{}", s);
            i1 += 8;
        }
    }
    if false {
        info!("data:");
        let mut i1 = 0x2809;
        while i1 < 0x2880 {
            let s: String = b[i1..i1 + 8].iter().map(|x| format!(" {:02x}", *x)).collect();
            info!("{}", s);
            i1 += 8;
        }
        info!("{}", String::from_utf8_lossy(&b[0x2800..0x2880]));
    }
    let mut ret = if version == 3 {
        IndexFileBasics {
            version,
            name_hash_anchor_beg: readu64(b, 0x04),
            name_hash_anchor_len: readu32(b, 12) as u64,
            fa_used_list_len: readu64(b, 16),
            fa_used_list_beg: readu64(b, 24),
            fa_used_list_end: readu64(b, 32),
            fa_free_list_len: readu64(b, 40),
            fa_free_list_beg: readu64(b, 48),
            fa_free_list_end: readu64(b, 56),
            fa_header_len: readu64(b, 64),
            fa_header_prev: readu64(b, 72),
            fa_header_next: readu64(b, 80),
            name_hash_entries: vec![],
        }
    } else if version == 2 {
        IndexFileBasics {
            version,
            name_hash_anchor_beg: readu32(b, 4) as u64,
            name_hash_anchor_len: readu32(b, 8) as u64,
            fa_used_list_len: readu32(b, 12) as u64,
            fa_used_list_beg: readu32(b, 16) as u64,
            fa_used_list_end: readu32(b, 20) as u64,
            fa_free_list_len: readu32(b, 24) as u64,
            fa_free_list_beg: readu32(b, 28) as u64,
            fa_free_list_end: readu32(b, 32) as u64,
            fa_header_len: readu32(b, 36) as u64,
            fa_header_prev: readu32(b, 40) as u64,
            fa_header_next: readu32(b, 44) as u64,
            name_hash_entries: vec![],
        }
    } else {
        panic!();
    };
    info!("read_file_basics  w/o hashposs {:?}", ret);
    {
        if ret.name_hash_anchor_len > 2000 {
            return Err(Error::with_msg_no_trace(format!(
                "name_hash_anchor_len {}",
                ret.name_hash_anchor_len
            )));
        }
        let u = if version == 3 {
            ret.name_hash_anchor_len * 8
        } else if version == 2 {
            ret.name_hash_anchor_len * 4
        } else {
            panic!()
        };
        buf.resize(u as usize, 0);
        read_exact(file, &mut buf).await?;
        let b = &buf;
        for i1 in 0..ret.name_hash_anchor_len {
            let pos = if version == 3 {
                readu64(b, i1 as usize * 8)
            } else if version == 2 {
                readu32(b, i1 as usize * 4) as u64
            } else {
                panic!()
            };
            let e = NamedHashTableEntry {
                named_hash_channel_entry_pos: pos,
            };
            ret.name_hash_entries.push(e);
        }
    }
    Ok(ret)
}

#[derive(Debug)]
pub struct RTreeNodeRecord {
    ts1: Nanos,
    ts2: Nanos,
    // TODO should probably be better name `child or offset` and be made enum.
    child_or_id: Offset,
}

#[derive(Debug)]
pub struct RTreeNode {
    pos: FilePos,
    records: Vec<RTreeNodeRecord>,
    is_leaf: bool,
    rtree_m: usize,
}

#[derive(Debug)]
pub struct RTreeNodeAtRecord {
    node: RTreeNode,
    rix: usize,
}

impl RTreeNodeAtRecord {
    pub fn rec(&self) -> &RTreeNodeRecord {
        &self.node.records[self.rix]
    }
}

// TODO refactor as struct, rtree_m is a property of the tree.
pub async fn read_rtree_node(file: &mut File, pos: FilePos, rtree_m: usize) -> Result<RTreeNode, Error> {
    const OFF1: usize = 9;
    const RLEN: usize = 24;
    const NANO_MAX: u32 = 999999999;
    seek(file, SeekFrom::Start(pos.into())).await?;
    let mut rb = RingBuf::new();
    // TODO must know how much data I need at least...
    rb.fill_min(file, OFF1 + rtree_m * RLEN).await?;
    if false {
        let s = format_hex_block(rb.data(), 128);
        info!("RTREE NODE:\n{}", s);
    }
    if rb.len() < 1 + OFFSET_SIZE {
        return Err(Error::with_msg_no_trace("could not read enough"));
    }
    let b = rb.data();
    let is_leaf = b[0] != 0;
    let parent = readu64(b, 1);
    if false {
        info!("is_leaf: {}  parent: {}", is_leaf, parent);
    }
    let recs = (0..rtree_m)
        .into_iter()
        .filter_map(|i| {
            let off2 = OFF1 + i * RLEN;
            let ts1a = readu32(b, off2 + 0);
            let ts1b = readu32(b, off2 + 4);
            let ts2a = readu32(b, off2 + 8);
            let ts2b = readu32(b, off2 + 12);
            let ts1b = ts1b.min(NANO_MAX);
            let ts2b = ts2b.min(NANO_MAX);
            let ts1 = ts1a as u64 * SEC + ts1b as u64 + EPICS_EPOCH_OFFSET;
            let ts2 = ts2a as u64 * SEC + ts2b as u64 + EPICS_EPOCH_OFFSET;
            let child_or_id = readu64(b, off2 + 16);
            //info!("NODE   {} {}   {} {}   {}", ts1a, ts1b, ts2a, ts2b, child_or_id);
            if child_or_id != 0 {
                let rec = RTreeNodeRecord {
                    ts1: Nanos { ns: ts1 },
                    ts2: Nanos { ns: ts2 },
                    child_or_id,
                };
                Some(rec)
            } else {
                None
            }
        })
        .collect();
    let node = RTreeNode {
        pos,
        records: recs,
        is_leaf,
        rtree_m,
    };
    Ok(node)
}

pub async fn read_rtree_entrypoint(file: &mut File, pos: u64, _basics: &IndexFileBasics) -> Result<RTreeNode, Error> {
    seek(file, SeekFrom::Start(pos)).await?;
    let mut rb = RingBuf::new();
    // TODO should be able to indicate:
    // • how much I need at most before I know that I will e.g. seek or abort.
    rb.fill_min(file, OFFSET_SIZE + 4).await?;
    if rb.len() < OFFSET_SIZE + 4 {
        return Err(Error::with_msg_no_trace("could not read enough"));
    }
    let b = rb.data();
    let node_offset = readu64(b, 0);
    let rtree_m = readu32(b, OFFSET_SIZE);
    //info!("node_offset: {}  rtree_m: {}", node_offset, rtree_m);
    let pos = FilePos { pos: node_offset };
    let node = read_rtree_node(file, pos, rtree_m as usize).await?;
    //info!("read_rtree_entrypoint   READ ROOT NODE: {:?}", node);
    Ok(node)
}

#[derive(Debug)]
pub struct TreeSearchStats {
    duration: Duration,
    node_reads: usize,
}

impl TreeSearchStats {
    fn new(ts1: Instant, node_reads: usize) -> Self {
        Self {
            duration: Instant::now().duration_since(ts1),
            node_reads,
        }
    }
}

pub async fn search_record(
    file: &mut File,
    rtree_m: usize,
    start_node_pos: FilePos,
    beg: Nanos,
) -> Result<(Option<RTreeNodeAtRecord>, TreeSearchStats), Error> {
    let ts1 = Instant::now();
    let mut node = read_rtree_node(file, start_node_pos, rtree_m).await?;
    let mut node_reads = 1;
    'outer: loop {
        let nr = node.records.len();
        for (i, rec) in node.records.iter().enumerate() {
            //info!("looking at record  i {}", i);
            if rec.ts2.ns > beg.ns {
                if node.is_leaf {
                    trace!("found leaf match at {} / {}", i, nr);
                    let ret = RTreeNodeAtRecord { node, rix: i };
                    let stats = TreeSearchStats::new(ts1, node_reads);
                    return Ok((Some(ret), stats));
                } else {
                    trace!("found non-leaf match at {} / {}", i, nr);
                    let pos = FilePos { pos: rec.child_or_id };
                    node = read_rtree_node(file, pos, rtree_m).await?;
                    node_reads += 1;
                    continue 'outer;
                }
            }
        }
        {
            let stats = TreeSearchStats::new(ts1, node_reads);
            return Ok((None, stats));
        }
    }
}

#[derive(Debug)]
pub struct ChannelInfoBasics {
    channel_name: String,
    rtree_m: usize,
    rtree_start_pos: FilePos,
}

pub async fn read_channel(index_file: &mut File, channel_name: &str) -> Result<Option<ChannelInfoBasics>, Error> {
    let basics = read_file_basics(index_file).await?;
    let hver2 = HeaderVersion2;
    let hver3 = HeaderVersion3;
    let hver: &dyn HeaderVersion = if basics.version == 3 {
        &hver3
    } else if basics.version == 2 {
        &hver2
    } else {
        panic!()
    };
    let chn_hash = name_hash(channel_name, basics.name_hash_anchor_len as u32);
    let epos = &basics.name_hash_entries[chn_hash as usize];
    let mut entries = vec![];
    let mut rb = RingBuf::new();
    let mut pos = epos.named_hash_channel_entry_pos;
    loop {
        rb.reset();
        seek(index_file, SeekFrom::Start(pos)).await?;
        let fill_min = if hver.offset_size() == 8 { 20 } else { 12 };
        rb.fill_min(index_file, fill_min).await?;
        if rb.len() < fill_min {
            warn!("not enough data to continue reading channel list from name hash list");
            break;
        }
        let buf = rb.data();
        let e = parse_name_hash_channel_entry(buf, hver)?;
        let next = e.next;
        entries.push(e);
        if next == 0 {
            break;
        } else {
            pos = next;
        }
    }
    for e in &entries {
        if e.channel_name == channel_name {
            let ep = read_rtree_entrypoint(index_file, e.id_rtree_pos, &basics).await?;
            let ret = ChannelInfoBasics {
                channel_name: channel_name.into(),
                rtree_m: ep.rtree_m,
                rtree_start_pos: ep.pos,
            };
            return Ok(Some(ret));
        }
    }
    Ok(None)
}

pub trait HeaderVersion: Send + Sync {
    fn version(&self) -> u8;
    fn read_offset(&self, buf: &[u8], pos: usize) -> u64;
    fn offset_size(&self) -> usize;
}

struct HeaderVersion2;

impl HeaderVersion for HeaderVersion2 {
    fn version(&self) -> u8 {
        2
    }

    fn read_offset(&self, buf: &[u8], pos: usize) -> u64 {
        readu32(buf, pos) as u64
    }

    fn offset_size(&self) -> usize {
        4
    }
}

struct HeaderVersion3;

impl HeaderVersion for HeaderVersion3 {
    fn version(&self) -> u8 {
        3
    }

    fn read_offset(&self, buf: &[u8], pos: usize) -> u64 {
        readu64(buf, pos)
    }

    fn offset_size(&self) -> usize {
        8
    }
}

fn parse_name_hash_channel_entry(buf: &[u8], hver: &dyn HeaderVersion) -> Result<NamedHashChannelEntry, Error> {
    let mut p1 = 0;
    let next = hver.read_offset(&buf, p1);
    p1 += hver.offset_size();
    let id = hver.read_offset(&buf, p1);
    p1 += hver.offset_size();
    let name_len = readu16(&buf, p1);
    p1 += 2;
    let id_txt_len = readu16(&buf, p1);
    p1 += 2;
    if next > 1024 * 1024 * 1024 * 1024 || id > 1024 * 1024 * 1024 * 1024 || name_len > 128 || id_txt_len > 128 {
        error!(
            "something bad: parse_name_hash_channel_entry  next {}  id {}  name_len {} id_txt_len {}",
            next, id, name_len, id_txt_len
        );
        return Err(Error::with_msg_no_trace("bad hash table entry"));
    }
    let n1 = name_len as usize;
    let n2 = id_txt_len as usize;
    let channel_name_found = String::from_utf8(buf[p1..p1 + n1].to_vec())?;
    p1 += n1;
    let id_txt = String::from_utf8(buf[p1..p1 + n2].to_vec())?;
    p1 += n2;
    let _ = p1;
    let e = NamedHashChannelEntry {
        next,
        id_rtree_pos: id,
        channel_name: channel_name_found,
        id_txt,
    };
    Ok(e)
}

async fn channel_list_from_index_name_hash_list(
    file: &mut File,
    pos: FilePos,
    hver: &dyn HeaderVersion,
) -> Result<Vec<NamedHashChannelEntry>, Error> {
    let mut pos = pos;
    let mut ret = vec![];
    let mut rb = RingBuf::new();
    loop {
        rb.reset();
        seek(file, SeekFrom::Start(pos.pos)).await?;
        let fill_min = if hver.offset_size() == 8 { 20 } else { 12 };
        rb.fill_min(file, fill_min).await?;
        if rb.len() < fill_min {
            warn!("not enough data to continue reading channel list from name hash list");
            break;
        }
        let e = parse_name_hash_channel_entry(rb.data(), hver)?;
        let next = e.next;
        ret.push(e);
        if next == 0 {
            break;
        } else {
            pos.pos = next;
        }
    }
    Ok(ret)
}

pub async fn channel_list(index_path: PathBuf) -> Result<Vec<String>, Error> {
    let mut ret = vec![];
    let file = &mut open_read(index_path.clone()).await?;
    let basics = read_file_basics(file).await?;
    let hver2 = HeaderVersion2;
    let hver3 = HeaderVersion3;
    let hver: &dyn HeaderVersion = if basics.version == 2 {
        &hver2
    } else if basics.version == 3 {
        &hver3
    } else {
        return Err(Error::with_msg_no_trace(format!(
            "unexpected version {}",
            basics.version
        )));
    };
    for (_i, name_hash_entry) in basics.name_hash_entries.iter().enumerate() {
        if name_hash_entry.named_hash_channel_entry_pos != 0 {
            let pos = FilePos {
                pos: name_hash_entry.named_hash_channel_entry_pos,
            };
            let list = channel_list_from_index_name_hash_list(file, pos, hver).await?;
            for e in list {
                ret.push(e.channel_name);
            }
        }
    }
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

#[derive(Debug)]
pub struct Datablock {
    next: Offset,
    data_header_pos: Offset,
    fname: String,
}

impl Datablock {
    fn file_name(&self) -> &str {
        &self.fname
    }

    fn data_header_pos(&self) -> DataHeaderPos {
        DataHeaderPos(self.data_header_pos)
    }
}

async fn read_index_datablockref(file: &mut File, pos: FilePos) -> Result<Datablock, Error> {
    seek(file, SeekFrom::Start(pos.pos)).await?;
    let mut rb = RingBuf::new();
    rb.fill_min(file, 18).await?;
    let buf = rb.data();
    let next = readoffset(buf, 0);
    let data = readoffset(buf, 8);
    let len = readu16(buf, 16) as usize;
    rb.fill_min(file, 18 + len).await?;
    let buf = rb.data();
    let fname = String::from_utf8(buf[18..18 + len].to_vec())?;
    let ret = Datablock {
        next,
        data_header_pos: data,
        fname,
    };
    Ok(ret)
}

#[derive(Debug)]
enum DbrType {
    DbrString = 0,
    DbrInt = 1,
    DbrStsFloat = 9,
    DbrTimeDouble = 20,
}

impl DbrType {
    fn from_u16(k: u16) -> Result<Self, Error> {
        use DbrType::*;
        let res = match k {
            0 => DbrString,
            1 => DbrInt,
            9 => DbrStsFloat,
            20 => DbrTimeDouble,
            _ => {
                let msg = format!("not a valid/supported dbr type: {}", k);
                return Err(Error::with_msg_no_trace(msg));
            }
        };
        Ok(res)
    }

    #[allow(dead_code)]
    fn byte_len(&self) -> usize {
        use DbrType::*;
        match self {
            DbrString => 0,
            DbrInt => 4,
            DbrStsFloat => 1,
            DbrTimeDouble => 16,
        }
    }
}

#[derive(Debug)]
pub struct DatafileHeader {
    pos: DataHeaderPos,
    dir_offset: u32,
    // Should be absolute file position of the next data header
    // together with `fname_next`.
    // But unfortunately not always set?
    next_offset: u32,
    prev_offset: u32,
    curr_offset: u32,
    num_samples: u32,
    ctrl_info_offset: u32,
    buf_size: u32,
    buf_free: u32,
    dbr_type: DbrType,
    dbr_count: usize,
    period: f64,
    ts_beg: Nanos,
    ts_end: Nanos,
    ts_next_file: Nanos,
    fname_next: String,
    fname_prev: String,
}

const DATA_HEADER_LEN_ON_DISK: usize = 72 + 40 + 40;

async fn read_datafile_header(file: &mut File, pos: DataHeaderPos) -> Result<DatafileHeader, Error> {
    seek(file, SeekFrom::Start(pos.0)).await?;
    let mut rb = RingBuf::new();
    rb.fill_min(file, DATA_HEADER_LEN_ON_DISK).await?;
    let buf = rb.data();
    let dir_offset = readu32(buf, 0);
    let next_offset = readu32(buf, 4);
    let prev_offset = readu32(buf, 8);
    let curr_offset = readu32(buf, 12);
    let num_samples = readu32(buf, 16);
    let ctrl_info_offset = readu32(buf, 20);
    let buf_size = readu32(buf, 24);
    let buf_free = readu32(buf, 28);
    let dbr_type = DbrType::from_u16(readu16(buf, 32))?;
    let dbr_count = readu16(buf, 34);
    // 4 bytes padding.
    let period = readf64(buf, 40);
    let ts1a = readu32(buf, 48);
    let ts1b = readu32(buf, 52);
    let ts2a = readu32(buf, 56);
    let ts2b = readu32(buf, 60);
    let ts3a = readu32(buf, 64);
    let ts3b = readu32(buf, 68);
    let ts_beg = if ts1a != 0 || ts1b != 0 {
        ts1a as u64 * SEC + ts1b as u64 + EPICS_EPOCH_OFFSET
    } else {
        0
    };
    let ts_end = if ts3a != 0 || ts3b != 0 {
        ts3a as u64 * SEC + ts3b as u64 + EPICS_EPOCH_OFFSET
    } else {
        0
    };
    let ts_next_file = if ts2a != 0 || ts2b != 0 {
        ts2a as u64 * SEC + ts2b as u64 + EPICS_EPOCH_OFFSET
    } else {
        0
    };
    let fname_prev = read_string(&buf[72..112])?;
    let fname_next = read_string(&buf[112..152])?;
    let ret = DatafileHeader {
        pos,
        dir_offset,
        next_offset,
        prev_offset,
        curr_offset,
        num_samples,
        ctrl_info_offset,
        buf_size,
        buf_free,
        dbr_type,
        dbr_count: dbr_count as usize,
        period,
        ts_beg: Nanos { ns: ts_beg },
        ts_end: Nanos { ns: ts_end },
        ts_next_file: Nanos { ns: ts_next_file },
        fname_next,
        fname_prev,
    };
    Ok(ret)
}

async fn read_data_1(file: &mut File, datafile_header: &DatafileHeader) -> Result<EventsItem, Error> {
    let dhpos = datafile_header.pos.0 + DATA_HEADER_LEN_ON_DISK as u64;
    seek(file, SeekFrom::Start(dhpos)).await?;
    let res = match &datafile_header.dbr_type {
        DbrType::DbrTimeDouble => {
            if datafile_header.dbr_count == 1 {
                info!("~~~~~~~~~~~~~~~~~~~~~   read  scalar  DbrTimeDouble");
                let mut evs = EventValues {
                    tss: vec![],
                    values: vec![],
                };
                let n1 = datafile_header.num_samples as usize;
                //let n2 = datafile_header.dbr_type.byte_len();
                let n2 = 2 + 2 + 4 + 4 + (4) + 8;
                let n3 = n1 * n2;
                let mut buf = vec![0; n3];
                read_exact(file, &mut buf).await?;
                let mut p1 = 0;
                while p1 < n3 - n2 {
                    let _status = u16::from_be_bytes(buf[p1..p1 + 2].try_into().unwrap());
                    p1 += 2;
                    let _severity = u16::from_be_bytes(buf[p1..p1 + 2].try_into().unwrap());
                    p1 += 2;
                    let ts1a = u32::from_be_bytes(buf[p1..p1 + 4].try_into().unwrap());
                    p1 += 4;
                    let ts1b = u32::from_be_bytes(buf[p1..p1 + 4].try_into().unwrap());
                    p1 += 4;
                    let ts1 = ts1a as u64 * SEC + ts1b as u64 + EPICS_EPOCH_OFFSET;
                    p1 += 4;
                    let value = f64::from_be_bytes(buf[p1..p1 + 8].try_into().unwrap());
                    p1 += 8;
                    //info!("read event  {}  {}  {}  {}  {}", status, severity, ts1a, ts1b, value);
                    evs.tss.push(ts1);
                    evs.values.push(value);
                }
                let evs = ScalarPlainEvents::Double(evs);
                let plain = PlainEvents::Scalar(evs);
                let item = EventsItem::Plain(plain);
                item
            } else {
                // 1d shape
                err::todoval()
            }
        }
        _ => err::todoval(),
    };
    Ok(res)
}

pub fn list_index_files(node: &ChannelArchiver) -> Receiver<Result<PathBuf, Error>> {
    let node = node.clone();
    let (tx, rx) = async_channel::bounded(4);
    let tx2 = tx.clone();
    let task = async move {
        for bp in &node.data_base_paths {
            let mut rd = read_dir(bp).await?;
            while let Some(e) = rd.next_entry().await? {
                let ft = e.file_type().await?;
                if ft.is_dir() {
                    let mut rd = read_dir(e.path()).await?;
                    while let Some(e) = rd.next_entry().await? {
                        let ft = e.file_type().await?;
                        if false && ft.is_dir() {
                            let mut rd = read_dir(e.path()).await?;
                            while let Some(e) = rd.next_entry().await? {
                                let ft = e.file_type().await?;
                                if ft.is_file() {
                                    if e.file_name().to_string_lossy() == "index" {
                                        tx.send(Ok(e.path())).await?;
                                    }
                                }
                            }
                        } else if ft.is_file() {
                            if e.file_name().to_string_lossy() == "index" {
                                tx.send(Ok(e.path())).await?;
                            }
                        }
                    }
                } else if ft.is_file() {
                    if e.file_name().to_string_lossy() == "index" {
                        tx.send(Ok(e.path())).await?;
                    }
                }
            }
        }
        Ok::<_, Error>(())
    };
    wrap_task(task, tx2);
    rx
}

fn wrap_task<T, O1, O2>(task: T, tx: Sender<Result<O2, Error>>)
where
    T: Future<Output = Result<O1, Error>> + Send + 'static,
    O1: Send + 'static,
    O2: Send + 'static,
{
    let task = async move {
        match task.await {
            Ok(_) => {}
            Err(e) => {
                match tx.send(Err(e)).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("wrap_task can not forward error: {:?}", e);
                    }
                };
            }
        }
    };
    taskrun::spawn(task);
}

pub fn list_all_channels(node: &ChannelArchiver) -> Receiver<Result<String, Error>> {
    let node = node.clone();
    let (tx, rx) = async_channel::bounded(4);
    let tx2 = tx.clone();
    let task = async move {
        let mut ixf = list_index_files(&node);
        while let Some(f) = ixf.next().await {
            let index_path = f?;
            if !index_path.to_str().unwrap().contains("archive_X02DA_LO/20130101/index") {
                //continue;
            }
            info!("try to read for {:?}", index_path);
            //continue;
            let channels = channel_list(index_path).await?;
            info!("list_all_channels  emit {} channels", channels.len());
            for ch in channels {
                tx.send(Ok(ch)).await?;
            }
        }
        info!("list_all_channels DONE");
        Ok::<_, Error>(())
    };
    wrap_task(task, tx2);
    rx
}

pub async fn channel_config(q: &ChannelConfigQuery, conf: &ChannelArchiver) -> Result<ChannelConfigResponse, Error> {
    let mut type_info = None;
    let mut stream = datablockstream::DatablockStream::for_channel_range(
        q.range.clone(),
        q.channel.clone(),
        conf.data_base_paths.clone().into(),
        true,
    );
    while let Some(item) = stream.next().await {
        match item {
            Ok(k) => match k {
                StreamItem::DataItem(k) => match k {
                    RangeCompletableItem::RangeComplete => (),
                    RangeCompletableItem::Data(k) => {
                        type_info = Some(k.type_info());
                        break;
                    }
                },
                StreamItem::Log(_) => (),
                StreamItem::Stats(_) => (),
            },
            Err(e) => {
                error!("{}", e);
                ()
            }
        }
    }
    if type_info.is_none() {
        let mut stream = datablockstream::DatablockStream::for_channel_range(
            q.range.clone(),
            q.channel.clone(),
            conf.data_base_paths.clone().into(),
            false,
        );
        while let Some(item) = stream.next().await {
            match item {
                Ok(k) => match k {
                    StreamItem::DataItem(k) => match k {
                        RangeCompletableItem::RangeComplete => (),
                        RangeCompletableItem::Data(k) => {
                            type_info = Some(k.type_info());
                            break;
                        }
                    },
                    StreamItem::Log(_) => (),
                    StreamItem::Stats(_) => (),
                },
                Err(e) => {
                    error!("{}", e);
                    ()
                }
            }
        }
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
    // TODO move RangeFilter to a different crate (items?)
    // because the `disk` crate should become the specific sf-databuffer reader engine.

    //use disk::rangefilter::RangeFilter;
    //use disk::{eventblobs::EventChunkerMultifile, eventchunker::EventChunkerConf};

    use super::search_record;
    use crate::archeng::{open_read, read_channel, read_data_1, read_file_basics, read_index_datablockref};
    use crate::archeng::{read_datafile_header, EPICS_EPOCH_OFFSET};
    use err::Error;
    use netpod::log::*;
    use netpod::timeunits::*;
    use netpod::FilePos;
    use netpod::Nanos;
    use std::path::PathBuf;

    /*
    Root node: most left record ts1 965081099942616289, most right record ts2 1002441959876114632
    */
    const CHN_0_MASTER_INDEX: &str = "/data/daqbuffer-testdata/sls/gfa03/bl_arch/archive_X05DA_SH/index";

    #[test]
    fn read_file_basic_info() -> Result<(), Error> {
        let fut = async {
            let mut f1 = open_read(CHN_0_MASTER_INDEX.into()).await?;
            let res = read_file_basics(&mut f1).await?;
            assert_eq!(res.version, 3);
            assert_eq!(res.name_hash_anchor_beg, 88);
            assert_eq!(res.name_hash_anchor_len, 1009);
            // TODO makes no sense:
            assert_eq!(res.fa_used_list_beg, 2611131);
            assert_eq!(res.fa_used_list_end, 64);
            assert_eq!(res.fa_used_list_len, 2136670);
            assert_eq!(res.fa_header_next, 8160);
            assert_eq!(res.fa_header_prev, 0);
            assert_eq!(res.fa_header_len, 8072);
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }

    #[test]
    fn read_for_channel() -> Result<(), Error> {
        let fut = async {
            let mut index_file = open_read(CHN_0_MASTER_INDEX.into()).await?;
            let channel_name = "X05DA-FE-WI1:TC1";
            let res = read_channel(&mut index_file, channel_name).await?;
            assert_eq!(res.is_some(), true);
            let res = res.unwrap();
            assert_eq!(res.channel_name, channel_name);
            assert_eq!(res.rtree_m, 50);
            assert_eq!(res.rtree_start_pos.pos, 329750);
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }

    #[test]
    fn search_record_middle() -> Result<(), Error> {
        /*
        These times are still without EPICS_EPOCH_OFFSET.
        RTreeNodeRecord { ts1: 969729779686636130, ts2: 970351442331056677, child_or_id: 130731 },
        RTreeNodeRecord { ts1: 970351499684884156, ts2: 970417919634086480, child_or_id: 185074 },
        RTreeNodeRecord { ts1: 970417979635219603, ts2: 970429859806669835, child_or_id: 185015 },
        */
        let fut = async {
            let index_path: PathBuf = CHN_0_MASTER_INDEX.into();
            let mut index_file = open_read(index_path.clone()).await?;
            let channel_name = "X05DA-FE-WI1:TC1";
            const T0: u64 = 970351442331056677 + 1 + EPICS_EPOCH_OFFSET;
            let beg = Nanos { ns: T0 };
            let res = read_channel(&mut index_file, channel_name).await?;
            let cib = res.unwrap();
            let (res, _stats) = search_record(&mut index_file, cib.rtree_m, cib.rtree_start_pos, beg).await?;
            assert_eq!(res.is_some(), true);
            let res = res.unwrap();
            assert_eq!(res.node.is_leaf, true);
            assert_eq!(res.node.pos.pos, 8216);
            assert_eq!(res.rix, 17);
            let rec = &res.node.records[res.rix];
            assert_eq!(rec.ts1.ns, 970351499684884156 + EPICS_EPOCH_OFFSET);
            assert_eq!(rec.ts2.ns, 970417919634086480 + EPICS_EPOCH_OFFSET);
            assert_eq!(rec.child_or_id, 185074);
            let pos = FilePos { pos: rec.child_or_id };
            let datablock = read_index_datablockref(&mut index_file, pos).await?;
            assert_eq!(datablock.data_header_pos, 52787);
            assert_eq!(datablock.fname, "20201001/20201001");
            // The actual datafile for that time was not retained any longer.
            // But the index still points to that.
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }

    #[test]
    fn search_record_data() -> Result<(), Error> {
        let fut = async {
            let index_path: PathBuf = CHN_0_MASTER_INDEX.into();
            let mut index_file = open_read(index_path.clone()).await?;
            let channel_name = "X05DA-FE-WI1:TC1";
            const T0: u64 = 1002000000 * SEC + EPICS_EPOCH_OFFSET;
            let beg = Nanos { ns: T0 };
            let res = read_channel(&mut index_file, channel_name).await?;
            let cib = res.unwrap();
            let (res, _stats) = search_record(&mut index_file, cib.rtree_m, cib.rtree_start_pos, beg).await?;
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
            let datablock = read_index_datablockref(&mut index_file, pos).await?;
            assert_eq!(datablock.data_header_pos().0, 9311367);
            assert_eq!(datablock.file_name(), "20211001/20211001");
            let data_path = index_path.parent().unwrap().join(datablock.file_name());
            let mut data_file = open_read(data_path).await?;
            let datafile_header = read_datafile_header(&mut data_file, datablock.data_header_pos()).await?;
            let events = read_data_1(&mut data_file, &datafile_header).await?;
            info!("read events: {:?}", events);
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }

    // Note: this tests only the index tree, but it does not look for any actual event in some file.
    #[test]
    fn search_record_at_beg() -> Result<(), Error> {
        let fut = async {
            let mut index_file = open_read(CHN_0_MASTER_INDEX.into()).await?;
            let channel_name = "X05DA-FE-WI1:TC1";
            const T0: u64 = 965081099942616289 + EPICS_EPOCH_OFFSET;
            let beg = Nanos { ns: T0 };
            let res = read_channel(&mut index_file, channel_name).await?;
            let cib = res.unwrap();
            let (res, _stats) = search_record(&mut index_file, cib.rtree_m, cib.rtree_start_pos, beg).await?;
            assert_eq!(res.is_some(), true);
            let res = res.unwrap();
            assert_eq!(res.node.is_leaf, true);
            assert_eq!(res.node.pos.pos, 8216);
            assert_eq!(res.rix, 0);
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }

    // Note: this tests only the index tree, but it does not look for any actual event in some file.
    #[test]
    fn search_record_at_end() -> Result<(), Error> {
        let fut = async {
            let mut index_file = open_read(CHN_0_MASTER_INDEX.into()).await?;
            let channel_name = "X05DA-FE-WI1:TC1";
            const T0: u64 = 1002441959876114632 - 1 + EPICS_EPOCH_OFFSET;
            let beg = Nanos { ns: T0 };
            let res = read_channel(&mut index_file, channel_name).await?;
            let cib = res.unwrap();
            let (res, _stats) = search_record(&mut index_file, cib.rtree_m, cib.rtree_start_pos, beg).await?;
            assert_eq!(res.is_some(), true);
            let res = res.unwrap();
            assert_eq!(res.node.pos.pos, 1861178);
            assert_eq!(res.rix, 46);
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }

    // Note: this tests only the index tree, but it does not look for any actual event in some file.
    #[test]
    fn search_record_beyond_end() -> Result<(), Error> {
        let fut = async {
            let mut index_file = open_read(CHN_0_MASTER_INDEX.into()).await?;
            let channel_name = "X05DA-FE-WI1:TC1";
            const T0: u64 = 1002441959876114632 - 0 + EPICS_EPOCH_OFFSET;
            let beg = Nanos { ns: T0 };
            let res = read_channel(&mut index_file, channel_name).await?;
            let cib = res.unwrap();
            let (res, _stats) = search_record(&mut index_file, cib.rtree_m, cib.rtree_start_pos, beg).await?;
            assert_eq!(res.is_none(), true);
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }
}
