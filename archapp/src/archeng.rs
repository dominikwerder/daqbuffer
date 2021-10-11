use err::Error;
use netpod::timeunits::SEC;
use netpod::{log::*, Nanos};
use std::convert::TryInto;
use std::io::{self, SeekFrom};
use std::path::PathBuf;
use std::time::Instant;
use tokio::fs::{File, OpenOptions};
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

async fn open_read(path: PathBuf) -> io::Result<File> {
    let ts1 = Instant::now();
    let res = OpenOptions::new().read(true).open(path).await;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1).as_secs_f64() * 1e3;
    info!("timed open_read  dt: {:.3} ms", dt);
    res
}

async fn seek(file: &mut File, pos: SeekFrom) -> io::Result<u64> {
    let ts1 = Instant::now();
    let res = file.seek(pos).await;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1).as_secs_f64() * 1e3;
    info!("timed seek  dt: {:.3} ms", dt);
    res
}

async fn read(file: &mut File, buf: &mut [u8]) -> io::Result<usize> {
    let ts1 = Instant::now();
    let res = file.read(buf).await;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1).as_secs_f64() * 1e3;
    info!("timed read  dt: {:.3} ms  res: {:?}", dt, res);
    res
}

async fn read_exact(file: &mut File, buf: &mut [u8]) -> io::Result<usize> {
    let ts1 = Instant::now();
    let res = file.read_exact(buf).await;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1).as_secs_f64() * 1e3;
    info!("timed read_exact  dt: {:.3} ms  res: {:?}", dt, res);
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

impl IndexFileBasics {
    pub fn file_offset_size(&self) -> u64 {
        if self.version == 3 {
            64
        } else if self.version == 2 {
            32
        } else {
            panic!()
        }
    }

    pub async fn read_named_hash_channel_entry(&self, file: &mut File) -> Result<(), Error> {
        Ok(())
    }
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

fn readu64(buf: &[u8], pos: usize) -> u64 {
    let pos = pos as usize;
    u64::from_be_bytes(buf.as_ref()[pos..pos + 8].try_into().unwrap())
}

fn readu32(buf: &[u8], pos: usize) -> u32 {
    let pos = pos as usize;
    u32::from_be_bytes(buf.as_ref()[pos..pos + 4].try_into().unwrap())
}

fn readu16(buf: &[u8], pos: usize) -> u16 {
    let pos = pos as usize;
    u16::from_be_bytes(buf.as_ref()[pos..pos + 2].try_into().unwrap())
}

pub async fn read_file_basics(f1: &mut File) -> Result<IndexFileBasics, Error> {
    let mut buf = vec![0; 0x58];
    read_exact(f1, &mut buf).await?;
    let version = String::from_utf8(buf[3..4].to_vec())?.parse()?;
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
    let mut ret = IndexFileBasics {
        version,
        name_hash_anchor_beg: readu64(b, 0x04),
        name_hash_anchor_len: readu32(b, 0x0c) as u64,
        fa_used_list_len: readu64(b, 0x10),
        fa_used_list_beg: readu64(b, 0x18),
        fa_used_list_end: readu64(b, 0x20),
        fa_free_list_len: readu64(b, 0x28),
        fa_free_list_beg: readu64(b, 0x30),
        fa_free_list_end: readu64(b, 0x38),
        fa_header_len: readu64(b, 0x40),
        fa_header_prev: readu64(b, 0x48),
        fa_header_next: readu64(b, 0x50),
        name_hash_entries: vec![],
    };
    info!("IndexFileBasics: {:?}", ret);
    if true {
        let u = ret.name_hash_anchor_len * 8;
        buf.resize(u as usize, 0);
        read_exact(f1, &mut buf).await?;
        let b = &buf;
        for i1 in 0..ret.name_hash_anchor_len {
            let pos = readu64(b, i1 as usize * 8);
            ret.name_hash_entries.push(NamedHashTableEntry {
                named_hash_channel_entry_pos: pos,
            });
        }
    }
    Ok(ret)
}

#[derive(Debug)]
pub struct RTreeNodeRecord {
    ts1: u64,
    ts2: u64,
    child_or_id: Offset,
}

#[derive(Debug)]
pub struct RTreeNode {
    records: Vec<RTreeNodeRecord>,
}

// TODO refactor as struct, rtree_m is a property of the tree.
pub async fn read_rtree_node(file: &mut File, pos: u64, rtree_m: usize) -> Result<RTreeNode, Error> {
    const OFF1: usize = 9;
    const RLEN: usize = 24;
    const NANO_MAX: u32 = 999999999;
    seek(file, SeekFrom::Start(pos)).await?;
    let mut rb = RingBuf::new();
    // TODO must know how much data I need at least...
    rb.fill_min(file, OFF1 + rtree_m * RLEN).await?;
    let s = format_hex_block(rb.data(), 128);
    info!("RTREE NODE:\n{}", s);
    if rb.len() < 1 + OFFSET_SIZE {
        return Err(Error::with_msg_no_trace("could not read enough"));
    }
    let b = rb.data();
    let is_leaf = b[0] != 0;
    let parent = readu64(b, 1);
    info!("is_leaf: {}  parent: {}", is_leaf, parent);
    let recs = (0..rtree_m)
        .into_iter()
        .map(|i| {
            let off2 = OFF1 + i * RLEN;
            let ts1a = readu32(b, off2 + 0);
            let ts1b = readu32(b, off2 + 4);
            let ts2a = readu32(b, off2 + 8);
            let ts2b = readu32(b, off2 + 12);
            let ts1b = ts1b.min(NANO_MAX);
            let ts2b = ts2b.min(NANO_MAX);
            let ts1 = ts1a as u64 * SEC + ts1b as u64;
            let ts2 = ts2a as u64 * SEC + ts2b as u64;
            let child_or_id = readu64(b, off2 + 16);
            //info!("NODE   {} {}   {} {}   {}", ts1a, ts1b, ts2a, ts2b, child_or_id);
            let rec = RTreeNodeRecord { ts1, ts2, child_or_id };
            rec
        })
        .collect();
    let node = RTreeNode { records: recs };
    Ok(node)
}

pub async fn read_rtree_entrypoint(file: &mut File, pos: u64, _basics: &IndexFileBasics) -> Result<(), Error> {
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
    info!("node_offset: {}  rtree_m: {}", node_offset, rtree_m);
    read_rtree_node(file, node_offset, rtree_m as usize).await?;
    Ok(())
}

// TODO
// Implement search as tail recursion or loop.
// What is a good new type to identify a node? the location in the file? Use a new type.
pub async fn search(file: &mut File, rtree_m: usize, start_node_pos: u64, beg: Nanos) -> Result<Option<String>, Error> {
    let node = read_rtree_node(file, start_node_pos, rtree_m).await?;
    if beg.ns < node.records[0].ts1 {
        None
    } else {
    };
    Ok(())
}

pub async fn read_channel(index_file: &mut File, channel_name: &str) -> Result<(), Error> {
    // TODO
    // How do I locate the correct index file?
    // Given a channel name, how do I find the master index?

    let f1 = index_file;
    let basics = read_file_basics(f1).await?;
    if false {
        info!("got basics: {:?}", basics);
    }
    let chn_hash = name_hash(channel_name, basics.name_hash_anchor_len as u32);
    info!("channel hash: {:08x}", chn_hash);
    let epos = &basics.name_hash_entries[chn_hash as usize];
    info!("table-entry: {:?}", epos);
    let mut entries = vec![];
    seek(f1, SeekFrom::Start(epos.named_hash_channel_entry_pos)).await?;
    let mut rb = RingBuf::new();
    loop {
        rb.fill_if_low(f1).await?;
        if rb.len() < 20 {
            warn!("break because not enough data");
            break;
        }
        let p1 = 0x00;
        let buf = rb.data();
        let next = readu64(&buf, p1 + 0);
        let id = readu64(&buf, p1 + 8);
        let name_len = readu16(&buf, p1 + 16);
        let id_txt_len = readu16(&buf, p1 + 18);
        let n0 = 20;
        let n1 = name_len as usize;
        let n2 = id_txt_len as usize;
        let channel_name = String::from_utf8(buf[n0..n0 + n1].to_vec())?;
        let id_txt = String::from_utf8(buf[n0 + n1..n0 + n1 + n2].to_vec())?;
        let e = NamedHashChannelEntry {
            next,
            id_rtree_pos: id,
            channel_name,
            id_txt,
        };
        entries.push(e);
        if next == 0 {
            info!("break because no next");
            break;
        } else if next > 1024 * 1024 * 1 {
            warn!("suspicious `next` {}", next);
            return Err(Error::with_msg_no_trace("bad next"));
        } else {
            rb.adv(next as usize);
        }
    }
    info!("parsed {} items", entries.len());

    if entries.len() > 0 {
        let e = &entries[0];
        read_rtree_entrypoint(f1, e.id_rtree_pos, &basics).await?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    // TODO move RangeFilter to a different crate (items?)
    // because the `disk` crate should become the specific sf-databuffer reader engine.

    //use disk::rangefilter::RangeFilter;
    //use disk::{eventblobs::EventChunkerMultifile, eventchunker::EventChunkerConf};

    use crate::archeng::{open_read, read_channel, read_file_basics};
    use err::Error;
    use futures_util::StreamExt;
    use items::{RangeCompletableItem, StreamItem};
    use netpod::log::*;
    use netpod::timeunits::{DAY, MS};
    use netpod::{ByteSize, ChannelConfig, FileIoBufferSize, Nanos};
    use std::path::PathBuf;

    fn open_index_inner(path: impl Into<PathBuf>) -> Result<(), Error> {
        let task = async move { Ok(()) };
        Ok(taskrun::run(task).unwrap())
    }

    const CHN_0_MASTER_INDEX: &str = "/data/daqbuffer-testdata/sls/gfa03/bl_arch/archive_X05DA_SH/index";

    #[test]
    fn read_file_basic_info() -> Result<(), Error> {
        let fut = async {
            let mut f1 = open_read(CHN_0_MASTER_INDEX.into()).await?;
            let res = read_file_basics(&mut f1).await?;
            info!("got {:?}", res);
            assert_eq!(res.version, 3);
            assert_eq!(res.file_offset_size(), 64);
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
            info!("got {:?}", res);
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }
}
