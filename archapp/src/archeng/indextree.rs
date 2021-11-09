use crate::archeng::{format_hex_block, name_hash, readu16, readu32, readu64, StatsChannel, EPICS_EPOCH_OFFSET};
use commonio::open_read;
use commonio::ringbuf::RingBuf;
use err::Error;
use netpod::{log::*, NanoRange};
use netpod::{timeunits::SEC, FilePos, Nanos};
use std::collections::VecDeque;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs::File;

use super::backreadbuf::BackReadBuf;

pub trait HeaderVersion: Send + Sync + fmt::Debug {
    fn version(&self) -> u8;
    fn read_offset(&self, buf: &[u8], pos: usize) -> u64;
    fn offset_size(&self) -> usize;
    fn duplicate(&self) -> Box<dyn HeaderVersion>;
}

#[derive(Debug)]
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

    fn duplicate(&self) -> Box<dyn HeaderVersion> {
        Box::new(Self)
    }
}

#[derive(Debug)]
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

    fn duplicate(&self) -> Box<dyn HeaderVersion> {
        Box::new(Self)
    }
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

impl NamedHashChannelEntry {
    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }
}

#[derive(Debug)]
pub struct IndexFileBasics {
    path: PathBuf,
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
    hver: Box<dyn HeaderVersion>,
}

impl IndexFileBasics {
    pub async fn from_file(path: impl Into<PathBuf>, file: &mut File, stats: &StatsChannel) -> Result<Self, Error> {
        let path = path.into();
        read_file_basics(path, file, stats).await
    }

    pub fn hver(&self) -> &Box<dyn HeaderVersion> {
        &self.hver
    }

    pub async fn all_channel_entries(
        &mut self,
        file: &mut File,
        stats: &StatsChannel,
    ) -> Result<Vec<NamedHashChannelEntry>, Error> {
        let mut entries = vec![];
        let mut rb = RingBuf::new(file, 0, stats.clone()).await?;
        for epos in &self.name_hash_entries {
            if epos.named_hash_channel_entry_pos != 0 {
                let mut pos = epos.named_hash_channel_entry_pos;
                while pos != 0 {
                    rb.seek(pos).await?;
                    let min0 = 4 + 2 * self.hver.offset_size();
                    rb.fill_min(min0).await?;
                    let buf = rb.data();
                    let entry = parse_name_hash_channel_entry(buf, self.hver.as_ref())?;
                    pos = entry.next;
                    entries.push(entry);
                }
            }
        }
        Ok(entries)
    }

    pub async fn rtree_for_channel(&self, channel_name: &str, stats: &StatsChannel) -> Result<Option<Rtree>, Error> {
        // TODO in the common case, the caller has already a opened file and could reuse that here.
        let mut index_file = open_read(self.path.clone(), stats).await?;
        let chn_hash = name_hash(channel_name, self.name_hash_anchor_len as u32);
        let epos = &self.name_hash_entries[chn_hash as usize];
        let mut pos = epos.named_hash_channel_entry_pos;
        if pos == 0 {
            warn!("no hash entry for channel {}", channel_name);
        }
        let mut entries = vec![];
        let mut rb = RingBuf::new(&mut index_file, pos, stats.clone()).await?;
        while pos != 0 {
            rb.seek(pos).await?;
            let min0 = 4 + 2 * self.hver.offset_size();
            rb.fill_min(min0).await?;
            let buf = rb.data();
            let e = parse_name_hash_channel_entry(buf, self.hver.as_ref())?;
            let next = e.next;
            entries.push(e);
            pos = next;
        }
        drop(rb);
        for e in &entries {
            if e.channel_name == channel_name {
                let hver = self.hver.duplicate();
                let pos = RtreePos(e.id_rtree_pos);
                // TODO Rtree could reuse the File here:
                let tree = Rtree::new(self.path.clone(), index_file, pos, hver, stats).await?;
                return Ok(Some(tree));
            }
        }
        Ok(None)
    }
}

pub async fn read_file_basics(path: PathBuf, file: &mut File, stats: &StatsChannel) -> Result<IndexFileBasics, Error> {
    let mut rb = RingBuf::new(file, 0, stats.clone()).await?;
    rb.fill_min(4).await?;
    let buf = rb.data();
    let version = String::from_utf8(buf[3..4].to_vec())?.parse()?;
    let min0;
    if version == 3 {
        min0 = 88;
    } else if version == 2 {
        min0 = 48;
    } else {
        panic!();
    }
    rb.fill_min(min0).await?;
    let buf = rb.data();
    let mut ret = if version == 3 {
        IndexFileBasics {
            path,
            version,
            name_hash_anchor_beg: readu64(buf, 4),
            name_hash_anchor_len: readu32(buf, 12) as u64,
            fa_used_list_len: readu64(buf, 16),
            fa_used_list_beg: readu64(buf, 24),
            fa_used_list_end: readu64(buf, 32),
            fa_free_list_len: readu64(buf, 40),
            fa_free_list_beg: readu64(buf, 48),
            fa_free_list_end: readu64(buf, 56),
            fa_header_len: readu64(buf, 64),
            fa_header_prev: readu64(buf, 72),
            fa_header_next: readu64(buf, 80),
            name_hash_entries: vec![],
            hver: Box::new(HeaderVersion3),
        }
    } else if version == 2 {
        IndexFileBasics {
            path,
            version,
            name_hash_anchor_beg: readu32(buf, 4) as u64,
            name_hash_anchor_len: readu32(buf, 8) as u64,
            fa_used_list_len: readu32(buf, 12) as u64,
            fa_used_list_beg: readu32(buf, 16) as u64,
            fa_used_list_end: readu32(buf, 20) as u64,
            fa_free_list_len: readu32(buf, 24) as u64,
            fa_free_list_beg: readu32(buf, 28) as u64,
            fa_free_list_end: readu32(buf, 32) as u64,
            fa_header_len: readu32(buf, 36) as u64,
            fa_header_prev: readu32(buf, 40) as u64,
            fa_header_next: readu32(buf, 44) as u64,
            name_hash_entries: vec![],
            hver: Box::new(HeaderVersion2),
        }
    } else {
        return Err(Error::with_msg_no_trace(format!("unhandled version {}", version)));
    };
    rb.adv(min0);
    if ret.name_hash_anchor_len > 2000 {
        return Err(Error::with_msg_no_trace(format!(
            "name_hash_anchor_len {}",
            ret.name_hash_anchor_len
        )));
    }
    {
        let hver = &ret.hver;
        for _ in 0..ret.name_hash_anchor_len {
            rb.fill_min(hver.offset_size()).await?;
            let buf = rb.data();
            let pos = hver.read_offset(buf, 0);
            rb.adv(hver.offset_size());
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
    pub ts1: Nanos,
    pub ts2: Nanos,
    // TODO should probably be better name `child or offset` and be made enum.
    pub child_or_id: u64,
}

#[derive(Debug)]
pub struct RTreeNode {
    pub pos: FilePos,
    pub records: Vec<RTreeNodeRecord>,
    pub is_leaf: bool,
    pub rtree_m: usize,
}

#[derive(Debug)]
pub struct RTreeNodeAtRecord {
    pub node: RTreeNode,
    pub rix: usize,
}

impl RTreeNodeAtRecord {
    pub fn rec(&self) -> &RTreeNodeRecord {
        &self.node.records[self.rix]
    }
}

// TODO refactor as struct, rtree_m is a property of the tree.
pub async fn read_rtree_node(
    file: &mut File,
    pos: FilePos,
    rtree_m: usize,
    stats: &StatsChannel,
) -> Result<RTreeNode, Error> {
    const OFF1: usize = 9;
    const RLEN: usize = 24;
    const NANO_MAX: u32 = 999999999;
    // TODO should not be used.
    let mut rb = RingBuf::new(file, pos.pos, stats.clone()).await?;
    // TODO must know how much data I need at least...
    rb.fill_min(OFF1 + rtree_m * RLEN).await?;
    if false {
        let s = format_hex_block(rb.data(), 128);
        info!("RTREE NODE:\n{}", s);
    }
    // TODO remove, this was using a hard offset size.
    if rb.len() < 1 + 8 {
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
            if child_or_id != 0 && ts2 != 0 {
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

#[derive(Clone, Debug, PartialEq)]
pub struct NodePos(u64);

#[derive(Clone, Debug, PartialEq)]
pub struct DatarefPos(pub u64);

#[derive(Clone, Debug, PartialEq)]
pub struct DataheaderPos(pub u64);

#[derive(Clone, Debug)]
pub struct RtreePos(u64);

#[derive(Clone, Debug)]
pub enum RecordTarget {
    Child(NodePos),
    Dataref(DatarefPos),
}

#[derive(Clone, Debug)]
pub struct RtreeRecord {
    pub beg: Nanos,
    pub end: Nanos,
    pub target: RecordTarget,
}

pub struct RtreeNode {
    pub pos: NodePos,
    pub parent: Option<NodePos>,
    pub records: Vec<RtreeRecord>,
    pub is_leaf: bool,
    pub rtree_m: usize,
}

impl fmt::Debug for RtreeNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let n = self.records.len();
        let recs = if n == 0 {
            vec![]
        } else if n == 1 {
            vec![&self.records[0]]
        } else {
            vec![&self.records[0], &self.records[n - 1]]
        };
        f.debug_struct("RtreeNode")
            .field("pos", &self.pos)
            .field("parent", &self.parent)
            .field("rec_count", &n)
            .field("recs", &recs)
            .field("is_leaf", &self.is_leaf)
            .field("rtree_m", &self.rtree_m)
            .finish()
    }
}

#[derive(Debug)]
pub struct RtreeNodeAtRecord {
    pub node: RtreeNode,
    pub rix: usize,
}

impl RtreeNodeAtRecord {
    pub fn rec(&self) -> Option<&RtreeRecord> {
        if self.rix < self.node.records.len() {
            Some(&self.node.records[self.rix])
        } else {
            None
        }
    }

    pub fn advance(&mut self) -> Result<bool, Error> {
        self.rix += 1;
        if self.rix < self.node.records.len() {
            //trace!("Rec-Adv Ok  {}", self.rix);
            Ok(true)
        } else {
            //trace!("Rec-Adv End {}", self.rix);
            Ok(false)
        }
    }
}

#[derive(Debug)]
pub struct Rtree {
    path: PathBuf,
    rb: RingBuf<File>,
    m: usize,
    root: NodePos,
    hver: Box<dyn HeaderVersion>,
}

impl Rtree {
    pub async fn new(
        path: impl AsRef<Path>,
        file: File,
        pos: RtreePos,
        hver: Box<dyn HeaderVersion>,
        stats: &StatsChannel,
    ) -> Result<Self, Error> {
        let mut file = file;
        let (m, root) = Self::read_entry(&mut file, pos.clone(), hver.as_ref(), stats).await?;
        let ret = Self {
            path: path.as_ref().into(),
            rb: RingBuf::new(file, pos.0, stats.clone()).await?,
            m,
            root,
            hver,
        };
        Ok(ret)
    }

    async fn read_entry(
        file: &mut File,
        pos: RtreePos,
        hver: &dyn HeaderVersion,
        stats: &StatsChannel,
    ) -> Result<(usize, NodePos), Error> {
        let mut rb = RingBuf::new(file, pos.0, stats.clone()).await?;
        // TODO should be able to indicate how much I need at most before I know that I will e.g. seek or abort.
        let min0 = hver.offset_size() + 4;
        rb.fill_min(min0).await?;
        if rb.len() < min0 {
            return Err(Error::with_msg_no_trace("could not read enough"));
        }
        let buf = rb.data();
        let mut p = 0;
        let node_offset = hver.read_offset(buf, p);
        p += hver.offset_size();
        let m = readu32(buf, p) as usize;
        p += 4;
        let _ = p;
        let root = NodePos(node_offset);
        let ret = (m, root);
        Ok(ret)
    }

    pub async fn read_node_at(&mut self, pos: NodePos, _stats: &StatsChannel) -> Result<RtreeNode, Error> {
        let rb = &mut self.rb;
        rb.seek(pos.0).await?;
        let off1 = 1 + self.hver.offset_size();
        let rlen = 4 * 4 + self.hver.offset_size();
        let min0 = off1 + self.m * rlen;
        rb.fill_min(min0).await?;
        if false {
            let s = format_hex_block(rb.data(), min0);
            trace!("RTREE NODE:\n{}", s);
        }
        let buf = rb.data();
        let mut p = 0;
        let is_leaf = buf[p] != 0;
        p += 1;
        let parent = self.hver.read_offset(buf, p);
        p += self.hver.offset_size();
        let _ = p;
        let parent = if parent == 0 { None } else { Some(NodePos(parent)) };
        if false {
            trace!("is_leaf: {}  parent: {:?}", is_leaf, parent);
        }
        let hver = self.hver.duplicate();
        let recs = (0..self.m)
            .into_iter()
            .filter_map(move |i| {
                const NANO_MAX: u32 = 999999999;
                let off2 = off1 + i * rlen;
                let ts1a = readu32(buf, off2 + 0);
                let ts1b = readu32(buf, off2 + 4);
                let ts2a = readu32(buf, off2 + 8);
                let ts2b = readu32(buf, off2 + 12);
                let ts1b = ts1b.min(NANO_MAX);
                let ts2b = ts2b.min(NANO_MAX);
                let ts1 = ts1a as u64 * SEC + ts1b as u64 + EPICS_EPOCH_OFFSET;
                let ts2 = ts2a as u64 * SEC + ts2b as u64 + EPICS_EPOCH_OFFSET;
                let target = hver.read_offset(buf, off2 + 16);
                //trace!("NODE   {} {}   {} {}   {}", ts1a, ts1b, ts2a, ts2b, child_or_id);
                if target != 0 && ts2 != 0 {
                    let target = if is_leaf {
                        RecordTarget::Dataref(DatarefPos(target))
                    } else {
                        RecordTarget::Child(NodePos(target))
                    };
                    let rec = RtreeRecord {
                        beg: Nanos { ns: ts1 },
                        end: Nanos { ns: ts2 },
                        target,
                    };
                    Some(rec)
                } else {
                    None
                }
            })
            .collect();
        let node = RtreeNode {
            pos,
            parent,
            records: recs,
            is_leaf,
            rtree_m: self.m,
        };
        Ok(node)
    }

    pub async fn iter_range(&mut self, range: NanoRange, stats: &StatsChannel) -> Result<Option<RecordIter>, Error> {
        // TODO RecordIter needs to know when to stop after range.
        let ts1 = Instant::now();
        let mut stack = VecDeque::new();
        let mut node = self.read_node_at(self.root.clone(), stats).await?;
        let mut node_reads = 1;
        'outer: loop {
            let nr = node.records.len();
            for (i, rec) in node.records.iter().enumerate() {
                if rec.beg.ns > range.beg {
                    match &rec.target {
                        RecordTarget::Child(child) => {
                            trace!("found non-leaf match at {} / {}", i, nr);
                            let child = child.clone();
                            let nr = RtreeNodeAtRecord { node, rix: i };
                            node = self.read_node_at(child, stats).await?;
                            node_reads += 1;
                            stack.push_back(nr);
                            continue 'outer;
                        }
                        RecordTarget::Dataref(_dataref) => {
                            trace!("found leaf match at {} / {}", i, nr);
                            let nr = RtreeNodeAtRecord { node, rix: i };
                            stack.push_back(nr);
                            let ret = RecordIter {
                                tree: self.reopen(stats).await?,
                                stack,
                                stats: stats.clone(),
                            };
                            let stats = TreeSearchStats::new(ts1, node_reads);
                            trace!("iter_range done  stats: {:?}", stats);
                            return Ok(Some(ret));
                        }
                    }
                }
            }
            let stats = TreeSearchStats::new(ts1, node_reads);
            trace!("iter_range done  stats: {:?}", stats);
            return Ok(None);
        }
    }

    pub async fn reopen(&self, stats: &StatsChannel) -> Result<Self, Error> {
        let file = open_read(self.path.clone(), stats).await?;
        let ret = Self {
            path: self.path.clone(),
            rb: RingBuf::new(file, 0, stats.clone()).await?,
            m: self.m,
            root: self.root.clone(),
            hver: self.hver.duplicate(),
        };
        Ok(ret)
    }
}

#[derive(Debug)]
pub struct RecordIter {
    tree: Rtree,
    stack: VecDeque<RtreeNodeAtRecord>,
    stats: StatsChannel,
}

impl RecordIter {
    pub async fn next(&mut self) -> Result<Option<RtreeRecord>, Error> {
        match self.stack.back_mut() {
            Some(nr) => {
                assert_eq!(nr.node.is_leaf, true);
                if let Some(ret) = nr.rec() {
                    let ret = ret.clone();
                    if nr.advance()? {
                        //trace!("still more records here  {} / {}", nr.rix, nr.node.records.len());
                    } else {
                        loop {
                            if self.stack.pop_back().is_none() {
                                panic!();
                            }
                            if let Some(n2) = self.stack.back_mut() {
                                if n2.advance()? {
                                    //trace!("advanced some parent  {} / {}", n2.rix, n2.node.records.len());
                                    break;
                                }
                            } else {
                                // The stack is empty. We can not advance, but want to return our current `ret`
                                break;
                            }
                        }
                        loop {
                            if let Some(n2) = self.stack.back() {
                                if let Some(r2) = n2.rec() {
                                    match &r2.target {
                                        RecordTarget::Child(child) => {
                                            trace!("Read next lower {:?}", child);
                                            let n3 = self.tree.read_node_at(child.clone(), &self.stats).await?;
                                            trace!("n3: {:?}", n3);
                                            let nr3 = RtreeNodeAtRecord { node: n3, rix: 0 };
                                            self.stack.push_back(nr3);
                                        }
                                        RecordTarget::Dataref(_) => {
                                            trace!("loop B is-leaf");
                                            // done, we've positioned the next result.
                                            break;
                                        }
                                    }
                                } else {
                                    // Should not get here, the above loop should not break in this case.
                                    panic!();
                                }
                            } else {
                                // Happens when we exhaust the iterator.
                                break;
                            }
                        }
                    }
                    Ok(Some(ret))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }
}

pub async fn read_rtree_entrypoint(
    file: &mut File,
    pos: u64,
    _basics: &IndexFileBasics,
    stats: &StatsChannel,
) -> Result<RTreeNode, Error> {
    let mut rb = RingBuf::new(file, pos, stats.clone()).await?;
    // TODO remove, this is anyway still using a hardcoded offset size.
    rb.fill_min(8 + 4).await?;
    if rb.len() < 8 + 4 {
        return Err(Error::with_msg_no_trace("could not read enough"));
    }
    let b = rb.data();
    let node_offset = readu64(b, 0);
    let rtree_m = readu32(b, 8);
    //info!("node_offset: {}  rtree_m: {}", node_offset, rtree_m);
    let pos = FilePos { pos: node_offset };
    let mut file = rb.into_file();
    let node = read_rtree_node(&mut file, pos, rtree_m as usize, stats).await?;
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
    stats: &StatsChannel,
) -> Result<(Option<RTreeNodeAtRecord>, TreeSearchStats), Error> {
    let ts1 = Instant::now();
    let mut node = read_rtree_node(file, start_node_pos, rtree_m, stats).await?;
    let mut node_reads = 1;
    'outer: loop {
        let nr = node.records.len();
        for (i, rec) in node.records.iter().enumerate() {
            if rec.ts2.ns > beg.ns {
                if node.is_leaf {
                    trace!("found leaf match at {} / {}", i, nr);
                    let ret = RTreeNodeAtRecord { node, rix: i };
                    let stats = TreeSearchStats::new(ts1, node_reads);
                    return Ok((Some(ret), stats));
                } else {
                    trace!("found non-leaf match at {} / {}", i, nr);
                    let pos = FilePos { pos: rec.child_or_id };
                    node = read_rtree_node(file, pos, rtree_m, stats).await?;
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

pub async fn search_record_expand_try(
    file: &mut File,
    rtree_m: usize,
    start_node_pos: FilePos,
    beg: Nanos,
    stats: &StatsChannel,
) -> Result<(Option<RTreeNodeAtRecord>, TreeSearchStats), Error> {
    let ts1 = Instant::now();
    let mut node = read_rtree_node(file, start_node_pos, rtree_m, stats).await?;
    let mut node_reads = 1;
    'outer: loop {
        let nr = node.records.len();
        for (i, rec) in node.records.iter().enumerate().rev() {
            if rec.ts1.ns <= beg.ns {
                if node.is_leaf {
                    trace!("found leaf match at {} / {}", i, nr);
                    let ret = RTreeNodeAtRecord { node, rix: i };
                    let stats = TreeSearchStats::new(ts1, node_reads);
                    return Ok((Some(ret), stats));
                } else {
                    // TODO
                    // We rely on channel archiver engine that there is at least one event
                    // in the referenced range. It should according to docs, but who knows.
                    trace!("found non-leaf match at {} / {}", i, nr);
                    let pos = FilePos { pos: rec.child_or_id };
                    node = read_rtree_node(file, pos, rtree_m, stats).await?;
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

pub async fn search_record_expand(
    file: &mut File,
    rtree_m: usize,
    start_node_pos: FilePos,
    beg: Nanos,
    stats: &StatsChannel,
) -> Result<(Option<RTreeNodeAtRecord>, TreeSearchStats), Error> {
    let ts1 = Instant::now();
    let res = search_record_expand_try(file, rtree_m, start_node_pos, beg, stats).await?;
    match res {
        (Some(res), stats) => {
            let ts2 = Instant::now();
            info!("search_record_expand  took {:?}", ts2.duration_since(ts1));
            Ok((Some(res), stats))
        }
        _ => {
            let res = search_record(file, rtree_m, start_node_pos, beg, stats).await;
            let ts2 = Instant::now();
            info!("search_record_expand  took {:?}", ts2.duration_since(ts1));
            res
        }
    }
}

#[derive(Debug)]
pub struct ChannelInfoBasics {
    pub channel_name: String,
    pub rtree_m: usize,
    pub rtree_start_pos: FilePos,
    pub basics: IndexFileBasics,
}

const HVER2: HeaderVersion2 = HeaderVersion2;
const HVER3: HeaderVersion3 = HeaderVersion3;

impl ChannelInfoBasics {
    pub fn hver(&self) -> &dyn HeaderVersion {
        if self.basics.version == 2 {
            &HVER2
        } else if self.basics.version == 3 {
            &HVER3
        } else {
            panic!()
        }
    }
}

// TODO retire this function.
pub async fn read_channel(
    path: impl Into<PathBuf>,
    index_file: File,
    channel_name: &str,
    stats: &StatsChannel,
) -> Result<Option<ChannelInfoBasics>, Error> {
    let path = path.into();
    let mut index_file = index_file;
    let basics = read_file_basics(path.clone(), &mut index_file, stats).await?;
    let chn_hash = name_hash(channel_name, basics.name_hash_anchor_len as u32);
    let epos = &basics.name_hash_entries[chn_hash as usize];
    let mut entries = vec![];
    let mut pos = epos.named_hash_channel_entry_pos;
    let mut rb = RingBuf::new(index_file, pos, stats.clone()).await?;
    loop {
        rb.seek(pos).await?;
        let fill_min = if basics.hver.offset_size() == 8 { 20 } else { 12 };
        rb.fill_min(fill_min).await?;
        if rb.len() < fill_min {
            warn!("not enough data to continue reading channel list from name hash list");
            break;
        }
        let buf = rb.data();
        let e = parse_name_hash_channel_entry(buf, basics.hver.as_ref())?;
        let next = e.next;
        entries.push(e);
        if next == 0 {
            break;
        } else {
            pos = next;
        }
    }
    let mut file2 = open_read(path, stats).await?;
    for e in &entries {
        if e.channel_name == channel_name {
            let ep = read_rtree_entrypoint(&mut file2, e.id_rtree_pos, &basics, stats).await?;
            let ret = ChannelInfoBasics {
                channel_name: channel_name.into(),
                rtree_m: ep.rtree_m,
                rtree_start_pos: ep.pos,
                basics,
            };
            return Ok(Some(ret));
        }
    }
    Ok(None)
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

#[derive(Debug)]
pub struct Dataref {
    pub next: DatarefPos,
    pub data_header_pos: DataheaderPos,
    pub fname: String,
}

impl Dataref {
    pub fn file_name(&self) -> &str {
        &self.fname
    }

    pub fn data_header_pos(&self) -> DataheaderPos {
        self.data_header_pos.clone()
    }

    pub fn next(&self) -> DatarefPos {
        self.next.clone()
    }
}

pub async fn read_datablockref(
    file: &mut File,
    pos: FilePos,
    hver: &dyn HeaderVersion,
    stats: &StatsChannel,
) -> Result<Dataref, Error> {
    let mut rb = RingBuf::new(file, pos.pos, stats.clone()).await?;
    let min0 = hver.offset_size() * 2 + 2;
    rb.fill_min(min0).await?;
    let buf = rb.data();
    let mut p = 0;
    let next = hver.read_offset(buf, p);
    p += hver.offset_size();
    let data = hver.read_offset(buf, p);
    p += hver.offset_size();
    let len = readu16(buf, p) as usize;
    p += 2;
    let _ = p;
    rb.fill_min(min0 + len).await?;
    let buf = rb.data();
    let fname = String::from_utf8(buf[min0..min0 + len].to_vec())?;
    let next = DatarefPos(next);
    let data_header_pos = DataheaderPos(data);
    let ret = Dataref {
        next,
        data_header_pos,
        fname,
    };
    Ok(ret)
}

pub async fn read_datablockref2(
    rb: &mut BackReadBuf<File>,
    pos: DatarefPos,
    hver: &dyn HeaderVersion,
) -> Result<Dataref, Error> {
    rb.seek(pos.0).await?;
    let min0 = hver.offset_size() * 2 + 2;
    rb.fill_min(min0).await?;
    let buf = rb.data();
    let mut p = 0;
    let next = hver.read_offset(buf, p);
    p += hver.offset_size();
    let data = hver.read_offset(buf, p);
    p += hver.offset_size();
    let len = readu16(buf, p) as usize;
    p += 2;
    let _ = p;
    rb.fill_min(min0 + len).await?;
    let buf = rb.data();
    let fname = String::from_utf8(buf[min0..min0 + len].to_vec())?;
    let next = DatarefPos(next);
    let data_header_pos = DataheaderPos(data);
    let ret = Dataref {
        next,
        data_header_pos,
        fname,
    };
    Ok(ret)
}

async fn channel_list_from_index_name_hash_list(
    file: &mut File,
    pos: FilePos,
    hver: &dyn HeaderVersion,
    stats: &StatsChannel,
) -> Result<Vec<NamedHashChannelEntry>, Error> {
    let mut pos = pos;
    let mut ret = vec![];
    let mut rb = RingBuf::new(file, pos.pos, stats.clone()).await?;
    loop {
        rb.seek(pos.pos).await?;
        let fill_min = if hver.offset_size() == 8 { 20 } else { 12 };
        rb.fill_min(fill_min).await?;
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

// TODO retire this function
pub async fn channel_list(index_path: PathBuf, stats: &StatsChannel) -> Result<Vec<String>, Error> {
    let mut ret = vec![];
    let mut file = open_read(index_path.clone(), stats).await?;
    let basics = read_file_basics(index_path.clone(), &mut file, stats).await?;
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
            let list = channel_list_from_index_name_hash_list(&mut file, pos, hver, stats).await?;
            for e in list {
                ret.push(e.channel_name);
            }
        }
    }
    Ok(ret)
}

#[cfg(test)]
mod test {
    use crate::archeng::indextree::{
        read_channel, read_datablockref, read_file_basics, search_record, IndexFileBasics,
    };
    use crate::archeng::EPICS_EPOCH_OFFSET;
    use commonio::{open_read, StatsChannel};
    use err::Error;
    #[allow(unused)]
    use netpod::log::*;
    #[allow(unused)]
    use netpod::timeunits::*;
    use netpod::{FilePos, NanoRange, Nanos};
    use std::path::PathBuf;

    /*
    Root node: most left record ts1 965081099942616289, most right record ts2 1002441959876114632
    */
    const CHN_0_MASTER_INDEX: &str = "/data/daqbuffer-testdata/sls/gfa03/bl_arch/archive_X05DA_SH/index";

    #[test]
    fn read_file_basic_info() -> Result<(), Error> {
        let fut = async {
            let stats = &StatsChannel::dummy();
            let mut file = open_read(CHN_0_MASTER_INDEX.into(), stats).await?;
            let res = read_file_basics(CHN_0_MASTER_INDEX.into(), &mut file, stats).await?;
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
    fn rtree_init() -> Result<(), Error> {
        let fut = async {
            let stats = &StatsChannel::dummy();
            let channel_name = "X05DA-FE-WI1:TC1";
            let mut file = open_read(CHN_0_MASTER_INDEX.into(), stats).await?;
            let basics = IndexFileBasics::from_file(CHN_0_MASTER_INDEX, &mut file, stats).await?;
            let tree = basics.rtree_for_channel(channel_name, stats).await?;
            let tree = tree.ok_or_else(|| Error::with_msg("no tree found for channel"))?;
            assert_eq!(tree.m, 50);
            assert_eq!(tree.root.0, 329750);
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }

    #[test]
    fn rtree_count_all_records() -> Result<(), Error> {
        let fut = async {
            let stats = &StatsChannel::dummy();
            let channel_name = "X05DA-FE-WI1:TC1";
            let range = NanoRange { beg: 0, end: u64::MAX };
            let mut file = open_read(CHN_0_MASTER_INDEX.into(), stats).await?;
            let basics = IndexFileBasics::from_file(CHN_0_MASTER_INDEX, &mut file, stats).await?;
            let mut tree = basics
                .rtree_for_channel(channel_name, stats)
                .await?
                .ok_or_else(|| Error::with_msg("no tree found for channel"))?;
            let mut iter = tree
                .iter_range(range, stats)
                .await?
                .ok_or_else(|| Error::with_msg("could not position iterator"))?;
            let mut i1 = 0;
            let mut ts_max = Nanos::from_ns(0);
            while let Some(rec) = iter.next().await? {
                if rec.beg <= ts_max {
                    return Err(Error::with_msg_no_trace("BAD ORDER"));
                }
                ts_max = rec.beg;
                i1 += 1;
                if i1 > 200000 {
                    break;
                }
            }
            assert_eq!(i1, 177);
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }

    #[test]
    fn rtree_search() -> Result<(), Error> {
        let fut = async {
            let stats = &StatsChannel::dummy();
            let channel_name = "X05DA-FE-WI1:TC1";
            let range = NanoRange {
                beg: 1601503499684884156,
                end: 1601569919634086480,
            };
            let mut file = open_read(CHN_0_MASTER_INDEX.into(), stats).await?;
            let basics = IndexFileBasics::from_file(CHN_0_MASTER_INDEX, &mut file, stats).await?;
            let mut tree = basics
                .rtree_for_channel(channel_name, stats)
                .await?
                .ok_or_else(|| Error::with_msg("no tree found for channel"))?;
            let mut iter = tree
                .iter_range(range, stats)
                .await?
                .ok_or_else(|| Error::with_msg("could not position iterator"))?;
            let mut i1 = 0;
            let mut ts_max = Nanos::from_ns(0);
            while let Some(rec) = iter.next().await? {
                // TODO assert
                debug!("GOT RECORD: {:?}  {:?}", rec.beg, rec.target);
                if rec.beg <= ts_max {
                    return Err(Error::with_msg_no_trace("BAD ORDER"));
                }
                ts_max = rec.beg;
                i1 += 1;
                if i1 > 20000000 {
                    break;
                }
            }
            /*
            assert_eq!(res.node.is_leaf, true);
            assert_eq!(res.node.pos.pos, 8216);
            assert_eq!(res.rix, 17);
            let rec = &res.node.records[res.rix];
            assert_eq!(rec.ts1.ns, 970351499684884156 + EPICS_EPOCH_OFFSET);
            assert_eq!(rec.ts2.ns, 970417919634086480 + EPICS_EPOCH_OFFSET);
            assert_eq!(rec.child_or_id, 185074);
            let pos = FilePos { pos: rec.child_or_id };
            let datablock = read_datablockref(&mut index_file, pos, cib.hver(), stats).await?;
            assert_eq!(datablock.data_header_pos().0, 52787);
            assert_eq!(datablock.file_name(), "20201001/20201001");
            */
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }

    #[test]
    fn read_for_channel() -> Result<(), Error> {
        let fut = async {
            let stats = &StatsChannel::dummy();
            let index_file = open_read(CHN_0_MASTER_INDEX.into(), stats).await?;
            let channel_name = "X05DA-FE-WI1:TC1";
            let res = read_channel(CHN_0_MASTER_INDEX, index_file, channel_name, stats).await?;
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
            let stats = &StatsChannel::dummy();
            let index_path: PathBuf = CHN_0_MASTER_INDEX.into();
            let index_file = open_read(index_path.clone(), stats).await?;
            let mut file2 = open_read(index_path.clone(), stats).await?;
            let channel_name = "X05DA-FE-WI1:TC1";
            const T0: u64 = 970351442331056677 + 1 + EPICS_EPOCH_OFFSET;
            let beg = Nanos { ns: T0 };
            let res = read_channel(CHN_0_MASTER_INDEX, index_file, channel_name, stats).await?;
            let cib = res.unwrap();
            let (res, _stats) = search_record(&mut file2, cib.rtree_m, cib.rtree_start_pos, beg, stats).await?;
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
            let datablock = read_datablockref(&mut file2, pos, cib.hver(), stats).await?;
            assert_eq!(datablock.data_header_pos().0, 52787);
            assert_eq!(datablock.file_name(), "20201001/20201001");
            // The actual datafile for that time was not retained any longer.
            // But the index still points to that.
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }

    // Note: this tests only the index tree, but it does not look for any actual event in some file.
    #[test]
    fn search_record_at_beg() -> Result<(), Error> {
        let fut = async {
            let stats = &StatsChannel::dummy();
            let index_file = open_read(CHN_0_MASTER_INDEX.into(), stats).await?;
            let mut file2 = open_read(CHN_0_MASTER_INDEX.into(), stats).await?;
            let channel_name = "X05DA-FE-WI1:TC1";
            const T0: u64 = 965081099942616289 + EPICS_EPOCH_OFFSET;
            let beg = Nanos { ns: T0 };
            let res = read_channel(CHN_0_MASTER_INDEX, index_file, channel_name, stats).await?;
            let cib = res.unwrap();
            let (res, _stats) = search_record(&mut file2, cib.rtree_m, cib.rtree_start_pos, beg, stats).await?;
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
            let stats = &StatsChannel::dummy();
            let index_file = open_read(CHN_0_MASTER_INDEX.into(), stats).await?;
            let mut file2 = open_read(CHN_0_MASTER_INDEX.into(), stats).await?;
            let channel_name = "X05DA-FE-WI1:TC1";
            const T0: u64 = 1002441959876114632 - 1 + EPICS_EPOCH_OFFSET;
            let beg = Nanos { ns: T0 };
            let res = read_channel(CHN_0_MASTER_INDEX, index_file, channel_name, stats).await?;
            let cib = res.unwrap();
            let (res, _stats) = search_record(&mut file2, cib.rtree_m, cib.rtree_start_pos, beg, stats).await?;
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
            let stats = &StatsChannel::dummy();
            let index_file = open_read(CHN_0_MASTER_INDEX.into(), stats).await?;
            let mut file2 = open_read(CHN_0_MASTER_INDEX.into(), stats).await?;
            let channel_name = "X05DA-FE-WI1:TC1";
            const T0: u64 = 1002441959876114632 - 0 + EPICS_EPOCH_OFFSET;
            let beg = Nanos { ns: T0 };
            let res = read_channel(CHN_0_MASTER_INDEX, index_file, channel_name, stats).await?;
            let cib = res.unwrap();
            let (res, _stats) = search_record(&mut file2, cib.rtree_m, cib.rtree_start_pos, beg, stats).await?;
            assert_eq!(res.is_none(), true);
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }
}
