use super::indextree::DataheaderPos;
use super::ringbuf::RingBuf;
use super::{open_read, StatsChannel};
use crate::archeng::blockrefstream::BlockrefItem;
use crate::archeng::datablock::{read_data2, read_datafile_header2};
use crate::eventsitem::EventsItem;
use err::Error;
use futures_core::{Future, Stream};
use futures_util::stream::FuturesOrdered;
use futures_util::StreamExt;
use items::{WithLen, WithTimestamps};
use netpod::{log::*, NanoRange};
use serde::Serialize;
use serde_json::Value as JsVal;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::fs::File;

#[derive(Debug, Serialize)]
pub struct StatsAcc {
    items: u64,
    events: u64,
    bytes: u64,
    #[serde(skip)]
    beg: Instant,
}

impl StatsAcc {
    pub fn new() -> Self {
        Self {
            items: 0,
            events: 0,
            bytes: 0,
            beg: Instant::now(),
        }
    }

    fn add(&mut self, events: u64, bytes: u64) {
        self.items += 1;
        self.events += events;
        self.bytes += bytes;
    }

    fn older(&self, dur: Duration) -> bool {
        Instant::now().duration_since(self.beg) >= dur
    }
}

struct Reader {
    fname: String,
    rb: RingBuf<File>,
}

impl Reader {}

struct FutAItem {
    fname: String,
    path: PathBuf,
    dfnotfound: bool,
    reader: Option<Reader>,
    bytes_read: u64,
    events_read: u64,
    events: Option<EventsItem>,
}

pub struct FutA {
    fname: String,
    pos: DataheaderPos,
    reader: Option<Reader>,
}

impl Future for FutA {
    type Output = Result<JsVal, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        err::todoval()
    }
}

pub enum BlockItem {}

pub struct BlockStream<S> {
    inp: S,
    inp_done: bool,
    range: NanoRange,
    dfnotfound: BTreeMap<PathBuf, bool>,
    block_reads: FuturesOrdered<Pin<Box<dyn Future<Output = Result<FutAItem, Error>> + Send>>>,
    max_reads: usize,
    readers: VecDeque<Reader>,
    last_dfname: String,
    last_dfhpos: DataheaderPos,
    ts_max: u64,
    done: bool,
    complete: bool,
    acc: StatsAcc,
    good_reader: u64,
    discard_reader: u64,
    not_found_hit: u64,
    same_block: u64,
}

impl<S> BlockStream<S> {
    pub fn new(inp: S, range: NanoRange, max_reads: usize) -> Self
    where
        S: Stream<Item = Result<BlockrefItem, Error>> + Unpin,
    {
        Self {
            inp,
            inp_done: false,
            range,
            dfnotfound: BTreeMap::new(),
            block_reads: FuturesOrdered::new(),
            max_reads,
            readers: VecDeque::new(),
            last_dfname: String::new(),
            last_dfhpos: DataheaderPos(u64::MAX),
            ts_max: 0,
            done: false,
            complete: false,
            acc: StatsAcc::new(),
            good_reader: 0,
            discard_reader: 0,
            not_found_hit: 0,
            same_block: 0,
        }
    }
}

enum Int<T> {
    NoWork,
    Pending,
    Empty,
    Item(T),
    Done,
}

impl<S> Stream for BlockStream<S>
where
    S: Stream<Item = Result<BlockrefItem, Error>> + Unpin,
{
    type Item = Result<JsVal, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll_next on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else {
                let item1 = if self.inp_done {
                    Int::Done
                } else if self.block_reads.len() >= self.max_reads {
                    Int::NoWork
                } else {
                    match self.inp.poll_next_unpin(cx) {
                        Ready(item) => match item {
                            Some(item) => match item {
                                Ok(item) => match item {
                                    BlockrefItem::Blockref(bref, _jsval) => {
                                        if let Some(_) = self.dfnotfound.get(&bref.dpath) {
                                            self.not_found_hit += 1;
                                        } else {
                                            if bref.dref.file_name() == self.last_dfname
                                                && bref.dref.data_header_pos() == self.last_dfhpos
                                            {
                                                self.same_block += 1;
                                            } else {
                                                let reader = if let Some(reader) = self.readers.pop_front() {
                                                    if reader.fname == bref.dref.file_name() {
                                                        self.good_reader += 1;
                                                        Some(reader)
                                                    } else {
                                                        self.discard_reader += 1;
                                                        None
                                                    }
                                                } else {
                                                    None
                                                };
                                                let fname = bref.dref.file_name().to_string();
                                                let dpath = bref.dpath;
                                                let pos = bref.dref.data_header_pos();
                                                let fut = {
                                                    let fname = fname.clone();
                                                    let pos = pos.clone();
                                                    let range = self.range.clone();
                                                    async move {
                                                        let reader = if let Some(reader) = reader {
                                                            Some(reader)
                                                        } else {
                                                            let stats = StatsChannel::dummy();
                                                            info!("open new reader file {:?}", dpath);
                                                            match open_read(dpath.clone(), &stats).await {
                                                                Ok(file) => {
                                                                    //
                                                                    let reader = Reader {
                                                                        fname: fname.clone(),
                                                                        rb: RingBuf::new(file, pos.0, stats).await?,
                                                                    };
                                                                    Some(reader)
                                                                }
                                                                Err(_) => None,
                                                            }
                                                        };
                                                        if let Some(mut reader) = reader {
                                                            let rp1 = reader.rb.bytes_read();
                                                            let dfheader =
                                                                read_datafile_header2(&mut reader.rb, pos).await?;
                                                            let data =
                                                                read_data2(&mut reader.rb, &dfheader, range, false)
                                                                    .await?;
                                                            let rp2 = reader.rb.bytes_read();
                                                            let bytes_read = rp2 - rp1;
                                                            let ret = FutAItem {
                                                                fname,
                                                                path: dpath,
                                                                dfnotfound: false,
                                                                reader: Some(reader),
                                                                bytes_read,
                                                                events_read: data.len() as u64,
                                                                events: Some(data),
                                                            };
                                                            Ok(ret)
                                                        } else {
                                                            let ret = FutAItem {
                                                                fname,
                                                                path: dpath,
                                                                dfnotfound: true,
                                                                reader: None,
                                                                bytes_read: 0,
                                                                events_read: 0,
                                                                events: None,
                                                            };
                                                            Ok(ret)
                                                        }
                                                    }
                                                };
                                                self.block_reads.push(Box::pin(fut));
                                                self.last_dfname = fname;
                                                self.last_dfhpos = pos;
                                            };
                                        }
                                        Int::Empty
                                    }
                                    BlockrefItem::JsVal(_jsval) => Int::Empty,
                                },
                                Err(e) => {
                                    self.done = true;
                                    Int::Item(Err(e))
                                }
                            },
                            None => {
                                self.inp_done = true;
                                Int::Done
                            }
                        },
                        Pending => Int::Pending,
                    }
                };
                let item2 = if let Int::Item(_) = item1 {
                    Int::NoWork
                } else {
                    if self.block_reads.len() == 0 {
                        Int::NoWork
                    } else {
                        match self.block_reads.poll_next_unpin(cx) {
                            Ready(Some(Ok(item))) => {
                                //
                                if item.dfnotfound {
                                    self.dfnotfound.insert(item.path, true);
                                }
                                if let Some(reader) = item.reader {
                                    self.readers.push_back(reader);
                                }
                                if let Some(ev) = &item.events {
                                    for i in 0..ev.len() {
                                        let ts = ev.ts(i);
                                        if ts < self.ts_max {
                                            let msg = format!("unordered event:  {}  {}", ts, self.ts_max);
                                            error!("{}", msg);
                                            self.done = true;
                                            return Ready(Some(Err(Error::with_msg_no_trace(msg))));
                                        }
                                    }
                                }
                                self.acc.add(item.events_read, item.bytes_read);
                                if false {
                                    let item = JsVal::String(format!(
                                        "bytes read {}  {}  events {}",
                                        item.bytes_read,
                                        item.events.is_some(),
                                        item.events_read
                                    ));
                                }
                                if self.acc.older(Duration::from_millis(1000)) {
                                    let ret = std::mem::replace(&mut self.acc, StatsAcc::new());
                                    match serde_json::to_value((ret, self.block_reads.len(), self.readers.len())) {
                                        Ok(item) => Int::Item(Ok(item)),
                                        Err(e) => {
                                            self.done = true;
                                            return Ready(Some(Err(e.into())));
                                        }
                                    }
                                } else {
                                    //Int::Item(Ok(item))
                                    Int::Empty
                                }
                            }
                            Ready(Some(Err(e))) => {
                                self.done = true;
                                Int::Item(Err(e))
                            }
                            Ready(None) => {
                                panic!();
                            }
                            Pending => Int::Pending,
                        }
                    }
                };
                match (item1, item2) {
                    (Int::Item(_), Int::Item(_)) => panic!(),
                    (Int::NoWork, Int::NoWork) => panic!(),
                    (_, Int::Done) => panic!(),
                    (Int::Item(item), _) => Ready(Some(item)),
                    (_, Int::Item(item)) => Ready(Some(item)),
                    (Int::Pending | Int::NoWork, Int::Pending) => Pending,
                    (Int::Pending, Int::NoWork) => Pending,
                    (Int::Done, Int::Pending) => Pending,
                    (Int::Pending | Int::Done | Int::Empty | Int::NoWork, Int::Empty) => continue,
                    (Int::Empty, Int::Pending | Int::NoWork) => continue,
                    (Int::Done, Int::NoWork) => {
                        self.done = true;
                        Ready(None)
                    }
                }
            };
        }
    }
}

impl<S> fmt::Debug for BlockStream<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BlockStream")
            .field("inp_done", &self.inp_done)
            .field("range", &self.range)
            .field("max_reads", &self.max_reads)
            .field("ts_max", &self.ts_max)
            .field("done", &self.done)
            .field("complete", &self.complete)
            .field("acc", &self.acc)
            .field("good_reader", &self.good_reader)
            .field("discard_reader", &self.discard_reader)
            .field("not_found_hit", &self.not_found_hit)
            .field("same_block", &self.same_block)
            .finish()
    }
}

impl<S> Drop for BlockStream<S> {
    fn drop(&mut self) {
        info!("Drop {:?}", self);
    }
}
