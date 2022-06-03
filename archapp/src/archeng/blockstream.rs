use crate::archeng::blockrefstream::BlockrefItem;
use crate::archeng::datablock::{read_data2, read_datafile_header2};
use crate::archeng::indextree::DataheaderPos;
use commonio::ringbuf::RingBuf;
use commonio::{open_read, StatsChannel};
use err::Error;
use futures_core::{Future, Stream};
use futures_util::stream::FuturesOrdered;
use futures_util::StreamExt;
use items::eventsitem::EventsItem;
use items::{WithLen, WithTimestamps};
use netpod::{log::*, NanoRange, Nanos};
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
    dpos: DataheaderPos,
    dfnotfound: bool,
    reader: Option<Reader>,
    bytes_read: u64,
    events_read: u64,
    events: Option<EventsItem>,
}

#[allow(unused)]
pub struct FutA {
    fname: String,
    pos: DataheaderPos,
    reader: Option<Reader>,
}

impl Future for FutA {
    type Output = Result<JsVal, Error>;

    #[allow(unused)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        err::todoval()
    }
}

#[derive(Debug)]
pub enum BlockItem {
    EventsItem(EventsItem),
    JsVal(JsVal),
}

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
    data_done: bool,
    raco: bool,
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
        debug!("new BlockStream  max_reads {}  {:?}", max_reads, range);
        Self {
            inp,
            inp_done: false,
            range,
            dfnotfound: BTreeMap::new(),
            block_reads: FuturesOrdered::new(),
            max_reads: max_reads.max(1),
            readers: VecDeque::new(),
            last_dfname: String::new(),
            last_dfhpos: DataheaderPos(u64::MAX),
            ts_max: 0,
            data_done: false,
            raco: false,
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
    type Item = Result<BlockItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll_next on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if self.data_done {
                self.done = true;
                if self.raco {
                    // currently handled downstream
                    continue;
                } else {
                    continue;
                }
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
                                                            trace!("open new reader file {:?}", dpath);
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
                                                                read_datafile_header2(&mut reader.rb, pos.clone())
                                                                    .await?;
                                                            // TODO handle expand
                                                            let expand = false;
                                                            let data =
                                                                read_data2(&mut reader.rb, &dfheader, range, expand)
                                                                    .await
                                                                    .map_err(|e| {
                                                                        Error::with_msg_no_trace(format!(
                                                                            "dpath {:?}  error {}",
                                                                            dpath, e
                                                                        ))
                                                                    })?;
                                                            let rp2 = reader.rb.bytes_read();
                                                            let bytes_read = rp2 - rp1;
                                                            let ret = FutAItem {
                                                                fname,
                                                                path: dpath,
                                                                dpos: pos,
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
                                                                dpos: pos,
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
                                    BlockrefItem::JsVal(jsval) => Int::Item(Ok(BlockItem::JsVal(jsval))),
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
                                let mut item = item;
                                item.events = if let Some(ev) = item.events {
                                    if ev.len() > 0 {
                                        if ev.ts(ev.len() - 1) > self.range.end {
                                            debug!(". . . . =====  DATA DONE ----------------------");
                                            self.raco = true;
                                            self.data_done = true;
                                        }
                                    }
                                    if ev.len() == 1 {
                                        trace!("From  {}  {:?}  {}", item.fname, item.path, item.dpos.0);
                                        trace!("See 1 event {:?}", Nanos::from_ns(ev.ts(0)));
                                    } else if ev.len() > 1 {
                                        trace!("From  {}  {:?}  {}", item.fname, item.path, item.dpos.0);
                                        trace!(
                                            "See {} events  {:?} to {:?}",
                                            ev.len(),
                                            Nanos::from_ns(ev.ts(0)),
                                            Nanos::from_ns(ev.ts(ev.len() - 1))
                                        );
                                    }
                                    let mut contains_unordered = false;
                                    for i in 0..ev.len() {
                                        // TODO factor for performance.
                                        let ts = ev.ts(i);
                                        if ts < self.ts_max {
                                            contains_unordered = true;
                                            if true {
                                                let msg = format!(
                                                    "unordered event in item at {}  ts {:?}  ts_max {:?}",
                                                    i,
                                                    Nanos::from_ns(ts),
                                                    Nanos::from_ns(self.ts_max)
                                                );
                                                error!("{}", msg);
                                                self.done = true;
                                                return Ready(Some(Err(Error::with_msg_no_trace(msg))));
                                            }
                                        }
                                        self.ts_max = ts;
                                    }
                                    if contains_unordered {
                                        Some(ev)
                                    } else {
                                        Some(ev)
                                    }
                                } else {
                                    None
                                };
                                let item = item;
                                if item.dfnotfound {
                                    self.dfnotfound.insert(item.path.clone(), true);
                                }
                                if let Some(reader) = item.reader {
                                    self.readers.push_back(reader);
                                }
                                self.acc.add(item.events_read, item.bytes_read);
                                if false {
                                    let item = JsVal::String(format!(
                                        "bytes read {}  {}  events {}",
                                        item.bytes_read,
                                        item.events.is_some(),
                                        item.events_read
                                    ));
                                    let _ = item;
                                }
                                if false {
                                    // TODO emit proper variant for optional performance measurement.
                                    if self.acc.older(Duration::from_millis(1000)) {
                                        let ret = std::mem::replace(&mut self.acc, StatsAcc::new());
                                        match serde_json::to_value((ret, self.block_reads.len(), self.readers.len())) {
                                            Ok(item) => Int::Item(Ok::<_, Error>(item)),
                                            Err(e) => {
                                                self.done = true;
                                                return Ready(Some(Err(e.into())));
                                            }
                                        }
                                    } else {
                                        //Int::Item(Ok(item))
                                        Int::Empty
                                    };
                                    err::todoval()
                                } else {
                                    if let Some(events) = item.events {
                                        Int::Item(Ok(BlockItem::EventsItem(events)))
                                    } else {
                                        Int::Empty
                                    }
                                }
                            }
                            Ready(Some(Err(e))) => {
                                self.done = true;
                                error!("{}", e);
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
        trace!("Drop {:?}", self);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::archeng::blockrefstream::blockref_stream;
    use crate::archeng::indexfiles::index_file_path_list;
    use chrono::{DateTime, Utc};
    use futures_util::StreamExt;
    use items::{LogItem, RangeCompletableItem, StreamItem};
    use netpod::{timeunits::SEC, Channel, Database};
    use streams::rangefilter::RangeFilter;

    struct EventCount {
        pre: usize,
        inside: usize,
        post: usize,
        raco: usize,
        tss: Vec<u64>,
    }

    impl EventCount {
        fn new() -> Self {
            Self {
                pre: 0,
                inside: 0,
                post: 0,
                raco: 0,
                tss: vec![],
            }
        }
    }

    async fn count_events(range: NanoRange, expand: bool, collect_ts: bool) -> Result<EventCount, Error> {
        let channel = Channel {
            backend: "sls-archive".into(),
            //name: "X05DA-FE-WI1:TC1".into(),
            name: "ARIDI-PCT:CURRENT".into(),
            series: None,
        };
        let dbconf = Database {
            host: "localhost".into(),
            port: 5432,
            name: "testingdaq".into(),
            user: "testingdaq".into(),
            pass: "testingdaq".into(),
        };
        let ixpaths = index_file_path_list(channel.clone(), dbconf).await?;
        info!("got categorized ixpaths: {:?}", ixpaths);
        let ixpath = ixpaths.first().unwrap().clone();
        let refs = Box::pin(blockref_stream(channel, range.clone(), expand, ixpath));
        let blocks = BlockStream::new(refs, range.clone(), 1);
        let events = blocks.map(|item| match item {
            Ok(k) => match k {
                BlockItem::EventsItem(k) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(k))),
                BlockItem::JsVal(k) => Ok(StreamItem::Log(LogItem::quick(Level::TRACE, format!("{:?}", k)))),
            },
            Err(e) => Err(e),
        });
        let mut filtered = RangeFilter::new(events, range.clone(), expand);
        let mut ret = EventCount::new();
        while let Some(item) = filtered.next().await {
            //info!("Got block  {:?}", item);
            match item {
                Ok(item) => match item {
                    StreamItem::DataItem(item) => match item {
                        RangeCompletableItem::RangeComplete => {
                            ret.raco += 1;
                        }
                        RangeCompletableItem::Data(item) => {
                            let n = item.len();
                            for i in 0..n {
                                let ts = item.ts(i);
                                //info!("See event {}", ts);
                                if ts < range.beg {
                                    ret.pre += 1;
                                } else if ts < range.end {
                                    ret.inside += 1;
                                } else {
                                    ret.post += 1;
                                }
                                if collect_ts {
                                    ret.tss.push(ts);
                                }
                            }
                        }
                    },
                    StreamItem::Log(_) => {}
                    StreamItem::Stats(_) => {}
                },
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(ret)
    }

    fn tons(dt: &DateTime<Utc>) -> u64 {
        dt.timestamp() as u64 * SEC + dt.timestamp_subsec_nanos() as u64
    }

    /*
    See event 1636498894380250805
    See event 1636499776028981476

    See event 1636413555754299959
    See event 1636413555908344145
    See event 1636498896546533901
    See event 1636498896546540966
    See event 1636499855546054375
    See event 1636499914581647548
    See event 1636537592102377806
    See event 1636545517217768432
    See event 1636560318439777562
    See event 1636585292173222036
    See event 1636585292173229436
    */

    #[test]
    fn read_blocks_one_event_basic() -> Result<(), Error> {
        //let ta = "2021-11-09T10:00:00Z";
        //let tb = "2021-11-09T11:00:00Z";
        let _ev1 = "2021-10-03T09:57:59.939651334Z";
        let _ev2 = "2021-10-03T09:58:59.940910313Z";
        let _ev3 = "2021-10-03T09:59:59.940112431Z";
        // This is from bl SH index:
        //let ev1ts = 1633255079939651334;
        //let ev2ts = 1633255139940910313;
        // [ev1..ev2]
        //let beg = tons(&ta.parse()?);
        //let end = tons(&tb.parse()?);
        //let ev1ts = 1636498896546533901;
        //let ev2ts = 1636498896546540966;

        let beg = 1636455492985809049;
        let end = 1636455493306756248;
        let range = NanoRange { beg, end };
        let res = taskrun::run(count_events(range, false, true))?;
        assert_eq!(res.pre, 0);
        assert_eq!(res.inside, 1);
        assert_eq!(res.post, 0);
        assert_eq!(res.tss[0], beg);
        assert_eq!(res.raco, 1);
        Ok(())
    }

    #[test]
    fn read_blocks_one_event_expand() -> Result<(), Error> {
        // This is from bl SH index:
        //let ev1ts = 1633255079939651334;
        //let ev2ts = 1633255139940910313;
        let beg = 1636455492985809049;
        let end = 1636455493306756248;
        let range = NanoRange { beg, end };
        let res = taskrun::run(count_events(range, true, true))?;
        assert_eq!(res.pre, 1);
        assert_eq!(res.inside, 1);
        assert_eq!(res.post, 1);
        assert_eq!(res.tss[1], beg);
        assert_eq!(res.raco, 1);
        Ok(())
    }

    #[test]
    fn read_blocks_two_events_basic() -> Result<(), Error> {
        let beg = 1636455492985809049;
        let end = 1636455493306756248;
        let range = NanoRange { beg: beg, end: end + 1 };
        let res = taskrun::run(count_events(range, false, true))?;
        assert_eq!(res.pre, 0);
        assert_eq!(res.inside, 2);
        assert_eq!(res.post, 0);
        assert_eq!(res.tss[0], beg);
        assert_eq!(res.tss[1], end);
        assert_eq!(res.raco, 1);
        Ok(())
    }

    #[test]
    fn read_blocks_two_events_expand() -> Result<(), Error> {
        let beg = 1636455492985809049;
        let end = 1636455493306756248;
        let range = NanoRange { beg: beg, end: end + 1 };
        let res = taskrun::run(count_events(range, true, true))?;
        assert_eq!(res.pre, 1);
        assert_eq!(res.inside, 2);
        assert_eq!(res.post, 1);
        assert_eq!(res.tss[1], beg);
        assert_eq!(res.tss[2], end);
        assert_eq!(res.raco, 1);
        Ok(())
    }

    #[test]
    fn read_blocks_many_1_basic() -> Result<(), Error> {
        let dtbeg = "2021-11-09T10:00:00Z";
        let dtend = "2021-11-09T11:00:00Z";
        let dtbeg: DateTime<Utc> = dtbeg.parse()?;
        let dtend: DateTime<Utc> = dtend.parse()?;
        let range = NanoRange {
            beg: tons(&dtbeg),
            end: tons(&dtend),
        };
        let res = taskrun::run(count_events(range, false, false))?;
        assert_eq!(res.pre, 0);
        assert_eq!(res.inside, 726);
        assert_eq!(res.post, 0);
        assert_eq!(res.raco, 1);
        Ok(())
    }

    #[test]
    fn read_blocks_many_1_expand() -> Result<(), Error> {
        let dtbeg = "2021-11-09T10:00:00Z";
        let dtend = "2021-11-09T11:00:00Z";
        let dtbeg: DateTime<Utc> = dtbeg.parse()?;
        let dtend: DateTime<Utc> = dtend.parse()?;
        let range = NanoRange {
            beg: tons(&dtbeg),
            end: tons(&dtend),
        };
        let res = taskrun::run(count_events(range, true, false))?;
        assert_eq!(res.pre, 1);
        assert_eq!(res.inside, 726);
        assert_eq!(res.post, 1);
        assert_eq!(res.raco, 1);
        Ok(())
    }

    #[test]
    fn read_blocks_many_3_basic() -> Result<(), Error> {
        let dtbeg = "2021-11-09T10:00:00Z";
        let dtend = "2021-11-09T13:00:00Z";
        let range = NanoRange {
            beg: tons(&dtbeg.parse()?),
            end: tons(&dtend.parse()?),
        };
        let res = taskrun::run(count_events(range, false, false))?;
        assert_eq!(res.pre, 0);
        assert_eq!(res.inside, 2089);
        assert_eq!(res.post, 0);
        assert_eq!(res.raco, 1);
        Ok(())
    }

    #[test]
    fn read_blocks_many_3_expand() -> Result<(), Error> {
        let dtbeg = "2021-11-09T10:00:00Z";
        let dtend = "2021-11-09T13:00:00Z";
        let range = NanoRange {
            beg: tons(&dtbeg.parse()?),
            end: tons(&dtend.parse()?),
        };
        let res = taskrun::run(count_events(range, true, false))?;
        assert_eq!(res.pre, 1);
        assert_eq!(res.inside, 2089);
        assert_eq!(res.post, 1);
        assert_eq!(res.raco, 1);
        Ok(())
    }

    #[test]
    fn read_blocks_many_4_basic() -> Result<(), Error> {
        let dtbeg = "2020-11-09T10:00:00Z";
        let dtend = "2021-11-09T13:00:00Z";
        let range = NanoRange {
            beg: tons(&dtbeg.parse()?),
            end: tons(&dtend.parse()?),
        };
        let res = taskrun::run(count_events(range, false, false))?;
        assert_eq!(res.pre, 0);
        assert_eq!(res.inside, 9518);
        assert_eq!(res.post, 0);
        assert_eq!(res.raco, 1);
        Ok(())
    }

    #[test]
    fn read_blocks_many_4_expand() -> Result<(), Error> {
        let dtbeg = "2020-11-09T10:00:00Z";
        let dtend = "2021-11-09T13:00:00Z";
        let range = NanoRange {
            beg: tons(&dtbeg.parse()?),
            end: tons(&dtend.parse()?),
        };
        let res = taskrun::run(count_events(range, true, false))?;
        assert_eq!(res.pre, 0);
        assert_eq!(res.inside, 9518);
        assert_eq!(res.post, 1);
        assert_eq!(res.raco, 1);
        Ok(())
    }

    #[test]
    fn read_blocks_late_basic() -> Result<(), Error> {
        let dtbeg = "2021-11-09T10:00:00Z";
        let dtend = "2022-11-09T13:00:00Z";
        let range = NanoRange {
            beg: tons(&dtbeg.parse()?),
            end: tons(&dtend.parse()?),
        };
        let res = taskrun::run(count_events(range, false, false))?;
        assert_eq!(res.pre, 0);
        assert_eq!(res.inside, 12689);
        assert_eq!(res.post, 0);
        assert_eq!(res.raco, 0);
        Ok(())
    }

    #[test]
    fn read_blocks_late_expand() -> Result<(), Error> {
        let dtbeg = "2021-11-09T10:00:00Z";
        let dtend = "2022-11-09T13:00:00Z";
        let range = NanoRange {
            beg: tons(&dtbeg.parse()?),
            end: tons(&dtend.parse()?),
        };
        let res = taskrun::run(count_events(range, true, false))?;
        assert_eq!(res.pre, 1);
        assert_eq!(res.inside, 12689);
        assert_eq!(res.post, 0);
        assert_eq!(res.raco, 0);
        Ok(())
    }
}
