use crate::archeng::datablock::{read_data_1, read_datafile_header};
use crate::archeng::indexfiles::index_file_path_list;
use crate::archeng::indextree::{read_channel, read_datablockref, search_record, search_record_expand, DataheaderPos};
use crate::storagemerge::StorageMerge;
use crate::timed::Timed;
use async_channel::{Receiver, Sender};
use commonio::{open_read, StatsChannel};
use err::Error;
use futures_core::{Future, Stream};
use futures_util::{FutureExt, StreamExt};
use items::eventsitem::EventsItem;
use items::{inspect_timestamps, RangeCompletableItem, Sitemty, StreamItem, WithLen};
use netpod::log::*;
use netpod::{Channel, NanoRange};
use netpod::{FilePos, Nanos};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

type FR = (Option<Sitemty<EventsItem>>, Box<dyn FretCb>);

trait FretCb {
    fn call(&mut self, stream: &mut Pin<&mut DatablockStream>);
}

static CHANNEL_SEND_ERROR: AtomicUsize = AtomicUsize::new(0);

fn channel_send_error() {
    let c = CHANNEL_SEND_ERROR.fetch_add(1, Ordering::AcqRel);
    if c < 10 {
        error!("CHANNEL_SEND_ERROR {}", c);
    }
}

async fn datablock_stream(
    range: NanoRange,
    channel: Channel,
    index_files_index_path: PathBuf,
    _base_dirs: VecDeque<PathBuf>,
    expand: bool,
    tx: Sender<Sitemty<EventsItem>>,
    max_events: u64,
) {
    match datablock_stream_inner(range, channel, expand, index_files_index_path, tx.clone(), max_events).await {
        Ok(_) => {}
        Err(e) => {
            if let Err(_) = tx.send(Err(e)).await {
                channel_send_error();
            }
        }
    }
}

async fn datablock_stream_inner_single_index(
    range: NanoRange,
    channel: Channel,
    index_path: PathBuf,
    expand: bool,
    tx: Sender<Sitemty<EventsItem>>,
    max_events: u64,
) -> Result<(), Error> {
    let mut events_tot = 0;
    let stats = &StatsChannel::new(tx.clone());
    debug!("try to open index file: {:?}", index_path);
    let index_file = open_read(index_path.clone(), stats).await?;
    let mut file2 = open_read(index_path.clone(), stats).await?;
    debug!("opened index file: {:?}  {:?}", index_path, index_file);
    if let Some(basics) = read_channel(index_path.clone(), index_file, channel.name(), stats).await? {
        let beg = Nanos { ns: range.beg };
        let mut expand_beg = expand;
        let mut index_ts_max = 0;
        let mut search_ts = beg.clone();
        let mut last_data_file_path = PathBuf::new();
        let mut last_data_file_pos = DataheaderPos(0);
        loop {
            let timed_search = Timed::new("search next record");
            let (res, _stats) = if expand_beg {
                // TODO even though this is an entry in the index, it may reference
                // non-existent blocks.
                // Therefore, lower expand_beg flag at some later stage only if we've really
                // found at least one event in the block.
                expand_beg = false;
                search_record_expand(&mut file2, basics.rtree_m, basics.rtree_start_pos, search_ts, stats).await?
            } else {
                search_record(&mut file2, basics.rtree_m, basics.rtree_start_pos, search_ts, stats).await?
            };
            drop(timed_search);
            if let Some(nrec) = res {
                let rec = nrec.rec();
                trace!("found record: {:?}", rec);
                let pos = FilePos { pos: rec.child_or_id };
                // TODO rename Datablock?  → IndexNodeDatablock
                trace!("READ Datablock FROM {:?}\n", pos);
                let datablock = read_datablockref(&mut file2, pos, basics.hver(), stats).await?;
                trace!("Datablock: {:?}\n", datablock);
                let data_path = index_path.parent().unwrap().join(datablock.file_name());
                if data_path == last_data_file_path && datablock.data_header_pos() == last_data_file_pos {
                    debug!("skipping because it is the same block");
                } else {
                    trace!("try to open data_path: {:?}", data_path);
                    match open_read(data_path.clone(), stats).await {
                        Ok(mut data_file) => {
                            let datafile_header =
                                read_datafile_header(&mut data_file, datablock.data_header_pos(), stats).await?;
                            trace!("datafile_header --------------  HEADER\n{:?}", datafile_header);
                            let events =
                                read_data_1(&mut data_file, &datafile_header, range.clone(), expand_beg, stats).await?;
                            if false {
                                let msg = inspect_timestamps(&events, range.clone());
                                trace!("datablock_stream_inner_single_index  read_data_1\n{}", msg);
                            }
                            {
                                let mut ts_max = 0;
                                use items::WithTimestamps;
                                for i in 0..events.len() {
                                    let ts = events.ts(i);
                                    if ts < ts_max {
                                        error!("unordered event within block at ts {}", ts);
                                        break;
                                    } else {
                                        ts_max = ts;
                                    }
                                    if ts < index_ts_max {
                                        error!(
                                            "unordered event in index branch  ts {}  index_ts_max {}",
                                            ts, index_ts_max
                                        );
                                        break;
                                    } else {
                                        index_ts_max = ts;
                                    }
                                }
                            }
                            trace!("Was able to read data: {} events", events.len());
                            events_tot += events.len() as u64;
                            let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(events)));
                            tx.send(item).await?;
                        }
                        Err(e) => {
                            // That's fine. The index mentions lots of datafiles which got purged already.
                            trace!("can not find file mentioned in index: {:?}  {}", data_path, e);
                        }
                    };
                }
                if datablock.next().0 != 0 {
                    warn!("MAYBE TODO? datablock.next != 0:  {:?}", datablock);
                }
                last_data_file_path = data_path;
                last_data_file_pos = datablock.data_header_pos();
                // TODO anything special to do in expand mode?
                search_ts.ns = rec.ts2.ns;
            } else {
                warn!("nothing found, break");
                break;
            }
            if events_tot >= max_events {
                warn!("reached events_tot {}  max_events {}", events_tot, max_events);
                break;
            }
        }
    } else {
        warn!("can not read channel basics from {:?}", index_path);
    }
    Ok(())
}

async fn datablock_stream_inner(
    range: NanoRange,
    channel: Channel,
    expand: bool,
    index_files_index_path: PathBuf,
    tx: Sender<Sitemty<EventsItem>>,
    max_events: u64,
) -> Result<(), Error> {
    let stats = &StatsChannel::new(tx.clone());
    let index_file_path_list = index_file_path_list(channel.clone(), index_files_index_path, stats).await?;
    let mut inner_rxs = vec![];
    let mut names = vec![];
    for index_path in index_file_path_list {
        let (tx, rx) = async_channel::bounded(2);
        let task = datablock_stream_inner_single_index(
            range.clone(),
            channel.clone(),
            (&index_path).into(),
            expand,
            tx,
            max_events,
        );
        taskrun::spawn(task);
        inner_rxs.push(Box::pin(rx) as Pin<Box<dyn Stream<Item = Sitemty<EventsItem>> + Send>>);
        names.push(index_path.to_str().unwrap().into());
    }
    let task = async move {
        let mut inp = StorageMerge::new(inner_rxs, names, range.clone());
        while let Some(k) = inp.next().await {
            if let Err(_) = tx.send(k).await {
                channel_send_error();
                break;
            }
        }
    };
    taskrun::spawn(task);
    Ok(())
}

pub struct DatablockStream {
    range: NanoRange,
    channel: Channel,
    base_dirs: VecDeque<PathBuf>,
    expand: bool,
    fut: Pin<Box<dyn Future<Output = FR> + Send>>,
    rx: Receiver<Sitemty<EventsItem>>,
    done: bool,
    complete: bool,
}

impl DatablockStream {
    pub fn _for_channel_range(
        range: NanoRange,
        channel: Channel,
        base_dirs: VecDeque<PathBuf>,
        expand: bool,
        max_events: u64,
    ) -> Self {
        let (tx, rx) = async_channel::bounded(1);
        taskrun::spawn(datablock_stream(
            range.clone(),
            channel.clone(),
            "/index/c5mapped".into(),
            base_dirs.clone(),
            expand.clone(),
            tx,
            max_events,
        ));
        let ret = Self {
            range,
            channel,
            base_dirs: VecDeque::new(),
            expand,
            fut: Box::pin(Self::start()),
            rx,
            done: false,
            complete: false,
        };
        // TODO keeping for compatibility at the moment:
        let _ = &ret.range;
        let _ = &ret.channel;
        let _ = &ret.expand;
        ret
    }

    async fn start() -> FR {
        struct Cb {}
        impl FretCb for Cb {
            fn call(&mut self, stream: &mut Pin<&mut DatablockStream>) {
                if let Some(path) = stream.base_dirs.pop_front() {
                    stream.fut = Box::pin(DatablockStream::start_with_base_dir(path));
                } else {
                    // TODO: done?
                    err::todo();
                }
            }
        }
        (None, Box::new(Cb {}))
    }

    async fn start_with_base_dir(_path: PathBuf) -> FR {
        warn!("start_with_base_dir");
        struct Cb {}
        impl FretCb for Cb {
            fn call(&mut self, stream: &mut Pin<&mut DatablockStream>) {
                let _ = stream;
            }
        }
        (None, Box::new(Cb {}))
    }
}

/*
Loop through all configured data directories.
Locate the index file.
Search for the correct Datablock to start with.
Iterate from there.
*/

impl Stream for DatablockStream {
    type Item = Sitemty<EventsItem>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if false {
                match self.fut.poll_unpin(cx) {
                    Ready((k, mut fr)) => {
                        fr.call(&mut self);
                        match k {
                            Some(item) => Ready(Some(item)),
                            None => continue,
                        }
                    }
                    Pending => Pending,
                }
            } else {
                self.rx.poll_next_unpin(cx)
            };
        }
    }
}

#[cfg(test)]
mod test {
    use super::DatablockStream;
    use chrono::{DateTime, Utc};
    use err::Error;
    use futures_util::StreamExt;
    use items::eventsitem::EventsItem;
    use items::{LogItem, Sitemty, StatsItem, StreamItem};
    use netpod::log::*;
    use netpod::timeunits::SEC;
    use netpod::{Channel, NanoRange, RangeFilterStats};
    use serde::Serialize;
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use streams::rangefilter::RangeFilter;

    #[test]
    fn read_file_basic_info() -> Result<(), Error> {
        // TODO redo test
        if true {
            panic!();
        }
        let fut = async {
            // file begin archive_X05DA_SH/20211001/20211001: 1633039259
            // 1633145759
            // October 1st CEST: 1633039200
            // archive_X05DA_SH/20210901/20210920  (has no next-links)
            // maybe there is no linking across files?
            // now in this case, there is a `next`. Does the rtree also contain an entry for that?
            let beg = "2021-10-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
            let end = "2021-10-01T02:00:00Z".parse::<DateTime<Utc>>().unwrap();
            let range = NanoRange {
                beg: beg.timestamp() as u64 * SEC,
                end: end.timestamp() as u64 * SEC,
            };
            let channel = Channel {
                backend: "test-archapp".into(),
                name: "X05DA-FE-WI1:TC1".into(),
            };
            let base_dirs: VecDeque<_> = ["/data/daqbuffer-testdata/sls/gfa03/bl_arch"]
                .iter()
                .map(PathBuf::from)
                .collect();
            let expand = false;
            let datablocks = DatablockStream::_for_channel_range(range.clone(), channel, base_dirs, expand, u64::MAX);
            let filtered = RangeFilter::<_, EventsItem>::new(datablocks, range, expand);
            let mut stream = filtered;
            while let Some(block) = stream.next().await {
                match block {
                    Ok(_) => {
                        //TODO assert more
                    }
                    Err(e) => return Err(e),
                }
            }
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
