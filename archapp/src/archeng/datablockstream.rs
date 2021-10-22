use crate::archeng::{
    open_read, read_channel, read_data_1, read_datafile_header, read_index_datablockref, search_record,
};
use crate::EventsItem;
use async_channel::{Receiver, Sender};
use err::Error;
use futures_core::{Future, Stream};
use futures_util::{FutureExt, StreamExt};
use items::{RangeCompletableItem, Sitemty, StreamItem, WithLen};
use netpod::{log::*, DataHeaderPos, FilePos, Nanos};
use netpod::{Channel, NanoRange};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

type FR = (Option<Sitemty<EventsItem>>, Box<dyn FretCb>);

trait FretCb {
    fn call(&mut self, stream: &mut Pin<&mut DatablockStream>);
}

async fn datablock_stream(
    range: NanoRange,
    channel: Channel,
    base_dirs: VecDeque<PathBuf>,
    expand: bool,
    tx: Sender<Sitemty<EventsItem>>,
) {
    match datablock_stream_inner(range, channel, base_dirs, expand, tx.clone()).await {
        Ok(_) => {}
        Err(e) => match tx.send(Err(e)).await {
            Ok(_) => {}
            Err(e) => {
                if false {
                    error!("can not send. error: {}", e);
                }
            }
        },
    }
}

async fn datablock_stream_inner(
    range: NanoRange,
    channel: Channel,
    base_dirs: VecDeque<PathBuf>,
    expand: bool,
    tx: Sender<Sitemty<EventsItem>>,
) -> Result<(), Error> {
    let basename = channel
        .name()
        .split("-")
        .next()
        .ok_or(Error::with_msg_no_trace("can not find base for channel"))?;
    for base in base_dirs {
        debug!(
            "search for {:?} with basename: {}  in path {:?}",
            channel, basename, base
        );
        // TODO need to try both:
        let index_path = base.join(format!("archive_{}_SH", basename)).join("index");
        let res = open_read(index_path.clone()).await;
        debug!("tried to open index file: {:?}", res);
        if let Ok(mut index_file) = res {
            if let Some(basics) = read_channel(&mut index_file, channel.name()).await? {
                let beg = Nanos { ns: range.beg };
                let mut search_ts = beg.clone();
                let mut last_data_file_path = PathBuf::new();
                let mut last_data_file_pos = DataHeaderPos(0);
                loop {
                    // TODO for expand mode, this needs another search function.
                    let (res, _stats) =
                        search_record(&mut index_file, basics.rtree_m, basics.rtree_start_pos, search_ts).await?;
                    if let Some(nrec) = res {
                        let rec = nrec.rec();
                        trace!("found record: {:?}", rec);
                        let pos = FilePos { pos: rec.child_or_id };
                        // TODO rename Datablock?  → IndexNodeDatablock
                        trace!("READ Datablock FROM {:?}\n", pos);
                        let datablock = read_index_datablockref(&mut index_file, pos).await?;
                        trace!("Datablock: {:?}\n", datablock);
                        let data_path = index_path.parent().unwrap().join(datablock.file_name());
                        if data_path == last_data_file_path && datablock.data_header_pos() == last_data_file_pos {
                            debug!("skipping because it is the same block");
                        } else {
                            trace!("try to open data_path: {:?}", data_path);
                            match open_read(data_path.clone()).await {
                                Ok(mut data_file) => {
                                    let datafile_header =
                                        read_datafile_header(&mut data_file, datablock.data_header_pos()).await?;
                                    trace!("datafile_header --------------  HEADER\n{:?}", datafile_header);
                                    let events = read_data_1(&mut data_file, &datafile_header).await?;
                                    trace!("Was able to read data: {} events", events.len());
                                    let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(events)));
                                    tx.send(item).await?;
                                }
                                Err(e) => {
                                    // That's fine. The index mentions lots of datafiles which got purged already.
                                    trace!("can not find file mentioned in index: {:?}  {}", data_path, e);
                                }
                            };
                        }
                        if datablock.next != 0 {
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
                }
            }
        } else {
            warn!("can not find index file at {:?}", index_path);
        }
    }
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
    pub fn for_channel_range(range: NanoRange, channel: Channel, base_dirs: VecDeque<PathBuf>, expand: bool) -> Self {
        let (tx, rx) = async_channel::bounded(1);
        taskrun::spawn(datablock_stream(
            range.clone(),
            channel.clone(),
            base_dirs.clone(),
            expand.clone(),
            tx,
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

    async fn start_with_base_dir(path: PathBuf) -> FR {
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
            } else if true {
                self.rx.poll_next_unpin(cx)
            } else {
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
            };
        }
    }
}

#[cfg(test)]
mod test {
    use crate::EventsItem;

    use super::DatablockStream;
    use chrono::{DateTime, Utc};
    use err::Error;
    use futures_util::StreamExt;
    use items::{LogItem, Sitemty, StatsItem, StreamItem};
    use netpod::timeunits::SEC;
    use netpod::{log::*, RangeFilterStats};
    use netpod::{Channel, NanoRange};
    use serde::Serialize;
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use streams::rangefilter::RangeFilter;

    #[test]
    fn read_file_basic_info() -> Result<(), Error> {
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
            let datablocks = DatablockStream::for_channel_range(range.clone(), channel, base_dirs, expand);
            let filtered = RangeFilter::<_, EventsItem>::new(datablocks, range, expand);
            let mut stream = filtered;
            while let Some(block) = stream.next().await {
                info!("DatablockStream yields: {:?}", block);
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
