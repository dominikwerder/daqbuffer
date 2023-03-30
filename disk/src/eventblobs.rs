use crate::dataopen::open_expanded_files;
use crate::dataopen::open_files;
use crate::dataopen::OpenedFileSet;
use crate::eventchunker::EventChunker;
use crate::eventchunker::EventChunkerConf;
use crate::SfDbChConf;
use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::WithLen;
use items_2::eventfull::EventFull;
use items_2::merger::Merger;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::SEC;
use netpod::DiskIoTune;
use netpod::Node;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use streams::rangefilter2::RangeFilter2;

pub trait InputTraits: Stream<Item = Sitemty<EventFull>> {}

impl<T> InputTraits for T where T: Stream<Item = Sitemty<EventFull>> {}

pub struct EventChunkerMultifile {
    channel_config: SfDbChConf,
    file_chan: async_channel::Receiver<Result<OpenedFileSet, Error>>,
    evs: Option<Pin<Box<dyn InputTraits + Send>>>,
    disk_io_tune: DiskIoTune,
    event_chunker_conf: EventChunkerConf,
    range: NanoRange,
    files_count: u32,
    node_ix: usize,
    expand: bool,
    do_decompress: bool,
    max_ts: u64,
    out_max_len: usize,
    emit_count: usize,
    do_emit_err_after: Option<usize>,
    range_final: bool,
    log_queue: VecDeque<LogItem>,
    done: bool,
    done_emit_range_final: bool,
    complete: bool,
}

impl EventChunkerMultifile {
    pub fn new(
        range: NanoRange,
        channel_config: SfDbChConf,
        node: Node,
        node_ix: usize,
        disk_io_tune: DiskIoTune,
        event_chunker_conf: EventChunkerConf,
        expand: bool,
        do_decompress: bool,
        out_max_len: usize,
    ) -> Self {
        info!("EventChunkerMultifile  expand {expand}  do_decompress {do_decompress}");
        let file_chan = if expand {
            open_expanded_files(&range, &channel_config, node)
        } else {
            open_files(&range, &channel_config, node)
        };
        Self {
            file_chan,
            evs: None,
            disk_io_tune,
            event_chunker_conf,
            channel_config,
            range,
            files_count: 0,
            node_ix,
            expand,
            do_decompress,
            max_ts: 0,
            out_max_len,
            emit_count: 0,
            do_emit_err_after: None,
            range_final: false,
            log_queue: VecDeque::new(),
            done: false,
            done_emit_range_final: false,
            complete: false,
        }
    }
}

impl Stream for EventChunkerMultifile {
    type Item = Result<StreamItem<RangeCompletableItem<EventFull>>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let span1 = span!(Level::INFO, "EvChMul", node_ix = self.node_ix);
        let _spg = span1.enter();
        use Poll::*;
        'outer: loop {
            break if let Some(item) = self.log_queue.pop_front() {
                Ready(Some(Ok(StreamItem::Log(item))))
            } else if self.complete {
                panic!("EventChunkerMultifile  poll_next on complete");
            } else if self.done_emit_range_final {
                self.complete = true;
                Ready(None)
            } else if self.done {
                self.done_emit_range_final = true;
                if self.range_final {
                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                } else {
                    continue;
                }
            } else {
                match &mut self.evs {
                    Some(evs) => match evs.poll_next_unpin(cx) {
                        Ready(Some(Ok(k))) => {
                            let k = if let StreamItem::DataItem(RangeCompletableItem::Data(h)) = k {
                                let mut h: EventFull = h;
                                if h.len() > 0 {
                                    let min = h.tss.iter().fold(u64::MAX, |a, &x| a.min(x));
                                    let max = h.tss.iter().fold(u64::MIN, |a, &x| a.max(x));
                                    if min <= self.max_ts {
                                        let msg = format!("EventChunkerMultifile  repeated or unordered ts {}", min);
                                        error!("{}", msg);
                                        let item = LogItem {
                                            node_ix: self.node_ix as _,
                                            level: Level::INFO,
                                            msg,
                                        };
                                        self.log_queue.push_back(item);
                                    }
                                    self.max_ts = max;
                                    if let Some(after) = self.do_emit_err_after {
                                        if self.emit_count < after {
                                            debug!(
                                                "EventChunkerMultifile  emit {}/{}  events {}",
                                                self.emit_count,
                                                after,
                                                h.len()
                                            );
                                            self.emit_count += 1;
                                        }
                                    }
                                    if max >= self.range.end {
                                        self.range_final = true;
                                        h.truncate_ts(self.range.end);
                                        self.evs = None;
                                        let (tx, rx) = async_channel::bounded(1);
                                        drop(tx);
                                        self.file_chan = rx;
                                    }
                                }
                                StreamItem::DataItem(RangeCompletableItem::Data(h))
                            } else {
                                k
                            };
                            Ready(Some(Ok(k)))
                        }
                        Ready(Some(Err(e))) => {
                            error!("{e}");
                            self.done = true;
                            Ready(Some(Err(e)))
                        }
                        Ready(None) => {
                            self.evs = None;
                            continue 'outer;
                        }
                        Pending => Pending,
                    },
                    None => match self.file_chan.poll_next_unpin(cx) {
                        Ready(Some(k)) => match k {
                            Ok(ofs) => {
                                self.files_count += ofs.files.len() as u32;
                                if ofs.files.len() == 1 {
                                    let mut ofs = ofs;
                                    let file = ofs.files.pop().unwrap();
                                    let path = file.path;
                                    let msg = format!("handle OFS {:?}", ofs);
                                    debug!("{}", msg);
                                    let item = LogItem::quick(Level::INFO, msg);
                                    match file.file {
                                        Some(file) => {
                                            let inp = Box::pin(crate::file_content_stream(
                                                path.clone(),
                                                file,
                                                self.disk_io_tune.clone(),
                                            ));
                                            let chunker = EventChunker::from_event_boundary(
                                                inp,
                                                self.channel_config.clone(),
                                                self.range.clone(),
                                                self.event_chunker_conf.clone(),
                                                path.clone(),
                                                self.expand,
                                                self.do_decompress,
                                            );
                                            let filtered = RangeFilter2::new(chunker, self.range.clone(), self.expand);
                                            self.evs = Some(Box::pin(filtered));
                                        }
                                        None => {}
                                    }
                                    Ready(Some(Ok(StreamItem::Log(item))))
                                } else if ofs.files.len() == 0 {
                                    let msg = format!("handle OFS {:?}  NO FILES", ofs);
                                    debug!("{}", msg);
                                    let item = LogItem::quick(Level::INFO, msg);
                                    Ready(Some(Ok(StreamItem::Log(item))))
                                } else {
                                    let msg = format!("handle OFS MERGED timebin {}", ofs.timebin);
                                    info!("{}", msg);
                                    for x in &ofs.files {
                                        info!("   path {:?}", x.path);
                                    }
                                    let item = LogItem::quick(Level::INFO, msg);
                                    let mut chunkers = Vec::new();
                                    for of in ofs.files {
                                        if let Some(file) = of.file {
                                            let inp = crate::file_content_stream(
                                                of.path.clone(),
                                                file,
                                                self.disk_io_tune.clone(),
                                            );
                                            let chunker = EventChunker::from_event_boundary(
                                                inp,
                                                self.channel_config.clone(),
                                                self.range.clone(),
                                                self.event_chunker_conf.clone(),
                                                of.path.clone(),
                                                self.expand,
                                                self.do_decompress,
                                            );
                                            chunkers.push(Box::pin(chunker) as _);
                                        }
                                    }
                                    let merged = Merger::new(chunkers, self.out_max_len);
                                    let filtered = RangeFilter2::new(merged, self.range.clone(), self.expand);
                                    self.evs = Some(Box::pin(filtered));
                                    Ready(Some(Ok(StreamItem::Log(item))))
                                }
                            }
                            Err(e) => {
                                self.done = true;
                                Ready(Some(Err(e)))
                            }
                        },
                        Ready(None) => {
                            self.done = true;
                            let item = LogItem::quick(
                                Level::INFO,
                                format!(
                                    "EventChunkerMultifile used {} datafiles  beg {}  end {}  node_ix {}",
                                    self.files_count,
                                    self.range.beg / SEC,
                                    self.range.end / SEC,
                                    self.node_ix
                                ),
                            );
                            Ready(Some(Ok(StreamItem::Log(item))))
                        }
                        Pending => Pending,
                    },
                }
            };
        }
    }
}

#[cfg(test)]
mod test {
    use crate::eventblobs::EventChunkerMultifile;
    use crate::eventchunker::EventChunkerConf;
    use crate::SfDbChConf;
    use err::Error;
    use futures_util::StreamExt;
    use items_0::streamitem::RangeCompletableItem;
    use items_0::streamitem::StreamItem;
    use items_0::WithLen;
    use netpod::log::*;
    use netpod::range::evrange::NanoRange;
    use netpod::timeunits::DAY;
    use netpod::timeunits::MS;
    use netpod::ByteSize;
    use netpod::DiskIoTune;
    use netpod::TsNano;
    use streams::rangefilter2::RangeFilter2;

    fn read_expanded_for_range(range: NanoRange, nodeix: usize) -> Result<(usize, Vec<u64>), Error> {
        let chn = netpod::Channel {
            backend: "test-disk-databuffer".into(),
            name: "scalar-i32-be".into(),
            series: None,
        };
        // TODO read config from disk.
        let channel_config = SfDbChConf {
            channel: chn,
            keyspace: 2,
            time_bin_size: TsNano(DAY),
            scalar_type: netpod::ScalarType::I32,
            byte_order: netpod::ByteOrder::Big,
            shape: netpod::Shape::Scalar,
            array: false,
            compression: false,
        };
        let cluster = netpod::test_cluster();
        let node = cluster.nodes[nodeix].clone();
        let event_chunker_conf = EventChunkerConf {
            disk_stats_every: ByteSize::kb(1024),
        };
        let disk_io_tune = DiskIoTune::default_for_testing();
        let task = async move {
            let mut event_count = 0;
            let events = EventChunkerMultifile::new(
                range.clone(),
                channel_config,
                node,
                nodeix,
                disk_io_tune,
                event_chunker_conf,
                true,
                true,
                // TODO do asserts depend on this?
                32,
            );
            //let mut events = MergedStream::new(vec![events], range.clone(), true);
            let mut events = RangeFilter2::new(events, range.clone(), true);
            let mut tss = Vec::new();
            while let Some(item) = events.next().await {
                match item {
                    Ok(item) => match item {
                        StreamItem::DataItem(item) => match item {
                            RangeCompletableItem::Data(item) => {
                                // TODO assert more
                                debug!("item: {:?}", item.tss.iter().map(|x| x / MS).collect::<Vec<_>>());
                                event_count += item.len();
                                for ts in item.tss {
                                    tss.push(ts);
                                }
                            }
                            _ => {}
                        },
                        _ => {}
                    },
                    Err(e) => return Err(e.into()),
                }
            }
            Ok((event_count, tss))
        };
        Ok(taskrun::run(task).unwrap())
    }

    #[test]
    fn read_expanded_0() -> Result<(), Error> {
        let range = NanoRange {
            beg: DAY + MS * 0,
            end: DAY + MS * 100,
        };
        let res = read_expanded_for_range(range, 0)?;
        // TODO assert more
        debug!("got {:?}", res.1);
        if res.0 != 3 {
            Err(Error::with_msg(format!("unexpected number of events: {}", res.0)))?;
        }
        assert_eq!(res.1, vec![DAY - MS * 1500, DAY, DAY + MS * 1500]);
        Ok(())
    }

    #[test]
    fn read_expanded_1() -> Result<(), Error> {
        let range = NanoRange {
            beg: DAY + MS * 0,
            end: DAY + MS * 1501,
        };
        let res = read_expanded_for_range(range, 0)?;
        if res.0 != 4 {
            Err(Error::with_msg(format!("unexpected number of events: {}", res.0)))?;
        }
        assert_eq!(res.1, vec![DAY - MS * 1500, DAY, DAY + MS * 1500, DAY + MS * 3000]);
        Ok(())
    }

    #[test]
    fn read_expanded_2() -> Result<(), Error> {
        let range = NanoRange {
            beg: DAY - MS * 100,
            end: DAY + MS * 1501,
        };
        let res = read_expanded_for_range(range, 0)?;
        assert_eq!(res.1, vec![DAY - MS * 1500, DAY, DAY + MS * 1500, DAY + MS * 3000]);
        Ok(())
    }

    #[test]
    fn read_expanded_3() -> Result<(), Error> {
        use netpod::timeunits::*;
        let range = NanoRange {
            beg: DAY - MS * 1500,
            end: DAY + MS * 1501,
        };
        let res = read_expanded_for_range(range, 0)?;
        assert_eq!(
            res.1,
            vec![DAY - MS * 3000, DAY - MS * 1500, DAY, DAY + MS * 1500, DAY + MS * 3000]
        );
        Ok(())
    }
}
