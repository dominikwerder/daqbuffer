use crate::dataopen::{open_expanded_files, open_files, OpenedFileSet};
use crate::eventchunker::{EventChunker, EventChunkerConf, EventFull};
use crate::file_content_stream;
use crate::merge::MergedStream;
use crate::rangefilter::RangeFilter;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::{LogItem, RangeCompletableItem, Sitemty, StreamItem};
use netpod::timeunits::SEC;
use netpod::{log::*, FileIoBufferSize};
use netpod::{ChannelConfig, NanoRange, Node};
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::task::{Context, Poll};

pub trait InputTraits: Stream<Item = Sitemty<EventFull>> {}

impl<T> InputTraits for T where T: Stream<Item = Sitemty<EventFull>> {}

pub struct EventChunkerMultifile {
    channel_config: ChannelConfig,
    file_chan: async_channel::Receiver<Result<OpenedFileSet, Error>>,
    evs: Option<Pin<Box<dyn InputTraits + Send>>>,
    file_io_buffer_size: FileIoBufferSize,
    event_chunker_conf: EventChunkerConf,
    range: NanoRange,
    data_completed: bool,
    errored: bool,
    completed: bool,
    max_ts: Arc<AtomicU64>,
    files_count: u32,
    node_ix: usize,
    expand: bool,
    do_decompress: bool,
}

impl EventChunkerMultifile {
    pub fn new(
        range: NanoRange,
        channel_config: ChannelConfig,
        node: Node,
        node_ix: usize,
        file_io_buffer_size: FileIoBufferSize,
        event_chunker_conf: EventChunkerConf,
        expand: bool,
        do_decompress: bool,
    ) -> Self {
        let file_chan = if expand {
            open_expanded_files(&range, &channel_config, node)
        } else {
            open_files(&range, &channel_config, node)
        };
        Self {
            file_chan,
            evs: None,
            file_io_buffer_size,
            event_chunker_conf,
            channel_config,
            range,
            data_completed: false,
            errored: false,
            completed: false,
            max_ts: Arc::new(AtomicU64::new(0)),
            files_count: 0,
            node_ix,
            expand,
            do_decompress,
        }
    }
}

impl Stream for EventChunkerMultifile {
    type Item = Result<StreamItem<RangeCompletableItem<EventFull>>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let span1 = span!(Level::INFO, "EventChunkerMultifile", desc = tracing::field::Empty);
        span1.record("desc", &"");
        span1.in_scope(|| {
            use Poll::*;
            'outer: loop {
                break if self.completed {
                    panic!("EventBlobsComplete  poll_next on completed");
                } else if self.errored {
                    self.completed = true;
                    return Ready(None);
                } else if self.data_completed {
                    self.completed = true;
                    return Ready(None);
                } else {
                    match &mut self.evs {
                        Some(evs) => match evs.poll_next_unpin(cx) {
                            Ready(Some(k)) => {
                                if let Ok(StreamItem::DataItem(RangeCompletableItem::Data(h))) = &k {
                                    if false {
                                        info!("EventChunkerMultifile  emit {} events", h.tss.len());
                                    };
                                }
                                Ready(Some(k))
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
                                        info!("{}", msg);
                                        let item = LogItem::quick(Level::INFO, msg);
                                        match file.file {
                                            Some(file) => {
                                                let inp = Box::pin(file_content_stream(
                                                    file,
                                                    self.file_io_buffer_size.clone(),
                                                ));
                                                let chunker = EventChunker::from_event_boundary(
                                                    inp,
                                                    self.channel_config.clone(),
                                                    self.range.clone(),
                                                    self.event_chunker_conf.clone(),
                                                    path,
                                                    self.max_ts.clone(),
                                                    self.expand,
                                                    self.do_decompress,
                                                );
                                                let filtered =
                                                    RangeFilter::new(chunker, self.range.clone(), self.expand);
                                                self.evs = Some(Box::pin(filtered));
                                            }
                                            None => {}
                                        }
                                        Ready(Some(Ok(StreamItem::Log(item))))
                                    } else if ofs.files.len() == 0 {
                                        let msg = format!("handle OFS {:?}  NO FILES", ofs);
                                        info!("{}", msg);
                                        let item = LogItem::quick(Level::INFO, msg);
                                        Ready(Some(Ok(StreamItem::Log(item))))
                                    } else {
                                        let msg = format!("handle OFS MERGED {:?}", ofs);
                                        warn!("{}", msg);
                                        let item = LogItem::quick(Level::INFO, msg);
                                        let mut chunkers = vec![];
                                        for of in ofs.files {
                                            if let Some(file) = of.file {
                                                let inp = Box::pin(file_content_stream(
                                                    file,
                                                    self.file_io_buffer_size.clone(),
                                                ));
                                                let chunker = EventChunker::from_event_boundary(
                                                    inp,
                                                    self.channel_config.clone(),
                                                    self.range.clone(),
                                                    self.event_chunker_conf.clone(),
                                                    of.path,
                                                    self.max_ts.clone(),
                                                    self.expand,
                                                    self.do_decompress,
                                                );
                                                chunkers.push(chunker);
                                            }
                                        }
                                        let merged = MergedStream::new(chunkers);
                                        let filtered = RangeFilter::new(merged, self.range.clone(), self.expand);
                                        self.evs = Some(Box::pin(filtered));
                                        Ready(Some(Ok(StreamItem::Log(item))))
                                    }
                                }
                                Err(e) => {
                                    self.errored = true;
                                    Ready(Some(Err(e)))
                                }
                            },
                            Ready(None) => {
                                self.data_completed = true;
                                let item = LogItem::quick(
                                    Level::INFO,
                                    format!(
                                        "EventBlobsComplete used {} datafiles  beg {}  end {}  node_ix {}",
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
        })
    }
}

#[cfg(test)]
mod test {
    use crate::rangefilter::RangeFilter;
    use crate::{eventblobs::EventChunkerMultifile, eventchunker::EventChunkerConf};
    use err::Error;
    use futures_util::StreamExt;
    use items::{RangeCompletableItem, StreamItem};
    use netpod::log::*;
    use netpod::timeunits::{DAY, MS};
    use netpod::{ByteSize, ChannelConfig, FileIoBufferSize, Nanos};

    fn read_expanded_for_range(range: netpod::NanoRange, nodeix: usize) -> Result<(usize, Vec<u64>), Error> {
        let chn = netpod::Channel {
            backend: "testbackend".into(),
            name: "scalar-i32-be".into(),
        };
        // TODO read config from disk.
        let channel_config = ChannelConfig {
            channel: chn,
            keyspace: 2,
            time_bin_size: Nanos { ns: DAY },
            scalar_type: netpod::ScalarType::I32,
            byte_order: netpod::ByteOrder::big_endian(),
            shape: netpod::Shape::Scalar,
            array: false,
            compression: false,
        };
        let cluster = taskrun::test_cluster();
        let node = cluster.nodes[nodeix].clone();
        let buffer_size = 512;
        let event_chunker_conf = EventChunkerConf {
            disk_stats_every: ByteSize::kb(1024),
        };
        let task = async move {
            let mut event_count = 0;
            let events = EventChunkerMultifile::new(
                range.clone(),
                channel_config,
                node,
                nodeix,
                FileIoBufferSize::new(buffer_size),
                event_chunker_conf,
                true,
                true,
            );
            //let mut events = MergedStream::new(vec![events], range.clone(), true);
            let mut events = RangeFilter::new(events, range.clone(), true);
            let mut tss = vec![];
            while let Some(item) = events.next().await {
                match item {
                    Ok(item) => match item {
                        StreamItem::DataItem(item) => match item {
                            RangeCompletableItem::Data(item) => {
                                info!("item: {:?}", item.tss.iter().map(|x| x / MS).collect::<Vec<_>>());
                                event_count += item.tss.len();
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
        let range = netpod::NanoRange {
            beg: DAY + MS * 0,
            end: DAY + MS * 100,
        };
        let res = read_expanded_for_range(range, 0)?;
        info!("got {:?}", res.1);
        if res.0 != 3 {
            Err(Error::with_msg(format!("unexpected number of events: {}", res.0)))?;
        }
        assert_eq!(res.1, vec![DAY - MS * 1500, DAY, DAY + MS * 1500]);
        Ok(())
    }

    #[test]
    fn read_expanded_1() -> Result<(), Error> {
        let range = netpod::NanoRange {
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
        let range = netpod::NanoRange {
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
        let range = netpod::NanoRange {
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
