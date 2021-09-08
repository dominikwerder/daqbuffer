use crate::dataopen::{open_expanded_files, open_files, OpenedFile};
use crate::eventchunker::{EventChunker, EventChunkerConf, EventFull};
use crate::file_content_stream;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::{LogItem, RangeCompletableItem, StreamItem};
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{ChannelConfig, NanoRange, Node};
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct EventChunkerMultifile {
    channel_config: ChannelConfig,
    file_chan: async_channel::Receiver<Result<OpenedFile, Error>>,
    evs: Option<EventChunker>,
    buffer_size: usize,
    event_chunker_conf: EventChunkerConf,
    range: NanoRange,
    data_completed: bool,
    errored: bool,
    completed: bool,
    max_ts: Arc<AtomicU64>,
    files_count: u32,
    node_ix: usize,
    seen_before_range_count: usize,
    seen_after_range_count: usize,
    expand: bool,
}

impl EventChunkerMultifile {
    pub fn new(
        range: NanoRange,
        channel_config: ChannelConfig,
        node: Node,
        node_ix: usize,
        buffer_size: usize,
        event_chunker_conf: EventChunkerConf,
        expand: bool,
    ) -> Self {
        let file_chan = if expand {
            open_expanded_files(&range, &channel_config, node)
        } else {
            open_files(&range, &channel_config, node)
        };
        Self {
            file_chan,
            evs: None,
            buffer_size,
            event_chunker_conf,
            channel_config,
            range,
            data_completed: false,
            errored: false,
            completed: false,
            max_ts: Arc::new(AtomicU64::new(0)),
            files_count: 0,
            node_ix,
            seen_before_range_count: 0,
            seen_after_range_count: 0,
            expand,
        }
    }

    pub fn seen_before_range_count(&self) -> usize {
        self.seen_before_range_count
    }

    pub fn seen_after_range_count(&self) -> usize {
        self.seen_after_range_count
    }

    pub fn close(&mut self) {
        if let Some(evs) = &mut self.evs {
            self.seen_before_range_count += evs.seen_before_range_count();
            self.evs = None;
        }
    }
}

impl Stream for EventChunkerMultifile {
    type Item = Result<StreamItem<RangeCompletableItem<EventFull>>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
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
                        Ready(Some(k)) => Ready(Some(k)),
                        Ready(None) => {
                            self.seen_before_range_count += evs.seen_before_range_count();
                            self.evs = None;
                            continue 'outer;
                        }
                        Pending => Pending,
                    },
                    None => match self.file_chan.poll_next_unpin(cx) {
                        Ready(Some(k)) => match k {
                            Ok(file) => {
                                self.files_count += 1;
                                let path = file.path;
                                let item = LogItem::quick(Level::INFO, format!("handle file {:?}", path));
                                match file.file {
                                    Some(file) => {
                                        let inp = Box::pin(file_content_stream(file, self.buffer_size as usize));
                                        let chunker = EventChunker::from_event_boundary(
                                            inp,
                                            self.channel_config.clone(),
                                            self.range.clone(),
                                            self.event_chunker_conf.clone(),
                                            path,
                                            self.max_ts.clone(),
                                            self.expand,
                                        );
                                        self.evs = Some(chunker);
                                    }
                                    None => {}
                                }
                                Ready(Some(Ok(StreamItem::Log(item))))
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
    }
}

#[cfg(test)]
fn read_expanded_for_range(range: netpod::NanoRange) -> Result<(usize, usize), Error> {
    use netpod::timeunits::*;
    use netpod::{ByteSize, Nanos};
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
    let node_ix = 0;
    let node = cluster.nodes[node_ix].clone();
    let buffer_size = 512;
    let event_chunker_conf = EventChunkerConf {
        disk_stats_every: ByteSize::kb(1024),
    };
    let task = async move {
        let mut event_count = 0;
        let mut events = EventChunkerMultifile::new(
            range,
            channel_config,
            node,
            node_ix,
            buffer_size,
            event_chunker_conf,
            true,
        );
        while let Some(item) = events.next().await {
            match item {
                Ok(item) => match item {
                    StreamItem::DataItem(item) => match item {
                        RangeCompletableItem::Data(item) => {
                            event_count += item.tss.len();
                        }
                        _ => {}
                    },
                    _ => {}
                },
                Err(e) => return Err(e.into()),
            }
        }
        events.close();
        if events.seen_before_range_count() != 1 {
            return Err(Error::with_msg(format!(
                "seen_before_range_count error: {}",
                events.seen_before_range_count(),
            )));
        }
        Ok((event_count, events.seen_before_range_count()))
    };
    Ok(taskrun::run(task).unwrap())
}

#[test]
fn read_expanded_0() -> Result<(), Error> {
    use netpod::timeunits::*;
    let range = netpod::NanoRange {
        beg: DAY + MS * 0,
        end: DAY + MS * 0 + MS * 1500,
    };
    let res = read_expanded_for_range(range)?;
    if res.0 != 2 {
        Err(Error::with_msg(format!("unexpected number of events: {}", res.0)))?;
    }
    Ok(())
}

#[test]
fn read_expanded_1() -> Result<(), Error> {
    use netpod::timeunits::*;
    let range = netpod::NanoRange {
        beg: DAY + MS * 0,
        end: DAY + MS * 0 + MS * 1501,
    };
    let res = read_expanded_for_range(range)?;
    if res.0 != 3 {
        Err(Error::with_msg(format!("unexpected number of events: {}", res.0)))?;
    }
    Ok(())
}

#[test]
fn read_expanded_2() -> Result<(), Error> {
    use netpod::timeunits::*;
    let range = netpod::NanoRange {
        beg: DAY - MS * 100,
        end: DAY + MS * 0 + MS * 1501,
    };
    let res = read_expanded_for_range(range)?;
    if res.0 != 3 {
        Err(Error::with_msg(format!("unexpected number of events: {}", res.0)))?;
    }
    Ok(())
}

#[test]
fn read_expanded_3() -> Result<(), Error> {
    use netpod::timeunits::*;
    let range = netpod::NanoRange {
        beg: DAY - MS * 1500,
        end: DAY + MS * 0 + MS * 1501,
    };
    let res = read_expanded_for_range(range)?;
    if res.0 != 4 {
        Err(Error::with_msg(format!("unexpected number of events: {}", res.0)))?;
    }
    Ok(())
}
