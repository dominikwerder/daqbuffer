use crate::agg::streams::StreamItem;
use crate::dataopen::{open_files, OpenedFile};
use crate::eventchunker::{EventChunker, EventChunkerConf};
use crate::file_content_stream;
use crate::streamlog::LogItem;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{ChannelConfig, NanoRange, Node};
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct EventBlobsComplete {
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
}

impl EventBlobsComplete {
    pub fn new(
        range: NanoRange,
        channel_config: ChannelConfig,
        node: Node,
        node_ix: usize,
        buffer_size: usize,
        event_chunker_conf: EventChunkerConf,
    ) -> Self {
        Self {
            file_chan: open_files(&range, &channel_config, node),
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
        }
    }
}

impl Stream for EventBlobsComplete {
    type Item = Result<StreamItem<EventChunkerItem>, Error>;

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
