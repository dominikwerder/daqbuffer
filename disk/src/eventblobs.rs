use crate::dataopen::open_files;
use crate::eventchunker::{EventChunker, EventChunkerConf, EventChunkerItem};
use crate::file_content_stream;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::{ChannelConfig, NanoRange, Node};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;

pub struct EventBlobsComplete {
    channel_config: ChannelConfig,
    file_chan: async_channel::Receiver<Result<File, Error>>,
    evs: Option<EventChunker>,
    buffer_size: usize,
    event_chunker_conf: EventChunkerConf,
    range: NanoRange,
    errored: bool,
    completed: bool,
}

impl EventBlobsComplete {
    pub fn new(
        range: NanoRange,
        channel_config: ChannelConfig,
        node: Node,
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
            errored: false,
            completed: false,
        }
    }
}

impl Stream for EventBlobsComplete {
    type Item = Result<EventChunkerItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("EventBlobsComplete  poll_next on completed");
            } else if self.errored {
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
                                let inp = Box::pin(file_content_stream(file, self.buffer_size as usize));
                                let chunker = EventChunker::from_event_boundary(
                                    inp,
                                    self.channel_config.clone(),
                                    self.range.clone(),
                                    self.event_chunker_conf.clone(),
                                );
                                self.evs = Some(chunker);
                                continue 'outer;
                            }
                            Err(e) => {
                                self.errored = true;
                                Ready(Some(Err(e)))
                            }
                        },
                        Ready(None) => {
                            self.completed = true;
                            Ready(None)
                        }
                        Pending => Pending,
                    },
                }
            };
        }
    }
}
