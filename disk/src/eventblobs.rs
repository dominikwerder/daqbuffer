use crate::dataopen::open_files;
use crate::eventchunker::{EventChunker, EventFull};
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
    range: NanoRange,
    errored: bool,
    completed: bool,
}

impl EventBlobsComplete {
    pub fn new(range: NanoRange, channel_config: ChannelConfig, node: Node, buffer_size: usize) -> Self {
        Self {
            file_chan: open_files(&range, &channel_config, node),
            evs: None,
            buffer_size,
            channel_config,
            range,
            errored: false,
            completed: false,
        }
    }
}

impl Stream for EventBlobsComplete {
    type Item = Result<EventFull, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("EventBlobsComplete  poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        'outer: loop {
            let z = match &mut self.evs {
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
                            let chunker =
                                EventChunker::from_event_boundary(inp, self.channel_config.clone(), self.range.clone());
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
            };
            break z;
        }
    }
}

pub fn event_blobs_complete(
    query: &netpod::AggQuerySingleChannel,
    node: Node,
) -> impl Stream<Item = Result<EventFull, Error>> + Send {
    let query = query.clone();
    let node = node.clone();
    async_stream::stream! {
        let filerx = open_files(err::todoval(), err::todoval(), node);
        while let Ok(fileres) = filerx.recv().await {
            match fileres {
                Ok(file) => {
                    let inp = Box::pin(file_content_stream(file, query.buffer_size as usize));
                    let mut chunker = EventChunker::from_event_boundary(inp, err::todoval(), err::todoval());
                    while let Some(evres) = chunker.next().await {
                        match evres {
                            Ok(evres) => {
                                yield Ok(evres);
                            }
                            Err(e) => {
                                yield Err(e)
                            }
                        }
                    }
                }
                Err(e) => {
                    yield Err(e);
                }
            }
        }
    }
}
