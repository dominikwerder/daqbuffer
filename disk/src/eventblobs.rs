use crate::eventchunker::{EventChunker, EventFull};
use crate::{file_content_stream, open_files};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::{ChannelConfig, NanoRange, Node};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::fs::File;

pub struct EventBlobsComplete {
    channel_config: ChannelConfig,
    file_chan: async_channel::Receiver<Result<File, Error>>,
    evs: Option<EventChunker>,
    buffer_size: u32,
    range: NanoRange,
}

impl EventBlobsComplete {
    pub fn new(
        query: &netpod::AggQuerySingleChannel,
        channel_config: ChannelConfig,
        range: NanoRange,
        node: Arc<Node>,
    ) -> Self {
        Self {
            file_chan: open_files(query, node),
            evs: None,
            buffer_size: query.buffer_size,
            channel_config,
            range,
        }
    }
}

impl Stream for EventBlobsComplete {
    type Item = Result<EventFull, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
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
                            let chunker = EventChunker::new(inp, self.channel_config.clone(), self.range.clone());
                            self.evs = Some(chunker);
                            continue 'outer;
                        }
                        Err(e) => Ready(Some(Err(e))),
                    },
                    Ready(None) => Ready(None),
                    Pending => Pending,
                },
            };
            break z;
        }
    }
}

pub fn event_blobs_complete(
    query: &netpod::AggQuerySingleChannel,
    node: Arc<Node>,
) -> impl Stream<Item = Result<EventFull, Error>> + Send {
    let query = query.clone();
    let node = node.clone();
    async_stream::stream! {
        let filerx = open_files(&query, node.clone());
        while let Ok(fileres) = filerx.recv().await {
            match fileres {
                Ok(file) => {
                    let inp = Box::pin(file_content_stream(file, query.buffer_size as usize));
                    let mut chunker = EventChunker::new(inp, err::todoval(), err::todoval());
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
