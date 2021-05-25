use crate::agg::streams::StreamItem;
use crate::binned::{BinnedStreamKind, RangeCompletableItem};
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::decode_frame;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

pub struct EventsFromFrames<T, SK>
where
    T: AsyncRead + Unpin,
    SK: BinnedStreamKind,
{
    inp: InMemoryFrameAsyncReadStream<T>,
    errored: bool,
    completed: bool,
    stream_kind: SK,
}

impl<T, SK> EventsFromFrames<T, SK>
where
    T: AsyncRead + Unpin,
    SK: BinnedStreamKind,
{
    pub fn new(inp: InMemoryFrameAsyncReadStream<T>, stream_kind: SK) -> Self {
        Self {
            inp,
            errored: false,
            completed: false,
            stream_kind,
        }
    }
}

impl<T, SK> Stream for EventsFromFrames<T, SK>
where
    T: AsyncRead + Unpin,
    SK: BinnedStreamKind,
{
    type Item = Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::XBinnedEvents>>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.completed {
                panic!("EventsFromFrames  poll_next on completed");
            } else if self.errored {
                self.completed = true;
                Ready(None)
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(Ok(item))) => match item {
                        StreamItem::Log(item) => Ready(Some(Ok(StreamItem::Log(item)))),
                        StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                        StreamItem::DataItem(frame) => {
                            match decode_frame::<<SK as BinnedStreamKind>::XBinnedEvents>(&frame) {
                                Ok(item) => match item {
                                    Ok(item) => Ready(Some(Ok(item))),
                                    Err(e) => {
                                        self.errored = true;
                                        Ready(Some(Err(e)))
                                    }
                                },
                                Err(e) => {
                                    error!(
                                        "EventsFromFrames  ~~~~~~~~   ERROR on frame payload {}",
                                        frame.buf().len(),
                                    );
                                    self.errored = true;
                                    Ready(Some(Err(e)))
                                }
                            }
                        }
                    },
                    Ready(Some(Err(e))) => {
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Ready(None) => {
                        self.completed = true;
                        Ready(None)
                    }
                    Pending => Pending,
                }
            };
        }
    }
}
