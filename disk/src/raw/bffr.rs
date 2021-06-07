use crate::agg::streams::StreamItem;
use crate::binned::{RangeCompletableItem, StreamKind, XBinnedEvents};
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::{decode_frame, FrameType};
use crate::Sitemty;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

// TODO remove usage of SK, no longer needed.
pub struct EventsFromFrames<T, I>
where
    T: AsyncRead + Unpin,
{
    inp: InMemoryFrameAsyncReadStream<T>,
    errored: bool,
    completed: bool,
    _m2: PhantomData<I>,
}

impl<T, I> EventsFromFrames<T, I>
where
    T: AsyncRead + Unpin,
{
    pub fn new(inp: InMemoryFrameAsyncReadStream<T>) -> Self {
        Self {
            inp,
            errored: false,
            completed: false,
            _m2: PhantomData,
        }
    }
}

impl<T, I> Stream for EventsFromFrames<T, I>
where
    T: AsyncRead + Unpin,
    //SK: StreamKind,
    I: DeserializeOwned + Unpin,
    // TODO see binned.rs better to express it on trait?
    //Result<StreamItem<RangeCompletableItem<<SK as BinnedStreamKind>::XBinnedEvents>>, Error>: FrameType,
    Sitemty<I>: FrameType,
{
    type Item = Sitemty<I>;

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
                        StreamItem::DataItem(frame) => match decode_frame::<Sitemty<I>>(&frame, 0) {
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
                        },
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
