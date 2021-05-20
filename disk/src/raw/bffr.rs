use crate::agg::eventbatch::MinMaxAvgScalarEventBatchStreamItem;
use crate::agg::streams::StreamItem;
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::decode_frame;
use crate::raw::conn::RawConnOut;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

pub struct MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    inp: InMemoryFrameAsyncReadStream<T>,
    errored: bool,
    completed: bool,
}

impl<T> MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    pub fn new(inp: InMemoryFrameAsyncReadStream<T>) -> Self {
        Self {
            inp,
            errored: false,
            completed: false,
        }
    }
}

impl<T> Stream for MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<StreamItem<MinMaxAvgScalarEventBatchStreamItem>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.completed {
                panic!("MinMaxAvgScalarEventBatchStreamFromFrames  poll_next on completed");
            } else if self.errored {
                self.completed = true;
                Ready(None)
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(Ok(item))) => match item {
                        StreamItem::Log(item) => Ready(Some(Ok(StreamItem::Log(item)))),
                        StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                        StreamItem::DataItem(frame) => {
                            type ExpectedType = RawConnOut;
                            match decode_frame::<ExpectedType>(&frame) {
                                Ok(item) => match item {
                                    Ok(item) => Ready(Some(Ok(item))),
                                    Err(e) => {
                                        self.errored = true;
                                        Ready(Some(Err(e)))
                                    }
                                },
                                Err(e) => {
                                    error!(
                                        "MinMaxAvgScalarEventBatchStreamFromFrames  ~~~~~~~~   ERROR on frame payload {}",
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
