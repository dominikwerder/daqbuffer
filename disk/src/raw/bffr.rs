use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::FrameType;
use crate::raw::conn::RawConnOut;
use err::Error;
use futures_core::Stream;
use futures_util::pin_mut;
#[allow(unused_imports)]
use netpod::log::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

pub struct MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    inp: InMemoryFrameAsyncReadStream<T>,
}

impl<T> MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    pub fn new(inp: InMemoryFrameAsyncReadStream<T>) -> Self {
        Self { inp }
    }
}

impl<T> Stream for MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<MinMaxAvgScalarEventBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let j = &mut self.inp;
            pin_mut!(j);
            break match j.poll_next(cx) {
                Ready(Some(Ok(frame))) => {
                    type ExpectedType = RawConnOut;
                    trace!(
                        "MinMaxAvgScalarEventBatchStreamFromFrames  got full frame buf  {}",
                        frame.buf().len()
                    );
                    assert!(frame.tyid() == <ExpectedType as FrameType>::FRAME_TYPE_ID);
                    match bincode::deserialize::<ExpectedType>(frame.buf()) {
                        Ok(item) => match item {
                            Ok(item) => Ready(Some(Ok(item))),
                            Err(e) => Ready(Some(Err(e))),
                        },
                        Err(e) => {
                            error!(
                                "MinMaxAvgScalarEventBatchStreamFromFrames  ~~~~~~~~   ERROR on frame payload {}",
                                frame.buf().len(),
                            );
                            Ready(Some(Err(e.into())))
                        }
                    }
                }
                Ready(Some(Err(e))) => Ready(Some(Err(e))),
                Ready(None) => Ready(None),
                Pending => Pending,
            };
        }
    }
}
