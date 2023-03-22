use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::framable::FrameTypeInnerStatic;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::frame::decode_frame;
use items_2::inmem::InMemoryFrame;
use netpod::log::*;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct EventsFromFrames<O> {
    inp: Pin<Box<dyn Stream<Item = Result<StreamItem<InMemoryFrame>, Error>> + Send>>,
    dbgdesc: String,
    errored: bool,
    completed: bool,
    _m1: PhantomData<O>,
}

impl<O> EventsFromFrames<O> {
    pub fn new(
        inp: Pin<Box<dyn Stream<Item = Result<StreamItem<InMemoryFrame>, Error>> + Send>>,
        dbgdesc: String,
    ) -> Self {
        Self {
            inp,
            dbgdesc,
            errored: false,
            completed: false,
            _m1: PhantomData,
        }
    }
}

impl<O> Stream for EventsFromFrames<O>
where
    O: FrameTypeInnerStatic + DeserializeOwned + Unpin,
{
    type Item = Sitemty<O>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let span = span!(Level::INFO, "EvFrFr", id = tracing::field::Empty);
        span.record("id", &self.dbgdesc);
        let _spg = span.enter();
        loop {
            break if self.completed {
                panic!("poll_next on completed");
            } else if self.errored {
                self.completed = true;
                Ready(None)
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(Ok(item))) => match item {
                        StreamItem::Log(item) => {
                            info!("{}  {:?}  {}", item.node_ix, item.level, item.msg);
                            Ready(Some(Ok(StreamItem::Log(item))))
                        }
                        StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                        StreamItem::DataItem(frame) => match decode_frame::<Sitemty<O>>(&frame) {
                            Ok(item) => match item {
                                Ok(item) => match item {
                                    StreamItem::Log(k) => {
                                        info!("rcvd log: {}  {:?}  {}", k.node_ix, k.level, k.msg);
                                        Ready(Some(Ok(StreamItem::Log(k))))
                                    }
                                    item => Ready(Some(Ok(item))),
                                },
                                Err(e) => {
                                    error!("rcvd err: {}", e);
                                    self.errored = true;
                                    Ready(Some(Err(e)))
                                }
                            },
                            Err(e) => {
                                error!("frame payload  len {}  tyid {}  {}", frame.buf().len(), frame.tyid(), e);
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
