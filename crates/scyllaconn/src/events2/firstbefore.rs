use daqbuf_err as err;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::Events;
use items_2::merger::Mergeable;
use netpod::log::*;
use netpod::stream_impl_tracer::StreamImplTracer;
use netpod::TsNano;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[allow(unused)]
macro_rules! trace_transition {
    ($($arg:tt)*) => {
        if true {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace_emit {
    ($($arg:tt)*) => {
        if true {
            trace!($($arg)*);
        }
    };
}

macro_rules! tracer_poll_enter {
    ($self:expr) => {
        if false && $self.tracer.poll_enter() {
            return Ready(Some(Err(Error::LimitPoll)));
        }
    };
}

macro_rules! tracer_loop_enter {
    ($self:expr) => {
        if false && $self.tracer.loop_enter() {
            return Ready(Some(Err(Error::LimitLoop)));
        }
    };
}

#[derive(Debug, ThisError)]
#[cstm(name = "EventsFirstBefore")]
pub enum Error {
    Unordered,
    Logic,
    Input(Box<dyn std::error::Error + Send>),
    LimitPoll,
    LimitLoop,
}

pub enum Output<T> {
    First(T, T),
    Bulk(T),
}

enum State {
    Begin,
    Bulk,
    Done,
}

pub struct FirstBeforeAndInside<S, T>
where
    S: Stream + Unpin,
    T: Events + Mergeable + Unpin,
{
    ts0: TsNano,
    inp: S,
    state: State,
    buf: Option<T>,
    tracer: StreamImplTracer,
}

impl<S, T> FirstBeforeAndInside<S, T>
where
    S: Stream + Unpin,
    T: Events + Mergeable + Unpin,
{
    pub fn new(inp: S, ts0: TsNano) -> Self {
        trace_transition!("FirstBeforeAndInside::new");
        Self {
            ts0,
            inp,
            state: State::Begin,
            buf: None,
            tracer: StreamImplTracer::new("FirstBeforeAndInside".into(), 2000, 100),
        }
    }
}

impl<S, T, E> Stream for FirstBeforeAndInside<S, T>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    T: Events + Mergeable + Unpin,
    E: std::error::Error + Send + 'static,
{
    type Item = Result<Output<T>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        tracer_poll_enter!(self);
        loop {
            tracer_loop_enter!(self);
            break match &self.state {
                State::Begin => match self.inp.poll_next_unpin(cx) {
                    Ready(Some(Ok(mut item))) => {
                        // It is an invariant that we process ordered streams, but for robustness
                        // verify the batch item again:
                        if item.verify() != true {
                            self.state = State::Done;
                            let e = Error::Unordered;
                            Ready(Some(Err(e)))
                        } else {
                            // Separate events into before and bulk
                            let tss = Events::tss(&item);
                            let pp = tss.partition_point(|&x| x < self.ts0.ns());
                            trace_transition!("partition_point  {pp:?}  {n:?}", n = tss.len());
                            if pp > item.len() {
                                error!("bad partition point  {}  {}", pp, item.len());
                                self.state = State::Done;
                                Ready(Some(Err(Error::Logic)))
                            } else if pp == item.len() {
                                // all entries are before, or empty item
                                if self.buf.is_none() {
                                    self.buf = Some(item.new_empty());
                                }
                                match item.drain_into_evs(self.buf.as_mut().unwrap(), (0, item.len())) {
                                    Ok(()) => {
                                        continue;
                                    }
                                    Err(e) => {
                                        self.state = State::Done;
                                        Ready(Some(Err(Error::Input(Box::new(e)))))
                                    }
                                }
                            } else if pp == 0 {
                                // all entries are bulk
                                trace_transition!("transition immediately to bulk");
                                self.state = State::Bulk;
                                let o1 = core::mem::replace(&mut self.buf, Some(item.new_empty()))
                                    .unwrap_or_else(|| item.new_empty());
                                Ready(Some(Ok(Output::First(o1, item))))
                            } else {
                                // mixed
                                if self.buf.is_none() {
                                    self.buf = Some(item.new_empty());
                                }
                                match item.drain_into_evs(self.buf.as_mut().unwrap(), (0, pp)) {
                                    Ok(()) => {
                                        trace_transition!("transition with mixed to bulk");
                                        self.state = State::Bulk;
                                        let o1 = core::mem::replace(&mut self.buf, Some(item.new_empty()))
                                            .unwrap_or_else(|| item.new_empty());
                                        Ready(Some(Ok(Output::First(o1, item))))
                                    }
                                    Err(e) => {
                                        self.state = State::Done;
                                        Ready(Some(Err(Error::Input(Box::new(e)))))
                                    }
                                }
                            }
                        }
                    }
                    Ready(Some(Err(e))) => {
                        self.state = State::Done;
                        Ready(Some(Err(Error::Input(Box::new(e)))))
                    }
                    Ready(None) => {
                        self.state = State::Done;
                        if let Some(x) = self.buf.take() {
                            let empty = x.new_empty();
                            Ready(Some(Ok(Output::First(x, empty))))
                        } else {
                            Ready(None)
                        }
                    }
                    Pending => Pending,
                },
                State::Bulk => {
                    if self.buf.as_ref().map_or(0, |x| x.len()) != 0 {
                        error!(
                            "State::Bulk  but buf non-empty  {}",
                            self.buf.as_ref().map_or(0, |x| x.len())
                        );
                        self.state = State::Done;
                        Ready(Some(Err(Error::Logic)))
                    } else {
                        match self.inp.poll_next_unpin(cx) {
                            Ready(Some(Ok(item))) => {
                                if item.verify() != true {
                                    self.state = State::Done;
                                    let e = Error::Unordered;
                                    Ready(Some(Err(e)))
                                } else {
                                    trace_emit!("output bulk item  len {}", item.len());
                                    Ready(Some(Ok(Output::Bulk(item))))
                                }
                            }
                            Ready(Some(Err(e))) => {
                                self.state = State::Done;
                                Ready(Some(Err(Error::Input(Box::new(e)))))
                            }
                            Ready(None) => {
                                trace_emit!("in bulk, input done");
                                self.state = State::Done;
                                Ready(None)
                            }
                            Pending => Pending,
                        }
                    }
                }
                State::Done => Ready(None),
            };
        }
    }
}

fn trait_assert<T>(_: T)
where
    T: Stream + Unpin + Send,
{
}

#[allow(unused)]
fn trait_assert_try() {
    let x: FirstBeforeAndInside<super::events::EventsStreamRt, items_2::channelevents::ChannelEvents> = phantomval();
    trait_assert(x);
}

fn phantomval<T>() -> T {
    panic!()
}
