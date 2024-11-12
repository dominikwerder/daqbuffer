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
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! trace_transition { ($($arg:tt)*) => ( if true { trace!($($arg)*); } ) }

macro_rules! trace_emit { ($($arg:tt)*) => ( if true { trace!($($arg)*); } ) }

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

#[allow(unused)]
macro_rules! debug_fetch { ($($arg:tt)*) => ( if true { debug!($($arg)*); } ) }

#[derive(Debug, ThisError)]
#[cstm(name = "EventsOneBeforeAndBulk")]
pub enum Error {
    Unordered,
    Logic,
    Input(Box<dyn std::error::Error + Send>),
    LimitPoll,
    LimitLoop,
}

#[derive(Debug)]
pub enum Output<T> {
    Before(T),
    Bulk(T),
}

enum State {
    Begin,
    Bulk,
    Done,
}

pub struct OneBeforeAndBulk<S, T>
where
    S: Stream + Unpin,
    T: Mergeable + Unpin,
{
    ts0: TsNano,
    inp: S,
    state: State,
    buf: Option<T>,
    out: VecDeque<T>,
    tracer: StreamImplTracer,
    seen_empty_during_begin: bool,
    seen_empty_during_bulk: bool,
    dbgname: String,
    tslast: TsNano,
}

impl<S, T> OneBeforeAndBulk<S, T>
where
    S: Stream + Unpin,
    T: Mergeable + Unpin,
{
    fn selfname() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(inp: S, ts0: TsNano, dbgname: String) -> Self {
        trace_transition!("{}::new", Self::selfname());
        Self {
            ts0,
            inp,
            state: State::Begin,
            buf: None,
            out: VecDeque::new(),
            tracer: StreamImplTracer::new(Self::selfname().into(), 2000, 100),
            seen_empty_during_begin: false,
            seen_empty_during_bulk: false,
            dbgname,
            tslast: TsNano::from_ns(0),
        }
    }

    fn consume_buf_get_latest(&mut self) -> Option<T> {
        if let Some(mut buf) = self.buf.take() {
            if buf.len() == 0 {
                debug!("buf set but empty");
                None
            } else {
                let mut ret = buf.new_empty();
                buf.drain_into(&mut ret, (buf.len() - 1, buf.len()));
                Some(ret)
            }
        } else {
            None
        }
    }
}

impl<S, T, E> Stream for OneBeforeAndBulk<S, T>
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
            break if let Some(item) = self.out.pop_front() {
                Ready(Some(Ok(Output::Bulk(item))))
            } else {
                match &self.state {
                    State::Begin => match self.inp.poll_next_unpin(cx) {
                        Ready(Some(Ok(mut item))) => {
                            if let Some(tsmin) = Mergeable::ts_min(&item) {
                                let tsmin = TsNano::from_ns(tsmin);
                                if tsmin < self.tslast {
                                    self.state = State::Done;
                                    let e = Error::Unordered;
                                    break Ready(Some(Err(e)));
                                } else {
                                    self.tslast = TsNano::from_ns(Mergeable::ts_max(&item).unwrap());
                                }
                            }
                            if item.verify() != true {
                                self.state = State::Done;
                                let e = Error::Unordered;
                                Ready(Some(Err(e)))
                            } else {
                                if item.len() == 0 {
                                    self.seen_empty_during_begin = true;
                                } else {
                                    if self.seen_empty_during_begin {
                                        debug_fetch!(
                                            "still in Begin  current event len {}  but seen empty before",
                                            item.len()
                                        );
                                    }
                                }
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
                                    trace_transition!("stay in Begin");
                                    trace_emit!(
                                        "State::Begin  Before  {}  all content still before  len {}",
                                        self.dbgname,
                                        item.len()
                                    );
                                    let buf = self.buf.get_or_insert_with(|| item.new_empty());
                                    match item.drain_into_evs(buf, (0, item.len())) {
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
                                    trace_transition!("transition with bulk to Bulk");
                                    self.state = State::Bulk;
                                    if let Some(before) = self.consume_buf_get_latest() {
                                        self.out.push_back(item);
                                        let item = Output::Before(before);
                                        trace_emit!("State::Begin  Before  {}  emit {:?}", self.dbgname, item);
                                        Ready(Some(Ok(item)))
                                    } else {
                                        let item = Output::Bulk(item);
                                        trace_emit!("State::Begin  Bulk    {}  emit {:?}", self.dbgname, item);
                                        Ready(Some(Ok(item)))
                                    }
                                } else {
                                    // mixed
                                    trace_transition!("transition with mixed to Bulk");
                                    self.state = State::Bulk;
                                    let buf = self.buf.get_or_insert_with(|| item.new_empty());
                                    match item.drain_into_evs(buf, (0, pp)) {
                                        Ok(()) => {
                                            if let Some(before) = self.consume_buf_get_latest() {
                                                self.out.push_back(item);
                                                let item = Output::Before(before);
                                                trace_emit!("State::Begin  Before  {}  emit {:?}", self.dbgname, item);
                                                Ready(Some(Ok(item)))
                                            } else {
                                                let item = Output::Bulk(item);
                                                trace_emit!("State::Begin  Bulk    {}  emit {:?}", self.dbgname, item);
                                                Ready(Some(Ok(item)))
                                            }
                                        }
                                        Err(e) => {
                                            self.state = State::Done;
                                            let e = Error::Input(Box::new(e));
                                            Ready(Some(Err(e)))
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
                            trace_transition!("transition from Begin to end of stream");
                            if let Some(before) = self.consume_buf_get_latest() {
                                let item = Output::Before(before);
                                trace_emit!("State::Begin  EOS  {}  emit {:?}", self.dbgname, item);
                                Ready(Some(Ok(item)))
                            } else {
                                trace_emit!("State::Begin  EOS  {}  emit None", self.dbgname);
                                Ready(None)
                            }
                        }
                        Pending => Pending,
                    },
                    State::Bulk => {
                        if self.buf.is_some() {
                            let n = self.buf.as_ref().map_or(0, |x| x.len());
                            error!("State::Bulk  but buf non-empty  {}", n);
                            self.state = State::Done;
                            Ready(Some(Err(Error::Logic)))
                        } else {
                            match self.inp.poll_next_unpin(cx) {
                                Ready(Some(Ok(item))) => {
                                    if let Some(tsmin) = Mergeable::ts_min(&item) {
                                        let tsmin = TsNano::from_ns(tsmin);
                                        if tsmin < self.tslast {
                                            self.state = State::Done;
                                            let e = Error::Unordered;
                                            break Ready(Some(Err(e)));
                                        } else {
                                            self.tslast = TsNano::from_ns(Mergeable::ts_max(&item).unwrap());
                                        }
                                    }
                                    if item.verify() != true {
                                        self.state = State::Done;
                                        let e = Error::Unordered;
                                        Ready(Some(Err(e)))
                                    } else {
                                        if item.len() == 0 {
                                            self.seen_empty_during_bulk = true;
                                        }
                                        let item = Output::Bulk(item);
                                        trace_emit!("State::Bulk  data  {}  emit {:?}", self.dbgname, item);
                                        Ready(Some(Ok(item)))
                                    }
                                }
                                Ready(Some(Err(e))) => {
                                    self.state = State::Done;
                                    Ready(Some(Err(Error::Input(Box::new(e)))))
                                }
                                Ready(None) => {
                                    trace_emit!("in bulk, input done");
                                    self.state = State::Done;
                                    trace_emit!("State::Bulk  EOS  {}  emit None", self.dbgname);
                                    Ready(None)
                                }
                                Pending => Pending,
                            }
                        }
                    }
                    State::Done => Ready(None),
                }
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
    let x: OneBeforeAndBulk<super::events::EventsStreamRt, items_2::channelevents::ChannelEvents> = phantomval();
    trait_assert(x);
}

fn phantomval<T>() -> T {
    panic!()
}
