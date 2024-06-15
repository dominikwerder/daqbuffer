use super::events::EventsStreamRt;
use super::nonempty::NonEmpty;
use crate::range::ScyllaSeriesRange;
use crate::worker::ScyllaQueue;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_2::channelevents::ChannelEvents;
use items_2::merger::Mergeable;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
use netpod::ttl::RetentionTime;
use netpod::ScalarType;
use netpod::Shape;
use series::SeriesId;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug, ThisError)]
pub enum Error {
    Events(#[from] crate::events2::events::Error),
    Logic,
}

enum Resolvable<F>
where
    F: Future,
{
    Future(F),
    Output(<F as Future>::Output),
    Taken,
}

impl<F> Resolvable<F>
where
    F: Future,
{
    fn unresolved(&self) -> bool {
        match self {
            Resolvable::Future(_) => true,
            Resolvable::Output(_) => false,
            Resolvable::Taken => false,
        }
    }

    fn take(&mut self) -> Option<<F as Future>::Output> {
        let x = std::mem::replace(self, Resolvable::Taken);
        match x {
            Resolvable::Future(_) => None,
            Resolvable::Output(x) => Some(x),
            Resolvable::Taken => None,
        }
    }
}

impl<F> Future for Resolvable<F>
where
    F: Future + Unpin,
{
    type Output = <F as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<<F as Future>::Output> {
        match unsafe { self.get_unchecked_mut() } {
            Resolvable::Future(fut) => fut.poll_unpin(cx),
            Resolvable::Output(_) => panic!(),
            Resolvable::Taken => panic!(),
        }
    }
}

struct ReadEvents {
    fut: Pin<Box<dyn Future<Output = Option<Result<ChannelEvents, crate::events2::events::Error>>> + Send>>,
}

enum State {
    Begin,
    FetchFirstSt(ReadEvents),
    FetchFirstMt(ReadEvents),
    FetchFirstLt(ReadEvents),
    ReadingLt(
        Option<ReadEvents>,
        VecDeque<ChannelEvents>,
        Option<Box<NonEmpty<EventsStreamRt>>>,
    ),
    ReadingMt(
        Option<ReadEvents>,
        VecDeque<ChannelEvents>,
        Option<Box<NonEmpty<EventsStreamRt>>>,
    ),
    ReadingSt(
        Option<ReadEvents>,
        VecDeque<ChannelEvents>,
        Option<Box<NonEmpty<EventsStreamRt>>>,
    ),
    Error,
}

pub struct MergeRts {
    series: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    range: ScyllaSeriesRange,
    with_values: bool,
    scyqueue: ScyllaQueue,
    inp_st: Option<Box<NonEmpty<EventsStreamRt>>>,
    inp_mt: Option<Box<NonEmpty<EventsStreamRt>>>,
    inp_lt: Option<Box<NonEmpty<EventsStreamRt>>>,
    state: State,
    buf_st: VecDeque<ChannelEvents>,
    buf_mt: VecDeque<ChannelEvents>,
    buf_lt: VecDeque<ChannelEvents>,
    out: VecDeque<ChannelEvents>,
}

impl MergeRts {
    pub fn new(
        series: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
        range: ScyllaSeriesRange,
        with_values: bool,
        scyqueue: ScyllaQueue,
    ) -> Self {
        Self {
            series,
            scalar_type,
            shape,
            range,
            with_values,
            scyqueue,
            inp_st: None,
            inp_mt: None,
            inp_lt: None,
            state: State::Begin,
            buf_st: VecDeque::new(),
            buf_mt: VecDeque::new(),
            buf_lt: VecDeque::new(),
            out: VecDeque::new(),
        }
    }

    fn setup_first_st(&mut self) {
        let inp = EventsStreamRt::new(
            RetentionTime::Short,
            self.series.clone(),
            self.scalar_type.clone(),
            self.shape.clone(),
            self.range.clone(),
            self.with_values,
            self.scyqueue.clone(),
        );
        let inp = NonEmpty::new(inp);
        self.inp_st = Some(Box::new(inp));
    }

    fn setup_first_mt(&mut self) {
        let inp = EventsStreamRt::new(
            RetentionTime::Medium,
            self.series.clone(),
            self.scalar_type.clone(),
            self.shape.clone(),
            Self::constrained_range(&self.range, &self.buf_st),
            self.with_values,
            self.scyqueue.clone(),
        );
        let inp = NonEmpty::new(inp);
        self.inp_mt = Some(Box::new(inp));
    }

    fn setup_first_lt(&mut self) {
        let inp = EventsStreamRt::new(
            RetentionTime::Long,
            self.series.clone(),
            self.scalar_type.clone(),
            self.shape.clone(),
            Self::constrained_range(&self.range, &self.buf_mt),
            self.with_values,
            self.scyqueue.clone(),
        );
        let inp = NonEmpty::new(inp);
        self.inp_lt = Some(Box::new(inp));
    }

    fn setup_read_st(&mut self) -> ReadEvents {
        let stream = unsafe { &mut *(self.inp_st.as_mut().unwrap().as_mut() as *mut NonEmpty<EventsStreamRt>) };
        let fut = Box::pin(stream.next());
        ReadEvents { fut }
    }

    fn setup_read_mt(&mut self) -> ReadEvents {
        let stream = unsafe { &mut *(self.inp_mt.as_mut().unwrap().as_mut() as *mut NonEmpty<EventsStreamRt>) };
        let fut = Box::pin(stream.next());
        ReadEvents { fut }
    }

    fn setup_read_lt(&mut self) -> ReadEvents {
        let stream = unsafe { &mut *(self.inp_lt.as_mut().unwrap().as_mut() as *mut NonEmpty<EventsStreamRt>) };
        let fut = Box::pin(stream.next());
        ReadEvents { fut }
    }

    fn setup_read_any(inp: &mut Option<Box<NonEmpty<EventsStreamRt>>>) -> ReadEvents {
        let stream = unsafe { &mut *(inp.as_mut().unwrap().as_mut() as *mut NonEmpty<EventsStreamRt>) };
        let fut = Box::pin(stream.next());
        ReadEvents { fut }
    }

    fn constrained_range(full: &ScyllaSeriesRange, buf: &VecDeque<ChannelEvents>) -> ScyllaSeriesRange {
        if let Some(e) = buf.front() {
            if let Some(ts) = e.ts_min() {
                let nrange = NanoRange::from((ts, 0));
                ScyllaSeriesRange::from(&SeriesRange::from(nrange))
            } else {
                debug!("no ts even though should not have empty buffers");
                full.clone()
            }
        } else {
            full.clone()
        }
    }

    fn dummy(&mut self) -> bool {
        if self.inp_lt.is_some() {
            // *fut = Some(self.setup_read_lt());
            true
        } else {
            false
        }
    }
}

impl Stream for MergeRts {
    type Item = Result<ChannelEvents, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let mut out2 = VecDeque::new();
        loop {
            while let Some(x) = out2.pop_front() {
                self.out.push_back(x);
            }
            if let Some(item) = self.out.pop_front() {
                break Ready(Some(Ok(item)));
            }
            break match &mut self.state {
                State::Begin => {
                    self.setup_first_st();
                    self.state = State::FetchFirstSt(self.setup_read_st());
                    continue;
                }
                State::FetchFirstSt(st2) => match st2.fut.poll_unpin(cx) {
                    Ready(Some(Ok(x))) => {
                        debug!("have first from ST");
                        self.buf_st.push_back(x);
                        self.setup_first_mt();
                        self.state = State::FetchFirstMt(self.setup_read_mt());
                        continue;
                    }
                    Ready(Some(Err(e))) => {
                        self.state = State::Error;
                        Ready(Some(Err(e.into())))
                    }
                    Ready(None) => {
                        debug!("no first from ST");
                        self.inp_st = None;
                        self.setup_first_mt();
                        self.state = State::FetchFirstMt(self.setup_read_mt());
                        continue;
                    }
                    Pending => Pending,
                },
                State::FetchFirstMt(st2) => match st2.fut.poll_unpin(cx) {
                    Ready(Some(Ok(x))) => {
                        debug!("have first from MT");
                        self.buf_mt.push_back(x);
                        self.setup_first_lt();
                        self.state = State::FetchFirstLt(self.setup_read_lt());
                        continue;
                    }
                    Ready(Some(Err(e))) => {
                        self.state = State::Error;
                        Ready(Some(Err(e.into())))
                    }
                    Ready(None) => {
                        debug!("no first from MT");
                        self.inp_mt = None;
                        self.setup_first_lt();
                        self.state = State::FetchFirstLt(self.setup_read_lt());
                        continue;
                    }
                    Pending => Pending,
                },
                State::FetchFirstLt(st2) => match st2.fut.poll_unpin(cx) {
                    Ready(Some(Ok(x))) => {
                        debug!("have first from LT");
                        self.buf_lt.push_back(x);
                        let buf = core::mem::replace(&mut self.buf_lt, VecDeque::new());
                        self.state = State::ReadingLt(None, buf, self.inp_lt.take());
                        continue;
                    }
                    Ready(Some(Err(e))) => {
                        self.state = State::Error;
                        Ready(Some(Err(e.into())))
                    }
                    Ready(None) => {
                        debug!("no first from LT");
                        self.inp_lt = None;
                        let buf = core::mem::replace(&mut self.buf_lt, VecDeque::new());
                        self.state = State::ReadingLt(None, buf, self.inp_lt.take());
                        continue;
                    }
                    Pending => Pending,
                },
                State::ReadingLt(fut, buf, inp) => {
                    if let Some(x) = buf.pop_front() {
                        out2.push_back(x);
                        continue;
                    } else if let Some(fut2) = fut {
                        match fut2.fut.poll_unpin(cx) {
                            Ready(Some(Ok(x))) => {
                                *fut = None;
                                buf.push_back(x);
                                continue;
                            }
                            Ready(Some(Err(e))) => {
                                *fut = None;
                                self.state = State::Error;
                                Ready(Some(Err(e.into())))
                            }
                            Ready(None) => {
                                *fut = None;
                                self.inp_lt = None;
                                continue;
                            }
                            Pending => Pending,
                        }
                    } else if inp.is_some() {
                        let buf = core::mem::replace(buf, VecDeque::new());
                        self.state = State::ReadingLt(Some(Self::setup_read_any(inp)), buf, inp.take());
                        // *fut = Some(self.setup_read_lt());
                        continue;
                    } else {
                        let buf = core::mem::replace(&mut self.buf_mt, VecDeque::new());
                        self.state = State::ReadingMt(None, buf, self.inp_mt.take());
                        continue;
                    }
                }
                State::ReadingMt(fut, buf, inp) => {
                    if let Some(x) = buf.pop_front() {
                        out2.push_back(x);
                        continue;
                    } else if let Some(fut2) = fut {
                        match fut2.fut.poll_unpin(cx) {
                            Ready(Some(Ok(x))) => {
                                *fut = None;
                                buf.push_back(x);
                                continue;
                            }
                            Ready(Some(Err(e))) => {
                                *fut = None;
                                self.state = State::Error;
                                Ready(Some(Err(e.into())))
                            }
                            Ready(None) => {
                                *fut = None;
                                self.inp_mt = None;
                                continue;
                            }
                            Pending => Pending,
                        }
                    } else if inp.is_some() {
                        let buf = core::mem::replace(buf, VecDeque::new());
                        self.state = State::ReadingMt(Some(Self::setup_read_any(inp)), buf, inp.take());
                        continue;
                    } else {
                        let buf = core::mem::replace(&mut self.buf_st, VecDeque::new());
                        self.state = State::ReadingSt(None, buf, self.inp_st.take());
                        continue;
                    }
                }
                State::ReadingSt(fut, buf, inp) => {
                    if let Some(x) = buf.pop_front() {
                        out2.push_back(x);
                        continue;
                    } else if let Some(fut2) = fut {
                        match fut2.fut.poll_unpin(cx) {
                            Ready(Some(Ok(x))) => {
                                *fut = None;
                                buf.push_back(x);
                                continue;
                            }
                            Ready(Some(Err(e))) => {
                                *fut = None;
                                self.state = State::Error;
                                Ready(Some(Err(e.into())))
                            }
                            Ready(None) => {
                                *fut = None;
                                self.inp_st = None;
                                continue;
                            }
                            Pending => Pending,
                        }
                    } else if inp.is_some() {
                        let buf = core::mem::replace(buf, VecDeque::new());
                        self.state = State::ReadingSt(Some(Self::setup_read_any(inp)), buf, inp.take());
                        continue;
                    } else {
                        debug!("fully done");
                        Ready(None)
                    }
                }
                State::Error => Ready(None),
            };
        }
    }
}
