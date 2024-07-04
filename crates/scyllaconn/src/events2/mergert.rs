use super::events::EventsStreamRt;
use super::firstbefore::FirstBeforeAndInside;
use crate::events2::firstbefore;
use crate::range::ScyllaSeriesRange;
use crate::worker::ScyllaQueue;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::WithLen;
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
#[cstm(name = "EventsMergeRt")]
pub enum Error {
    Input(#[from] crate::events2::firstbefore::Error),
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

type TI = FirstBeforeAndInside<EventsStreamRt, ChannelEvents>;
type INPI = Result<crate::events2::firstbefore::Output<ChannelEvents>, crate::events2::firstbefore::Error>;

struct ReadEvents {
    fut: Pin<Box<dyn Future<Output = Option<INPI>> + Send>>,
}

enum State {
    Begin,
    FetchFirstSt(ReadEvents),
    FetchFirstMt(ReadEvents),
    FetchFirstLt(ReadEvents),
    ReadingLt(Option<ReadEvents>, VecDeque<ChannelEvents>, Option<Box<TI>>),
    ReadingMt(Option<ReadEvents>, VecDeque<ChannelEvents>, Option<Box<TI>>),
    ReadingSt(Option<ReadEvents>, VecDeque<ChannelEvents>, Option<Box<TI>>),
    Done,
}

pub struct MergeRts {
    series: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    range: ScyllaSeriesRange,
    range_mt: ScyllaSeriesRange,
    range_lt: ScyllaSeriesRange,
    with_values: bool,
    scyqueue: ScyllaQueue,
    inp_st: Option<Box<TI>>,
    inp_mt: Option<Box<TI>>,
    inp_lt: Option<Box<TI>>,
    state: State,
    buf_st: VecDeque<ChannelEvents>,
    buf_mt: VecDeque<ChannelEvents>,
    buf_lt: VecDeque<ChannelEvents>,
    out: VecDeque<ChannelEvents>,
    buf_before: Option<ChannelEvents>,
    ts_seen_max: u64,
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
            range_mt: range.clone(),
            range_lt: range.clone(),
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
            buf_before: None,
            ts_seen_max: 0,
        }
    }

    fn setup_first_st(&mut self) {
        let rt = RetentionTime::Short;
        let limbuf = &VecDeque::new();
        let inpdst = &mut self.inp_st;
        let range = Self::constrained_range(&self.range, limbuf);
        debug!("setup_first_st  constrained beg  {}", range.beg().ns());
        let tsbeg = range.beg();
        let inp = EventsStreamRt::new(
            rt,
            self.series.clone(),
            self.scalar_type.clone(),
            self.shape.clone(),
            range,
            self.with_values,
            self.scyqueue.clone(),
        );
        let inp = TI::new(inp, tsbeg);
        *inpdst = Some(Box::new(inp));
    }

    fn setup_first_mt(&mut self) {
        let rt = RetentionTime::Medium;
        let limbuf = &self.buf_st;
        let inpdst = &mut self.inp_mt;
        let range = Self::constrained_range(&self.range_mt, limbuf);
        self.range_lt = range.clone();
        debug!("setup_first_mt  constrained beg  {}", range.beg().ns());
        let tsbeg = range.beg();
        let inp = EventsStreamRt::new(
            rt,
            self.series.clone(),
            self.scalar_type.clone(),
            self.shape.clone(),
            range,
            self.with_values,
            self.scyqueue.clone(),
        );
        let inp = TI::new(inp, tsbeg);
        *inpdst = Some(Box::new(inp));
    }

    fn setup_first_lt(&mut self) {
        let rt = RetentionTime::Long;
        let limbuf = &self.buf_mt;
        let inpdst = &mut self.inp_lt;
        let range = Self::constrained_range(&self.range_lt, limbuf);
        debug!("setup_first_lt  constrained beg  {}", range.beg().ns());
        let tsbeg = range.beg();
        let inp = EventsStreamRt::new(
            rt,
            self.series.clone(),
            self.scalar_type.clone(),
            self.shape.clone(),
            range,
            self.with_values,
            self.scyqueue.clone(),
        );
        let inp = TI::new(inp, tsbeg);
        *inpdst = Some(Box::new(inp));
    }

    fn setup_read_st(&mut self) -> ReadEvents {
        let stream = unsafe { &mut *(self.inp_st.as_mut().unwrap().as_mut() as *mut TI) };
        let fut = Box::pin(stream.next());
        ReadEvents { fut }
    }

    fn setup_read_mt(&mut self) -> ReadEvents {
        let stream = unsafe { &mut *(self.inp_mt.as_mut().unwrap().as_mut() as *mut TI) };
        let fut = Box::pin(stream.next());
        ReadEvents { fut }
    }

    fn setup_read_lt(&mut self) -> ReadEvents {
        let stream = unsafe { &mut *(self.inp_lt.as_mut().unwrap().as_mut() as *mut TI) };
        let fut = Box::pin(stream.next());
        ReadEvents { fut }
    }

    fn setup_read_any(inp: &mut Option<Box<TI>>) -> ReadEvents {
        let stream = unsafe { &mut *(inp.as_mut().unwrap().as_mut() as *mut TI) };
        let fut = Box::pin(stream.next());
        ReadEvents { fut }
    }

    fn constrained_range(full: &ScyllaSeriesRange, buf: &VecDeque<ChannelEvents>) -> ScyllaSeriesRange {
        debug!("constrained_range  {:?}  {:?}", full, buf.front());
        if let Some(e) = buf.front() {
            if let Some(ts) = e.ts_min() {
                let nrange = NanoRange::from((full.beg().ns(), ts));
                ScyllaSeriesRange::from(&SeriesRange::from(nrange))
            } else {
                debug!("no ts even though should not have empty buffers");
                full.clone()
            }
        } else {
            full.clone()
        }
    }

    fn handle_first_st(&mut self, mut before: ChannelEvents, bulk: ChannelEvents) {
        Self::move_latest_to_before_buf(&mut before, &mut self.buf_before);
        self.buf_st.push_back(bulk);
        self.setup_first_mt();
        self.state = State::FetchFirstMt(self.setup_read_mt());
    }

    fn handle_first_mt(&mut self, mut before: ChannelEvents, bulk: ChannelEvents) {
        Self::move_latest_to_before_buf(&mut before, &mut self.buf_before);
        self.buf_mt.push_back(bulk);
        self.setup_first_lt();
        self.state = State::FetchFirstLt(self.setup_read_lt());
    }

    fn handle_first_lt(&mut self, mut before: ChannelEvents, bulk: ChannelEvents) {
        Self::move_latest_to_before_buf(&mut before, &mut self.buf_before);
        self.buf_lt.push_back(bulk);
        let buf = core::mem::replace(&mut self.buf_lt, VecDeque::new());
        self.state = State::ReadingLt(None, buf, self.inp_lt.take());
    }

    fn move_latest_to_before_buf(before: &mut ChannelEvents, buf: &mut Option<ChannelEvents>) {
        if buf.is_none() {
            *buf = Some(before.new_empty());
        }
        let buf = buf.as_mut().unwrap();
        if let Some(tsn) = before.ts_max() {
            if let Some(tse) = buf.ts_max() {
                if tsn > tse {
                    let n = before.len();
                    buf.clear();
                    before.drain_into(buf, (n - 1, n)).unwrap();
                }
            }
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
                debug!("emit item  {}  {:?}", items_0::Events::verify(&item), item);
                if items_0::Events::verify(&item) != true {
                    debug!("{}bad item {:?}", "\n\n--------------------------\n", item);
                    self.state = State::Done;
                }
                if let Some(item_min) = item.ts_min() {
                    if item_min < self.ts_seen_max {
                        debug!(
                            "{}ordering error A  {}  {}",
                            "\n\n--------------------------\n", item_min, self.ts_seen_max
                        );
                        self.state = State::Done;
                        break Ready(Some(Err(Error::Logic)));
                    }
                }
                if let Some(item_max) = item.ts_max() {
                    if item_max < self.ts_seen_max {
                        debug!(
                            "{}ordering error B  {}  {}",
                            "\n\n--------------------------\n", item_max, self.ts_seen_max
                        );
                        self.state = State::Done;
                        break Ready(Some(Err(Error::Logic)));
                    } else {
                        self.ts_seen_max = item_max;
                    }
                }
                break Ready(Some(Ok(item)));
            }
            break match &mut self.state {
                State::Begin => {
                    self.setup_first_st();
                    self.state = State::FetchFirstSt(self.setup_read_st());
                    continue;
                }
                State::FetchFirstSt(st2) => match st2.fut.poll_unpin(cx) {
                    Ready(Some(Ok(x))) => match x {
                        firstbefore::Output::First(before, bulk) => {
                            debug!("have first from ST");
                            self.handle_first_st(before, bulk);
                            continue;
                        }
                        firstbefore::Output::Bulk(_) => {
                            self.state = State::Done;
                            let e = Error::Logic;
                            Ready(Some(Err(e)))
                        }
                    },
                    Ready(Some(Err(e))) => {
                        self.state = State::Done;
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
                    Ready(Some(Ok(x))) => match x {
                        firstbefore::Output::First(before, bulk) => {
                            debug!("have first from MT");
                            self.handle_first_mt(before, bulk);
                            continue;
                        }
                        firstbefore::Output::Bulk(_) => {
                            self.state = State::Done;
                            let e = Error::Logic;
                            Ready(Some(Err(e)))
                        }
                    },
                    Ready(Some(Err(e))) => {
                        self.state = State::Done;
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
                    Ready(Some(Ok(x))) => match x {
                        firstbefore::Output::First(before, bulk) => {
                            debug!("have first from LT");
                            self.handle_first_lt(before, bulk);
                            continue;
                        }
                        firstbefore::Output::Bulk(_) => {
                            self.state = State::Done;
                            let e = Error::Logic;
                            Ready(Some(Err(e)))
                        }
                    },
                    Ready(Some(Err(e))) => {
                        self.state = State::Done;
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
                                match x {
                                    firstbefore::Output::Bulk(x) => {
                                        buf.push_back(x);
                                        continue;
                                    }
                                    firstbefore::Output::First(_, _) => {
                                        self.state = State::Done;
                                        let e = Error::Logic;
                                        Ready(Some(Err(e)))
                                    }
                                }
                            }
                            Ready(Some(Err(e))) => {
                                *fut = None;
                                self.state = State::Done;
                                Ready(Some(Err(e.into())))
                            }
                            Ready(None) => {
                                *fut = None;
                                *inp = None;
                                continue;
                            }
                            Pending => Pending,
                        }
                    } else if inp.is_some() {
                        let buf = core::mem::replace(buf, VecDeque::new());
                        self.state = State::ReadingLt(Some(Self::setup_read_any(inp)), buf, inp.take());
                        continue;
                    } else {
                        debug!("transition ReadingLt to ReadingMt");
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
                                match x {
                                    firstbefore::Output::Bulk(x) => {
                                        buf.push_back(x);
                                        continue;
                                    }
                                    firstbefore::Output::First(_, _) => {
                                        self.state = State::Done;
                                        let e = Error::Logic;
                                        Ready(Some(Err(e)))
                                    }
                                }
                            }
                            Ready(Some(Err(e))) => {
                                *fut = None;
                                self.state = State::Done;
                                Ready(Some(Err(e.into())))
                            }
                            Ready(None) => {
                                *fut = None;
                                *inp = None;
                                continue;
                            }
                            Pending => Pending,
                        }
                    } else if inp.is_some() {
                        let buf = core::mem::replace(buf, VecDeque::new());
                        self.state = State::ReadingMt(Some(Self::setup_read_any(inp)), buf, inp.take());
                        continue;
                    } else {
                        debug!("transition ReadingMt to ReadingSt");
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
                                match x {
                                    firstbefore::Output::Bulk(x) => {
                                        buf.push_back(x);
                                        continue;
                                    }
                                    firstbefore::Output::First(_, _) => {
                                        self.state = State::Done;
                                        let e = Error::Logic;
                                        Ready(Some(Err(e)))
                                    }
                                }
                            }
                            Ready(Some(Err(e))) => {
                                *fut = None;
                                self.state = State::Done;
                                Ready(Some(Err(e.into())))
                            }
                            Ready(None) => {
                                *fut = None;
                                *inp = None;
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
    let x: MergeRts = phantomval();
    trait_assert(x);
}

fn phantomval<T>() -> T {
    panic!()
}
