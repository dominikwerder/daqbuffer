use super::msp::MspStreamRt;
use crate::events::read_next_values;
use crate::events::ReadJobTrace;
use crate::events::ReadNextValuesOpts;
use crate::range::ScyllaSeriesRange;
use crate::worker::ScyllaQueue;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::Events;
use items_2::channelevents::ChannelEvents;
use netpod::log::*;
use netpod::ttl::RetentionTime;
use netpod::ChConf;
use netpod::EnumVariant;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsMsVecFmt;
use series::SeriesId;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[allow(unused)]
macro_rules! trace_fetch {
    ($($arg:tt)*) => ( if true { trace!($($arg)*); } ) }

#[allow(unused)]
macro_rules! trace_emit {
    ($($arg:tt)*) => {
        if true {
            trace!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! warn_item {
    ($($arg:tt)*) => {
        if true {
            debug!($($arg)*);
        }
    };
}

#[allow(unused)]
macro_rules! trace_every_event {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[derive(Debug, Clone)]
pub struct EventReadOpts {
    pub with_values: bool,
    pub enum_as_strings: bool,
    pub one_before: bool,
}

impl EventReadOpts {
    pub fn new(one_before: bool, with_values: bool, enum_as_strings: bool) -> Self {
        Self {
            one_before,
            with_values,
            enum_as_strings,
        }
    }
}

#[derive(Debug, ThisError)]
#[cstm(name = "ScyllaEvents")]
pub enum Error {
    Worker(#[from] crate::worker::Error),
    Events(#[from] crate::events::Error),
    Msp(#[from] crate::events2::msp::Error),
    Unordered,
    OutOfRange,
    BadBatch,
    Logic,
    Merge(#[from] items_0::MergeError),
    TruncateLogic,
    AlreadyTaken,
}

struct FetchMsp {
    fut: Pin<Box<dyn Future<Output = Option<Result<TsMs, crate::events2::msp::Error>>> + Send>>,
}

type ReadEventsFutOut = Result<(Box<dyn Events>, ReadJobTrace), crate::events2::events::Error>;

type FetchEventsFut = Pin<Box<dyn Future<Output = ReadEventsFutOut> + Send>>;

enum Fst<F>
where
    F: Future + Unpin,
    <F as Future>::Output: Unpin,
{
    Ongoing(F),
    Ready(<F as Future>::Output),
    Taken,
}

impl<F> Fst<F>
where
    F: Future + Unpin,
    <F as Future>::Output: Unpin,
{
    fn take_if_ready(&mut self) -> Poll<Option<<F as Future>::Output>> {
        use Poll::*;
        match self {
            Fst::Ongoing(_) => Pending,
            Fst::Ready(_) => match core::mem::replace(self, Fst::Taken) {
                Fst::Ready(x) => Ready(Some(x)),
                _ => panic!(),
            },
            Fst::Taken => Ready(None),
        }
    }
}

impl<F> Future for Fst<F>
where
    F: Future + Unpin,
    <F as Future>::Output: Unpin,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        match self.as_mut().get_mut() {
            Fst::Ongoing(fut) => match fut.poll_unpin(cx) {
                Ready(x) => {
                    *self = Fst::Ready(x);
                    Ready(())
                }
                Pending => Pending,
            },
            Fst::Ready(_) => Ready(()),
            Fst::Taken => Ready(()),
        }
    }
}

struct FetchEvents2 {
    fut: Fst<FetchEventsFut>,
}

struct FetchEvents {
    a: FetchEvents2,
    b: Option<FetchEvents2>,
}

enum ReadingState {
    FetchMsp(FetchMsp),
    FetchEvents(FetchEvents),
}

struct ReadingBck {
    reading_state: ReadingState,
}

struct ReadingFwd {
    reading_state: ReadingState,
}

enum State {
    Begin,
    ReadingBck(ReadingBck),
    ReadingFwd(ReadingFwd),
    InputDone,
    Done,
}

pub struct EventsStreamRt {
    rt: RetentionTime,
    ch_conf: ChConf,
    series: SeriesId,
    range: ScyllaSeriesRange,
    readopts: EventReadOpts,
    state: State,
    scyqueue: ScyllaQueue,
    msp_inp: MspStreamRt,
    msp_buf: VecDeque<TsMs>,
    msp_buf_bck: VecDeque<TsMs>,
    out: VecDeque<Box<dyn Events>>,
    out_cnt: u64,
    ts_seen_max: u64,
}

impl EventsStreamRt {
    pub fn new(
        rt: RetentionTime,
        ch_conf: ChConf,
        range: ScyllaSeriesRange,
        readopts: EventReadOpts,
        scyqueue: ScyllaQueue,
    ) -> Self {
        debug!("EventsStreamRt::new  {ch_conf:?}  {range:?}  {rt:?}  {readopts:?}");
        let series = SeriesId::new(ch_conf.series());
        let msp_inp = crate::events2::msp::MspStreamRt::new(rt.clone(), series, range.clone(), scyqueue.clone());
        Self {
            rt,
            ch_conf,
            series,
            range,
            readopts,
            state: State::Begin,
            scyqueue,
            msp_inp,
            msp_buf: VecDeque::new(),
            msp_buf_bck: VecDeque::new(),
            out: VecDeque::new(),
            out_cnt: 0,
            ts_seen_max: 0,
        }
    }

    fn make_msp_read_fut(
        msp_inp: &mut MspStreamRt,
    ) -> Pin<Box<dyn Future<Output = Option<Result<TsMs, crate::events2::msp::Error>>> + Send>> {
        trace_fetch!("make_msp_read_fut");
        let msp_inp = unsafe {
            let ptr = msp_inp as *mut MspStreamRt;
            &mut *ptr
        };
        let fut = Box::pin(msp_inp.next());
        fut
    }

    fn make_read_events_fut(
        &mut self,
        ts_msp: TsMs,
        bck: bool,
        scyqueue: ScyllaQueue,
        jobtrace: ReadJobTrace,
    ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn Events>, ReadJobTrace), Error>> + Send>> {
        trace!("make_read_events_fut   ---   {}   ---", ts_msp);
        let opts = ReadNextValuesOpts::new(
            self.rt.clone(),
            self.series.clone(),
            ts_msp,
            self.range.clone(),
            !bck,
            self.readopts.clone(),
            scyqueue,
        );
        let scalar_type = self.ch_conf.scalar_type().clone();
        let shape = self.ch_conf.shape().clone();
        trace_fetch!(
            "make_read_events_fut  bck {}  msp {:?} {}  {:?}  {:?}",
            bck,
            ts_msp,
            ts_msp.fmt(),
            shape,
            scalar_type
        );
        let fut = async move {
            let params = crate::events::ReadNextValuesParams { opts, jobtrace };
            let ret = match &shape {
                Shape::Scalar => match &scalar_type {
                    ScalarType::U8 => read_next_values::<u8>(params).await,
                    ScalarType::U16 => read_next_values::<u16>(params).await,
                    ScalarType::U32 => read_next_values::<u32>(params).await,
                    ScalarType::U64 => read_next_values::<u64>(params).await,
                    ScalarType::I8 => read_next_values::<i8>(params).await,
                    ScalarType::I16 => read_next_values::<i16>(params).await,
                    ScalarType::I32 => read_next_values::<i32>(params).await,
                    ScalarType::I64 => read_next_values::<i64>(params).await,
                    ScalarType::F32 => read_next_values::<f32>(params).await,
                    ScalarType::F64 => read_next_values::<f64>(params).await,
                    ScalarType::BOOL => read_next_values::<bool>(params).await,
                    ScalarType::STRING => read_next_values::<String>(params).await,
                    ScalarType::Enum => read_next_values::<EnumVariant>(params).await,
                },
                Shape::Wave(_) => match &scalar_type {
                    ScalarType::U8 => read_next_values::<Vec<u8>>(params).await,
                    ScalarType::U16 => read_next_values::<Vec<u16>>(params).await,
                    ScalarType::U32 => read_next_values::<Vec<u32>>(params).await,
                    ScalarType::U64 => read_next_values::<Vec<u64>>(params).await,
                    ScalarType::I8 => read_next_values::<Vec<i8>>(params).await,
                    ScalarType::I16 => read_next_values::<Vec<i16>>(params).await,
                    ScalarType::I32 => read_next_values::<Vec<i32>>(params).await,
                    ScalarType::I64 => read_next_values::<Vec<i64>>(params).await,
                    ScalarType::F32 => read_next_values::<Vec<f32>>(params).await,
                    ScalarType::F64 => read_next_values::<Vec<f64>>(params).await,
                    ScalarType::BOOL => read_next_values::<Vec<bool>>(params).await,
                    ScalarType::STRING => {
                        warn!("read not yet supported  {:?}  {:?}", shape, scalar_type);
                        err::todoval()
                    }
                    ScalarType::Enum => {
                        warn!("read not yet supported  {:?}  {:?}", shape, scalar_type);
                        err::todoval()
                    }
                },
                _ => {
                    error!("TODO ReadValues add more types");
                    err::todoval()
                }
            };
            ret.map_err(Error::from)
        };
        Box::pin(fut)
    }

    fn transition_to_bck_read(&mut self) {
        trace_fetch!("transition_to_bck_read  A  {}", TsMsVecFmt(&self.msp_buf));
        for ts in self.msp_buf.iter() {
            if ts.ns() < self.range.beg() {
                self.msp_buf_bck.push_back(ts.clone());
            }
        }
        let c = self.msp_buf.iter().take_while(|x| x.ns() < self.range.beg()).count();
        let g = c.max(1) - 1;
        for _ in 0..g {
            self.msp_buf.pop_front();
        }
        trace_fetch!(
            "transition_to_bck_read  B  {}  {}",
            TsMsVecFmt(&self.msp_buf_bck),
            TsMsVecFmt(&self.msp_buf)
        );
        self.setup_bck_read();
    }

    fn setup_bck_read(&mut self) {
        if let Some(ts) = self.msp_buf_bck.pop_back() {
            trace_fetch!("setup_bck_read  {}", ts.fmt());
            let jobtrace = ReadJobTrace::new();
            let scyqueue = self.scyqueue.clone();
            let fut = self.make_read_events_fut(ts, true, scyqueue, jobtrace);
            self.state = State::ReadingBck(ReadingBck {
                reading_state: ReadingState::FetchEvents(FetchEvents {
                    a: FetchEvents2 { fut: Fst::Ongoing(fut) },
                    b: None,
                }),
            });
        } else {
            trace_fetch!("setup_bck_read  no msp");
            self.transition_to_fwd_read();
        }
    }

    fn transition_to_fwd_read(&mut self) {
        trace_fetch!("transition_to_fwd_read");
        self.msp_buf_bck = VecDeque::new();
        trace_fetch!("transition_to_fwd_read  {}", TsMsVecFmt(&self.msp_buf));
        self.setup_fwd_read();
    }

    fn setup_fwd_read(&mut self) {
        // TODO always try to setup all available slots.
        if let Some(ts) = self.msp_buf.pop_front() {
            trace_fetch!("setup_fwd_read  {}", ts.fmt());
            let jobtrace = ReadJobTrace::new();
            let scyqueue = self.scyqueue.clone();
            let fut = self.make_read_events_fut(ts, false, scyqueue, jobtrace);

            // Assert that this fn is only called when there is at least one slot available.
            // At the moment with 2 slots, this means that the 2nd is always empty.
            // TODO careful in general, MUST NOT overwrite the secondary slot with None, there could be something running.

            if let State::ReadingFwd(st2) = &self.state {
                if let ReadingState::FetchEvents(st3) = &st2.reading_state {
                    if st3.b.is_some() {
                        panic!()
                    } else {
                    }
                } else {
                    self.state = State::ReadingFwd(ReadingFwd {
                        reading_state: ReadingState::FetchEvents(FetchEvents {
                            a: FetchEvents2 { fut: Fst::Ongoing(fut) },
                            b: None,
                        }),
                    });
                }
            } else {
                self.state = State::ReadingFwd(ReadingFwd {
                    reading_state: ReadingState::FetchEvents(FetchEvents {
                        a: FetchEvents2 { fut: Fst::Ongoing(fut) },
                        b: None,
                    }),
                });
            }

            if let State::ReadingFwd(st2) = &self.state {
                if let ReadingState::FetchEvents(st3) = &st2.reading_state {
                    if st3.b.is_some() {
                        panic!()
                    } else {
                        // Try the same with the 2nd slot
                        if let Some(ts) = self.msp_buf.pop_front() {
                            trace_fetch!("setup_fwd_read  {}  SECONDARY SLOT", ts.fmt());
                            let jobtrace = ReadJobTrace::new();
                            let scyqueue = self.scyqueue.clone();
                            let fut = self.make_read_events_fut(ts, false, scyqueue, jobtrace);
                            if let State::ReadingFwd(st2) = &mut self.state {
                                if let ReadingState::FetchEvents(st3) = &mut st2.reading_state {
                                    if st3.b.is_some() {
                                        panic!()
                                    } else {
                                        st3.b = Some(FetchEvents2 { fut: Fst::Ongoing(fut) });
                                    }
                                }
                            }
                        } else {
                            // nothing to do
                        }
                    }
                }
            }
        } else {
            trace_fetch!("setup_fwd_read  no msp");
            let fut = Self::make_msp_read_fut(&mut self.msp_inp);
            self.state = State::ReadingFwd(ReadingFwd {
                reading_state: ReadingState::FetchMsp(FetchMsp { fut }),
            });
        }
    }
}

impl Stream for EventsStreamRt {
    type Item = Result<ChannelEvents, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            if let Some(mut item) = self.out.pop_front() {
                if !item.verify() {
                    warn_item!("{}bad item {:?}", "\n\n--------------------------\n", item);
                    self.state = State::Done;
                    break Ready(Some(Err(Error::BadBatch)));
                }
                if let Some(item_min) = item.ts_min() {
                    if !self.readopts.one_before && item_min < self.range.beg().ns() {
                        warn_item!(
                            "{}out of range error A  {}  {:?}",
                            "\n\n--------------------------\n",
                            item_min,
                            self.range
                        );
                        self.state = State::Done;
                        break Ready(Some(Err(Error::OutOfRange)));
                    }
                    if item_min < self.ts_seen_max {
                        warn_item!(
                            "{}ordering error A  {}  {}",
                            "\n\n--------------------------\n",
                            item_min,
                            self.ts_seen_max
                        );
                        let mut r = items_2::merger::Mergeable::new_empty(&item);
                        match items_2::merger::Mergeable::find_highest_index_lt(&item, self.ts_seen_max) {
                            Some(ix) => match items_2::merger::Mergeable::drain_into(&mut item, &mut r, (0, 1 + ix)) {
                                Ok(()) => {
                                    // TODO count for metrics
                                }
                                Err(e) => {
                                    self.state = State::Done;
                                    break Ready(Some(Err(e.into())));
                                }
                            },
                            None => {
                                self.state = State::Done;
                                break Ready(Some(Err(Error::TruncateLogic)));
                            }
                        }
                    }
                }
                if let Some(item_max) = item.ts_max() {
                    if item_max >= self.range.end().ns() {
                        warn_item!(
                            "{}out of range error B  {}  {:?}",
                            "\n\n--------------------------\n",
                            item_max,
                            self.range
                        );
                        self.state = State::Done;
                        break Ready(Some(Err(Error::OutOfRange)));
                    }
                    if item_max < self.ts_seen_max {
                        warn_item!(
                            "{}ordering error B  {}  {}",
                            "\n\n--------------------------\n",
                            item_max,
                            self.ts_seen_max
                        );
                        self.state = State::Done;
                        break Ready(Some(Err(Error::Unordered)));
                    } else {
                        self.ts_seen_max = item_max;
                    }
                }
                trace_emit!("deliver item  {}", item.output_info());
                self.out_cnt += item.len() as u64;
                break Ready(Some(Ok(ChannelEvents::Events(item))));
            }
            break match &mut self.state {
                State::Begin => {
                    if self.readopts.one_before {
                        trace_fetch!("State::Begin  Bck");
                        let fut = Self::make_msp_read_fut(&mut self.msp_inp);
                        self.state = State::ReadingBck(ReadingBck {
                            reading_state: ReadingState::FetchMsp(FetchMsp { fut }),
                        });
                    } else {
                        trace_fetch!("State::Begin  Fwd");
                        let fut = Self::make_msp_read_fut(&mut self.msp_inp);
                        self.state = State::ReadingFwd(ReadingFwd {
                            reading_state: ReadingState::FetchMsp(FetchMsp { fut }),
                        });
                    }
                    continue;
                }
                State::ReadingBck(st) => match &mut st.reading_state {
                    ReadingState::FetchMsp(st2) => match st2.fut.poll_unpin(cx) {
                        Ready(Some(Ok(ts))) => {
                            trace_fetch!("ReadingBck  FetchMsp  {}", ts.fmt());
                            self.msp_buf.push_back(ts);
                            if ts.ns() >= self.range.beg() {
                                self.transition_to_bck_read();
                            } else {
                                let fut = Self::make_msp_read_fut(&mut self.msp_inp);
                                self.state = State::ReadingBck(ReadingBck {
                                    reading_state: ReadingState::FetchMsp(FetchMsp { fut }),
                                });
                            }
                            continue;
                        }
                        Ready(Some(Err(e))) => Ready(Some(Err(e.into()))),
                        Ready(None) => {
                            self.transition_to_bck_read();
                            continue;
                        }
                        Pending => Pending,
                    },
                    ReadingState::FetchEvents(st2) => match st2.a.fut.poll_unpin(cx) {
                        Ready(()) => match st2.a.fut.take_if_ready() {
                            Ready(Some(x)) => match x {
                                Ok((mut evs, jobtrace)) => {
                                    use items_2::merger::Mergeable;
                                    trace!("ReadingBck  {jobtrace}");
                                    trace_fetch!("ReadingBck  FetchEvents  got len {}", evs.len());
                                    for ts in Mergeable::tss(&evs) {
                                        trace_every_event!("ReadingBck  FetchEvents     ts {}", ts.fmt());
                                    }
                                    if let Some(ix) = Mergeable::find_highest_index_lt(&evs, self.range.beg().ns()) {
                                        trace_fetch!("ReadingBck  FetchEvents  find_highest_index_lt {:?}", ix);
                                        let mut y = Mergeable::new_empty(&evs);
                                        match Mergeable::drain_into(&mut evs, &mut y, (ix, 1 + ix)) {
                                            Ok(()) => {
                                                trace_fetch!("ReadingBck  FetchEvents  drained y len {:?}", y.len());
                                                self.out.push_back(y);
                                                self.transition_to_fwd_read();
                                                continue;
                                            }
                                            Err(e) => {
                                                self.state = State::Done;
                                                Ready(Some(Err(e.into())))
                                            }
                                        }
                                    } else {
                                        trace_fetch!("ReadingBck  FetchEvents  find_highest_index_lt None");
                                        self.setup_bck_read();
                                        continue;
                                    }
                                }
                                Err(e) => {
                                    self.state = State::Done;
                                    Ready(Some(Err(e)))
                                }
                            },
                            Ready(None) => {
                                self.state = State::Done;
                                Ready(Some(Err(Error::AlreadyTaken)))
                            }
                            Pending => {
                                self.state = State::Done;
                                Ready(Some(Err(Error::Logic)))
                            }
                        },
                        Pending => Pending,
                    },
                },
                State::ReadingFwd(st) => match &mut st.reading_state {
                    ReadingState::FetchMsp(st2) => match st2.fut.poll_unpin(cx) {
                        Ready(Some(Ok(ts))) => {
                            trace_fetch!("ReadingFwd  FetchMsp  {}", ts.fmt());
                            self.msp_buf.push_back(ts);
                            self.setup_fwd_read();
                            continue;
                        }
                        Ready(Some(Err(e))) => Ready(Some(Err(e.into()))),
                        Ready(None) => {
                            self.state = State::InputDone;
                            continue;
                        }
                        Pending => Pending,
                    },
                    ReadingState::FetchEvents(st2) => {
                        let _ = st2.a.fut.poll_unpin(cx);
                        if let Some(st3) = st2.b.as_mut() {
                            let _ = st3.fut.poll_unpin(cx);
                        }
                        match st2.a.fut.take_if_ready() {
                            Ready(Some(x)) => {
                                if let Some(b) = st2.b.take() {
                                    st2.a = b;
                                }
                                match x {
                                    Ok((evs, mut jobtrace)) => {
                                        jobtrace.add_event_now(crate::events::ReadEventKind::EventsStreamRtSees(
                                            evs.len() as u32,
                                        ));
                                        use items_2::merger::Mergeable;
                                        trace!("ReadingFwd  {jobtrace}");
                                        for ts in Mergeable::tss(&evs) {
                                            trace_every_event!("ReadingFwd  FetchEvents     ts {}", ts.fmt());
                                        }
                                        self.out.push_back(evs);
                                        self.setup_fwd_read();
                                        continue;
                                    }
                                    Err(e) => {
                                        self.state = State::Done;
                                        Ready(Some(Err(e.into())))
                                    }
                                }
                            }
                            Ready(None) => {
                                self.state = State::Done;
                                Ready(Some(Err(Error::Logic)))
                            }
                            Pending => Pending,
                        }
                    }
                },
                State::InputDone => {
                    if self.out.len() == 0 {
                        self.state = State::Done;
                        if self.out_cnt == 0 {
                            let d =
                                items_2::empty::empty_events_dyn_ev(self.ch_conf.scalar_type(), self.ch_conf.shape());
                            match d {
                                Ok(empty) => {
                                    // let empty = items_0::streamitem::sitem_data(ChannelEvents::Events(empty));
                                    let item = items_2::channelevents::ChannelEvents::Events(empty);
                                    Ready(Some(Ok(item)))
                                }
                                Err(_) => {
                                    self.state = State::Done;
                                    Ready(Some(Err(Error::Logic)))
                                }
                            }
                        } else {
                            continue;
                        }
                    } else {
                        continue;
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
    let x: EventsStreamRt = phantomval();
    trait_assert(x);
}

fn phantomval<T>() -> T {
    panic!()
}
