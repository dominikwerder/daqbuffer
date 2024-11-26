use super::msp::MspStreamRt;
use crate::events::read_next_values;
use crate::events::ReadJobTrace;
use crate::events::ReadNextValuesOpts;
use crate::range::ScyllaSeriesRange;
use crate::worker::ScyllaQueue;
use daqbuf_err as err;
use daqbuf_series::SeriesId;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::merge::DrainIntoNewDynResult;
use items_0::merge::MergeableDyn;
use items_0::timebin::BinningggContainerEventsDyn;
use items_2::channelevents::ChannelEvents;
use netpod::log::*;
use netpod::ttl::RetentionTime;
use netpod::ChConf;
use netpod::EnumVariant;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsMsVecFmt;
use netpod::TsNano;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

macro_rules! trace_init { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

macro_rules! trace_fetch { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

macro_rules! trace_msp_fetch { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

macro_rules! trace_redo_fwd_read { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

macro_rules! trace_emit { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

macro_rules! trace_every_event { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

macro_rules! warn_item { ($($arg:tt)*) => ( if true { debug!($($arg)*); } ) }

#[derive(Debug, Clone)]
pub struct EventReadOpts {
    with_values: bool,
    one_before: bool,
    qucap: u32,
}

impl EventReadOpts {
    pub fn new(one_before: bool, with_values: bool, qucap: Option<u32>) -> Self {
        Self {
            one_before,
            with_values,
            qucap: qucap.unwrap_or(1),
        }
    }

    pub fn with_values(&self) -> bool {
        self.with_values
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
    ReadQueueEmptyBck,
    ReadQueueEmptyFwd,
    Logic,
    TruncateLogic,
    AlreadyTaken,
    DrainFailure,
}

struct FetchMsp {
    fut: Pin<Box<dyn Future<Output = Result<Vec<TsMs>, crate::events2::msp::Error>> + Send>>,
}

type ReadEventsFutOut = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), crate::events2::events::Error>;

type FetchEventsFut = Pin<Box<dyn Future<Output = ReadEventsFutOut> + Send>>;

enum Fst<F>
where
    F: Future + Unpin,
    <F as Future>::Output: Unpin,
{
    Ongoing(F),
    Ready(<F as Future>::Output),
}

impl<F> Fst<F>
where
    F: Future + Unpin,
    <F as Future>::Output: Unpin,
{
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
        }
    }
}

struct ReadQueue {
    cap: usize,
    futs: VecDeque<Fst<FetchEventsFut>>,
}

impl ReadQueue {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            futs: VecDeque::new(),
        }
    }

    fn len(&self) -> usize {
        self.futs.len()
    }

    fn has_space(&self) -> bool {
        self.len() < self.cap
    }

    fn push(&mut self, fut: FetchEventsFut) {
        self.futs.push_back(Fst::Ongoing(fut));
    }
}

impl Stream for ReadQueue {
    type Item = <FetchEventsFut as Future>::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.futs.len() == 0 {
            Ready(None)
        } else {
            for fut in self.futs.iter_mut() {
                let _ = fut.poll_unpin(cx);
            }
            if let Some(Fst::Ready(_)) = self.futs.front() {
                if let Some(Fst::Ready(k)) = self.futs.pop_front() {
                    Ready(Some(k))
                } else {
                    panic!()
                }
            } else {
                Pending
            }
        }
    }
}

struct FetchEvents {
    fut: FetchEventsFut,
}

impl FetchEvents {
    fn from_fut(
        fut: Pin<Box<dyn Future<Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>> + Send>>,
    ) -> Self {
        Self { fut }
    }
}

enum ReadingState {
    FetchMsp(FetchMsp),
    FetchEvents(FetchEvents),
}

struct ReadingBck {
    reading_state: ReadingState,
}

struct ReadingFwd {
    msp_done: bool,
    msp_fut: Option<FetchMsp>,
    qu: ReadQueue,
    make_fut_info: MakeFutInfo,
}

impl ReadingFwd {
    fn new(k: &EventsStreamRt) -> Self {
        Self {
            msp_done: false,
            msp_fut: None,
            qu: ReadQueue::new(k.qucap),
            make_fut_info: MakeFutInfo::new(k),
        }
    }
}

#[derive(Clone)]
struct MakeFutInfo {
    scyqueue: ScyllaQueue,
    rt: RetentionTime,
    series: SeriesId,
    range: ScyllaSeriesRange,
    readopts: EventReadOpts,
    ch_conf: ChConf,
}

impl MakeFutInfo {
    fn new(k: &EventsStreamRt) -> Self {
        Self {
            scyqueue: k.scyqueue.clone(),
            rt: k.rt.clone(),
            series: k.series.clone(),
            range: k.range.clone(),
            readopts: k.readopts.clone(),
            ch_conf: k.ch_conf.clone(),
        }
    }
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
    out: VecDeque<Box<dyn BinningggContainerEventsDyn>>,
    out_cnt: u64,
    ts_seen_max: TsNano,
    qucap: usize,
}

impl EventsStreamRt {
    pub fn new(
        rt: RetentionTime,
        ch_conf: ChConf,
        range: ScyllaSeriesRange,
        readopts: EventReadOpts,
        scyqueue: ScyllaQueue,
    ) -> Self {
        trace_init!("EventsStreamRt::new  {ch_conf:?}  {range:?}  {rt:?}  {readopts:?}");
        let series = SeriesId::new(ch_conf.series());
        let msp_inp = crate::events2::msp::MspStreamRt::new(rt.clone(), series, range.clone(), scyqueue.clone());
        Self {
            qucap: readopts.qucap as usize,
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
            ts_seen_max: TsNano::from_ns(0),
        }
    }

    fn make_msp_read_fut(
        msp_inp: &mut MspStreamRt,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TsMs>, crate::events2::msp::Error>> + Send>> {
        trace_fetch!("make_msp_read_fut");
        let msp_inp = unsafe {
            let ptr = msp_inp as *mut MspStreamRt;
            &mut *ptr
        };
        let fut = async {
            let cap = 128;
            let mut a = Vec::with_capacity(cap);
            while let Some(x) = msp_inp.next().await {
                match x {
                    Ok(x) => {
                        a.push(x);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
                if a.len() >= cap {
                    break;
                }
            }
            Ok(a)
        };
        let fut = Box::pin(fut);
        fut
    }

    fn make_read_events_fut(
        ts_msp: TsMs,
        bck: bool,
        mfi: MakeFutInfo,
        jobtrace: ReadJobTrace,
    ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>> + Send>> {
        let opts = ReadNextValuesOpts::new(mfi.rt, mfi.series, ts_msp, mfi.range, !bck, mfi.readopts, mfi.scyqueue);
        let scalar_type = mfi.ch_conf.scalar_type().clone();
        let shape = mfi.ch_conf.shape().clone();
        trace_fetch!(
            "make_read_events_fut  bck {}  msp {:?} {}  {:?}  {:?}",
            bck,
            ts_msp,
            ts_msp.fmt(),
            shape,
            scalar_type
        );
        let fut = async move {
            if false {
                taskrun::tokio::time::sleep(Duration::from_millis(10)).await;
            }
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
            let mfi = MakeFutInfo::new(self);
            let fut = Self::make_read_events_fut(ts, true, mfi, jobtrace);
            self.state = State::ReadingBck(ReadingBck {
                reading_state: ReadingState::FetchEvents(FetchEvents::from_fut(fut)),
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
        let selfname = "setup_fwd_read";
        trace_fetch!("{selfname}");
        self.state = State::ReadingFwd(ReadingFwd::new(self));
    }

    fn redo_fwd_read(st: &mut ReadingFwd, msp_buf: &mut VecDeque<TsMs>) {
        let selfname = "redo_fwd_read";
        let qu = &mut st.qu;
        trace_redo_fwd_read!("{selfname}  {}  {}  BEFORE", msp_buf.len(), qu.len());
        while qu.has_space() {
            if let Some(ts) = msp_buf.pop_front() {
                trace_fetch!("{selfname}  {}  FILL A SLOT", ts.fmt());
                let jobtrace = ReadJobTrace::new();
                let mfi = st.make_fut_info.clone();
                let fut = Self::make_read_events_fut(ts, false, mfi, jobtrace);
                qu.push(fut);
            } else {
                break;
            }
        }
        trace_redo_fwd_read!("{selfname}  {}  {}  AFTER", msp_buf.len(), qu.len());
    }
}

impl Stream for EventsStreamRt {
    type Item = Result<ChannelEvents, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let mut i = 0usize;
        loop {
            i += 1;
            if i > 500000000000 {
                panic!("too many iterations")
            }
            if let Some(mut item) = self.out.pop_front() {
                if item.is_consistent() == false {
                    warn_item!("{}bad item {:?}", "\n\n--------------------------\n", item);
                    self.state = State::Done;
                    break Ready(Some(Err(Error::BadBatch)));
                }
                if let Some(item_min) = item.ts_min() {
                    if !self.readopts.one_before && item_min < self.range.beg() {
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
                        match MergeableDyn::find_highest_index_lt(item.as_ref(), self.ts_seen_max) {
                            Some(ix) => match MergeableDyn::drain_into_new(item.as_mut(), 0..1 + ix) {
                                DrainIntoNewDynResult::Done(_) => {
                                    // TODO count drained elements for metrics
                                }
                                DrainIntoNewDynResult::Partial(_) => {
                                    self.state = State::Done;
                                    break Ready(Some(Err(Error::DrainFailure)));
                                }
                                DrainIntoNewDynResult::NotCompatible => {
                                    self.state = State::Done;
                                    break Ready(Some(Err(Error::DrainFailure)));
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
                    if item_max >= self.range.end() {
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
                trace_emit!("deliver item  {:?}", item);
                self.out_cnt += item.len() as u64;
                break Ready(Some(Ok(ChannelEvents::Events(item))));
            }
            let self2 = self.as_mut().get_mut();
            let (state, msp_buf) = (&mut self2.state, &mut self2.msp_buf);
            break match state {
                State::Begin => {
                    if self.readopts.one_before {
                        trace_fetch!("State::Begin  Bck");
                        let fut = Self::make_msp_read_fut(&mut self.msp_inp);
                        self.state = State::ReadingBck(ReadingBck {
                            reading_state: ReadingState::FetchMsp(FetchMsp { fut }),
                        });
                    } else {
                        trace_fetch!("State::Begin  Fwd");
                        self.setup_fwd_read();
                    }
                    continue;
                }
                State::ReadingBck(st) => match &mut st.reading_state {
                    ReadingState::FetchMsp(st2) => match st2.fut.poll_unpin(cx) {
                        Ready(Ok(a)) => {
                            if a.len() == 0 {
                                self.transition_to_bck_read();
                                continue;
                            } else {
                                for x in a {
                                    self.msp_buf.push_back(x);
                                }
                                if let Some(ts) = self.msp_buf.back() {
                                    if ts.ns() >= self.range.beg() {
                                        self.transition_to_bck_read();
                                    } else {
                                        let fut = Self::make_msp_read_fut(&mut self.msp_inp);
                                        self.state = State::ReadingBck(ReadingBck {
                                            reading_state: ReadingState::FetchMsp(FetchMsp { fut }),
                                        });
                                    }
                                } else {
                                    panic!("absolutely nothing to read");
                                }
                                continue;
                            }
                        }
                        Ready(Err(e)) => Ready(Some(Err(e.into()))),
                        Pending => Pending,
                    },
                    ReadingState::FetchEvents(st2) => match st2.fut.poll_unpin(cx) {
                        Ready(x) => match x {
                            Ok((mut evs, jobtrace)) => {
                                trace_fetch!("ReadingBck  {jobtrace}");
                                trace_fetch!("ReadingBck  FetchEvents  got len {}", evs.len());
                                for ts in MergeableDyn::tss_for_testing(evs.as_ref()) {
                                    trace_every_event!("ReadingBck  FetchEvents     ts {}", ts.fmt());
                                }
                                if let Some(ix) = MergeableDyn::find_highest_index_lt(evs.as_ref(), self.range.beg()) {
                                    trace_fetch!("ReadingBck  FetchEvents  find_highest_index_lt {:?}", ix);
                                    match MergeableDyn::drain_into_new(evs.as_mut(), ix..1 + ix) {
                                        DrainIntoNewDynResult::Done(y) => {
                                            trace_fetch!("ReadingBck  FetchEvents  drained y len {:?}", y.len());
                                            self.out.push_back(y);
                                            self.transition_to_fwd_read();
                                            continue;
                                        }
                                        DrainIntoNewDynResult::Partial(_) => {
                                            self.state = State::Done;
                                            Ready(Some(Err(Error::DrainFailure)))
                                        }
                                        DrainIntoNewDynResult::NotCompatible => {
                                            self.state = State::Done;
                                            Ready(Some(Err(Error::DrainFailure)))
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
                        Pending => Pending,
                    },
                },
                State::ReadingFwd(st) => {
                    let mut have_pending = false;
                    let mut dbg_have_new_msp_fut = false;
                    if let Some(fut) = st.msp_fut.as_mut() {
                        match fut.fut.poll_unpin(cx) {
                            Ready(a) => {
                                st.msp_fut = None;
                                match a {
                                    Ok(a) => {
                                        if a.len() == 0 {
                                            trace_msp_fetch!("msp input done");
                                            st.msp_done = true;
                                        }
                                        for x in a {
                                            msp_buf.push_back(x);
                                        }
                                    }
                                    Err(e) => {
                                        self.state = State::Done;
                                        return Ready(Some(Err(e.into())));
                                    }
                                }
                            }
                            Pending => {
                                have_pending = true;
                            }
                        }
                    } else if st.msp_done == false && msp_buf.len() < 100 {
                        trace_msp_fetch!("create msp read fut");
                        let fut = Self::make_msp_read_fut(&mut self2.msp_inp);
                        st.msp_fut = Some(FetchMsp { fut });
                        dbg_have_new_msp_fut = true;
                    }
                    if st.qu.has_space() {
                        Self::redo_fwd_read(st, msp_buf);
                    }
                    match st.qu.poll_next_unpin(cx) {
                        Ready(Some(x)) => match x {
                            Ok((evs, mut jobtrace)) => {
                                jobtrace
                                    .add_event_now(crate::events::ReadEventKind::EventsStreamRtSees(evs.len() as u32));
                                trace_fetch!("ReadingFwd  {jobtrace}");
                                for ts in MergeableDyn::tss_for_testing(evs.as_ref()) {
                                    trace_every_event!("ReadingFwd  FetchEvents     ts {}", ts.fmt());
                                }
                                self.out.push_back(evs);
                                continue;
                            }
                            Err(e) => {
                                self.state = State::Done;
                                return Ready(Some(Err(e.into())));
                            }
                        },
                        Ready(None) => {}
                        Pending => {
                            have_pending = true;
                        }
                    }
                    if have_pending {
                        Pending
                    } else {
                        if msp_buf.len() == 0 && st.msp_done && st.qu.len() == 0 {
                            self.state = State::InputDone;
                            continue;
                        } else if self.out.len() != 0 {
                            continue;
                        } else if dbg_have_new_msp_fut {
                            continue;
                        } else {
                            panic!("not pending, nothing to output")
                        }
                    }
                }
                State::InputDone => {
                    if self.out.len() == 0 {
                        self.state = State::Done;
                        if self.out_cnt == 0 {
                            let d =
                                items_2::empty::empty_events_dyn_ev(self.ch_conf.scalar_type(), self.ch_conf.shape());
                            match d {
                                Ok(empty) => {
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
