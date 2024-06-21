use super::msp::MspStreamRt;
use crate::events::read_next_values;
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
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use series::SeriesId;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

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

#[derive(Debug, ThisError)]
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
}

struct FetchMsp {
    fut: Pin<Box<dyn Future<Output = Option<Result<TsMs, crate::events2::msp::Error>>> + Send>>,
}

struct FetchEvents {
    fut: Pin<Box<dyn Future<Output = Result<Box<dyn Events>, crate::events2::events::Error>> + Send>>,
}

enum ReadingState {
    FetchMsp(FetchMsp),
    FetchEvents(FetchEvents),
}

struct Reading {
    scyqueue: ScyllaQueue,
    reading_state: ReadingState,
}

enum State {
    Begin,
    Reading(Reading),
    InputDone,
    Done,
}

pub struct EventsStreamRt {
    rt: RetentionTime,
    series: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
    range: ScyllaSeriesRange,
    with_values: bool,
    state: State,
    scyqueue: ScyllaQueue,
    msp_inp: MspStreamRt,
    out: VecDeque<Box<dyn Events>>,
    ts_seen_max: u64,
}

impl EventsStreamRt {
    pub fn new(
        rt: RetentionTime,
        series: SeriesId,
        scalar_type: ScalarType,
        shape: Shape,
        range: ScyllaSeriesRange,
        with_values: bool,
        scyqueue: ScyllaQueue,
    ) -> Self {
        debug!("EventsStreamRt::new  {series:?}  {range:?}  {rt:?}");
        let msp_inp =
            crate::events2::msp::MspStreamRt::new(rt.clone(), series.clone(), range.clone(), scyqueue.clone());
        Self {
            rt,
            series,
            scalar_type,
            shape,
            range,
            with_values,
            state: State::Begin,
            scyqueue,
            msp_inp,
            out: VecDeque::new(),
            ts_seen_max: 0,
        }
    }

    fn __handle_reading(self: Pin<&mut Self>, st: &mut Reading, cx: &mut Context) -> Result<(), Error> {
        let _ = st;
        let _ = cx;
        todo!()
    }

    fn make_read_events_fut(
        &mut self,
        ts_msp: TsMs,
        scyqueue: ScyllaQueue,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn Events>, Error>> + Send>> {
        let fwd = true;
        let opts = ReadNextValuesOpts::new(
            self.rt.clone(),
            self.series.clone(),
            ts_msp,
            self.range.clone(),
            fwd,
            self.with_values,
            scyqueue,
        );
        let scalar_type = self.scalar_type.clone();
        let shape = self.shape.clone();
        let fut = async move {
            let ret = match &shape {
                Shape::Scalar => match &scalar_type {
                    ScalarType::U8 => read_next_values::<u8>(opts).await,
                    ScalarType::U16 => read_next_values::<u16>(opts).await,
                    ScalarType::U32 => read_next_values::<u32>(opts).await,
                    ScalarType::U64 => read_next_values::<u64>(opts).await,
                    ScalarType::I8 => read_next_values::<i8>(opts).await,
                    ScalarType::I16 => read_next_values::<i16>(opts).await,
                    ScalarType::I32 => read_next_values::<i32>(opts).await,
                    ScalarType::I64 => read_next_values::<i64>(opts).await,
                    ScalarType::F32 => read_next_values::<f32>(opts).await,
                    ScalarType::F64 => read_next_values::<f64>(opts).await,
                    ScalarType::BOOL => read_next_values::<bool>(opts).await,
                    ScalarType::STRING => read_next_values::<String>(opts).await,
                    ScalarType::Enum => read_next_values::<String>(opts).await,
                    ScalarType::ChannelStatus => {
                        warn!("read scalar channel status not yet supported");
                        err::todoval()
                    }
                },
                Shape::Wave(_) => match &scalar_type {
                    ScalarType::U8 => read_next_values::<Vec<u8>>(opts).await,
                    ScalarType::U16 => read_next_values::<Vec<u16>>(opts).await,
                    ScalarType::U32 => read_next_values::<Vec<u32>>(opts).await,
                    ScalarType::U64 => read_next_values::<Vec<u64>>(opts).await,
                    ScalarType::I8 => read_next_values::<Vec<i8>>(opts).await,
                    ScalarType::I16 => read_next_values::<Vec<i16>>(opts).await,
                    ScalarType::I32 => read_next_values::<Vec<i32>>(opts).await,
                    ScalarType::I64 => read_next_values::<Vec<i64>>(opts).await,
                    ScalarType::F32 => read_next_values::<Vec<f32>>(opts).await,
                    ScalarType::F64 => read_next_values::<Vec<f64>>(opts).await,
                    ScalarType::BOOL => read_next_values::<Vec<bool>>(opts).await,
                    ScalarType::STRING => {
                        warn!("read array string not yet supported");
                        err::todoval()
                    }
                    ScalarType::Enum => read_next_values::<Vec<String>>(opts).await,
                    ScalarType::ChannelStatus => {
                        warn!("read array channel status not yet supported");
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
                    if item_min < self.range.beg().ns() {
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
                            Some(ix) => {
                                match items_2::merger::Mergeable::drain_into(&mut item, &mut r, (0, ix)) {
                                    Ok(()) => {}
                                    Err(e) => {
                                        self.state = State::Done;
                                        break Ready(Some(Err(e.into())));
                                    }
                                }
                                // self.state = State::Done;
                                // break Ready(Some(Err(Error::Unordered)));
                            }
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
                break Ready(Some(Ok(ChannelEvents::Events(item))));
            }
            break match &mut self.state {
                State::Begin => {
                    let msp_inp = unsafe {
                        let ptr = (&mut self.msp_inp) as *mut MspStreamRt;
                        &mut *ptr
                    };
                    let fut = Box::pin(msp_inp.next());
                    self.state = State::Reading(Reading {
                        scyqueue: self.scyqueue.clone(),
                        reading_state: ReadingState::FetchMsp(FetchMsp { fut }),
                    });
                    continue;
                }
                State::Reading(st) => match &mut st.reading_state {
                    ReadingState::FetchMsp(st2) => match st2.fut.poll_unpin(cx) {
                        Ready(Some(Ok(ts))) => {
                            let scyqueue = st.scyqueue.clone();
                            let fut = self.make_read_events_fut(ts, scyqueue);
                            if let State::Reading(st) = &mut self.state {
                                st.reading_state = ReadingState::FetchEvents(FetchEvents { fut });
                                continue;
                            } else {
                                self.state = State::Done;
                                Ready(Some(Err(Error::Logic)))
                            }
                        }
                        Ready(Some(Err(e))) => Ready(Some(Err(e.into()))),
                        Ready(None) => {
                            self.state = State::InputDone;
                            continue;
                        }
                        Pending => Pending,
                    },
                    ReadingState::FetchEvents(st2) => match st2.fut.poll_unpin(cx) {
                        Ready(Ok(x)) => {
                            self.out.push_back(x);
                            let msp_inp = unsafe {
                                let ptr = (&mut self.msp_inp) as *mut MspStreamRt;
                                &mut *ptr
                            };
                            let fut = Box::pin(msp_inp.next());
                            if let State::Reading(st) = &mut self.state {
                                st.reading_state = ReadingState::FetchMsp(FetchMsp { fut });
                                continue;
                            } else {
                                self.state = State::Done;
                                Ready(Some(Err(Error::Logic)))
                            }
                        }
                        Ready(Err(e)) => {
                            self.state = State::Done;
                            Ready(Some(Err(e.into())))
                        }
                        Pending => Pending,
                    },
                },
                State::InputDone => {
                    if self.out.len() == 0 {
                        Ready(None)
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
