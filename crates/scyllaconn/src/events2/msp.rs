use crate::range::ScyllaSeriesRange;
use crate::worker::ScyllaQueue;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use netpod::ttl::RetentionTime;
use netpod::TsMs;
use series::SeriesId;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug, ThisError)]
pub enum Error {
    Worker(#[from] crate::worker::Error),
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
    fn take(&mut self) -> Option<<F as Future>::Output> {
        let x = std::mem::replace(self, Resolvable::Taken);
        match x {
            Resolvable::Future(_) => None,
            Resolvable::Output(x) => Some(x),
            Resolvable::Taken => None,
        }
    }
}

struct BckAndFirstFwd {
    scyqueue: ScyllaQueue,
    fut_bck: Resolvable<Pin<Box<dyn Future<Output = Result<VecDeque<TsMs>, crate::worker::Error>> + Send>>>,
    fut_fwd: Resolvable<Pin<Box<dyn Future<Output = Result<VecDeque<TsMs>, crate::worker::Error>> + Send>>>,
}

enum State {
    BckAndFirstFwd(BckAndFirstFwd),
    InputDone,
}

#[pin_project::pin_project]
pub struct MspStreamRt {
    rt: RetentionTime,
    series: SeriesId,
    range: ScyllaSeriesRange,
    #[pin]
    state: State,
    out: VecDeque<TsMs>,
}

impl MspStreamRt {
    pub fn new(rt: RetentionTime, series: SeriesId, range: ScyllaSeriesRange, scyqueue: ScyllaQueue) -> Self {
        let fut_bck = {
            let scyqueue = scyqueue.clone();
            let rt = rt.clone();
            let series = series.clone();
            let range = range.clone();
            async move { scyqueue.find_ts_msp(rt, series.id(), range, true).await }
        };
        let fut_fwd = {
            let scyqueue = scyqueue.clone();
            let rt = rt.clone();
            let series = series.clone();
            let range = range.clone();
            async move { scyqueue.find_ts_msp(rt, series.id(), range, false).await }
        };
        Self {
            rt,
            series,
            range,
            state: State::BckAndFirstFwd(BckAndFirstFwd {
                scyqueue,
                fut_bck: Resolvable::Future(Box::pin(fut_bck)),
                fut_fwd: Resolvable::Future(Box::pin(fut_fwd)),
            }),
            out: VecDeque::new(),
        }
    }
}

impl Stream for MspStreamRt {
    type Item = Result<TsMs, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match &mut self.state {
                State::BckAndFirstFwd(st) => {
                    let mut have_pending = false;
                    let rsv = &mut st.fut_bck;
                    match rsv {
                        Resolvable::Future(fut) => match fut.poll_unpin(cx) {
                            Ready(x) => {
                                *rsv = Resolvable::Output(x);
                            }
                            Pending => {
                                have_pending = true;
                            }
                        },
                        _ => {}
                    }
                    let rsv = &mut st.fut_fwd;
                    match rsv {
                        Resolvable::Future(fut) => match fut.poll_unpin(cx) {
                            Ready(x) => {
                                *rsv = Resolvable::Output(x);
                            }
                            Pending => {
                                have_pending = true;
                            }
                        },
                        _ => {}
                    }
                    if have_pending {
                        Pending
                    } else {
                        let taken_bck = st.fut_bck.take();
                        let taken_fwd = st.fut_fwd.take();
                        self.state = State::InputDone;
                        if let (Some(taken_bck), Some(taken_fwd)) = (taken_bck, taken_fwd) {
                            match taken_bck {
                                Ok(v1) => match taken_fwd {
                                    Ok(v2) => {
                                        for e in v1 {
                                            self.out.push_back(e)
                                        }
                                        for e in v2 {
                                            self.out.push_back(e)
                                        }
                                        continue;
                                    }
                                    Err(e) => Ready(Some(Err(e.into()))),
                                },
                                Err(e) => Ready(Some(Err(e.into()))),
                            }
                        } else {
                            Ready(Some(Err(Error::Logic)))
                        }
                    }
                }
                State::InputDone => {
                    if let Some(x) = self.out.pop_front() {
                        Ready(Some(Ok(x)))
                    } else {
                        Ready(None)
                    }
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
    let x: MspStreamRt = phantomval();
    trait_assert(x);
}

fn phantomval<T>() -> T {
    panic!()
}
