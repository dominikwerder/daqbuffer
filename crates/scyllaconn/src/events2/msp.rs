use super::prepare::StmtsEvents;
use crate::range::ScyllaSeriesRange;
use crate::worker::ScyllaQueue;
use core::fmt;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::ttl::RetentionTime;
use netpod::TsMs;
use netpod::TsMsVecFmt;
use scylla::Session;
use series::SeriesId;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[allow(unused)]
macro_rules! trace_emit {
    ($det:expr, $($arg:tt)*) => {
        if $det {
            trace!($($arg)*);
        }
    };
}

#[derive(Debug, ThisError)]
#[cstm(name = "EventsMsp")]
pub enum Error {
    Logic,
    Worker(Box<crate::worker::Error>),
    ScyllaQuery(#[from] scylla::transport::errors::QueryError),
    ScyllaRow(#[from] scylla::transport::iterator::NextRowError),
}

impl From<crate::worker::Error> for Error {
    fn from(value: crate::worker::Error) -> Self {
        Self::Worker(Box::new(value))
    }
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

    fn is_taken(&self) -> bool {
        if let Self::Taken = self {
            true
        } else {
            false
        }
    }
}

struct BckAndFirstFwd {
    fut_bck: Resolvable<Pin<Box<dyn Future<Output = Result<VecDeque<TsMs>, crate::worker::Error>> + Send>>>,
    fut_fwd: Resolvable<Pin<Box<dyn Future<Output = Result<VecDeque<TsMs>, crate::worker::Error>> + Send>>>,
}

struct Fwd {
    fut_fwd: Resolvable<Pin<Box<dyn Future<Output = Result<VecDeque<TsMs>, crate::worker::Error>> + Send>>>,
}

enum State {
    BckAndFirstFwd(BckAndFirstFwd),
    Fwd(Fwd),
}

#[pin_project::pin_project]
pub struct MspStreamRt {
    rt: RetentionTime,
    series: SeriesId,
    range: ScyllaSeriesRange,
    no_more: bool,
    #[pin]
    state: State,
    out: VecDeque<TsMs>,
    scyqueue: ScyllaQueue,
    do_trace_detail: bool,
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
        let do_trace_detail = netpod::TRACE_SERIES_ID.contains(&series.id());
        trace_emit!(do_trace_detail, "-------------------------------------  TEST TRACE");
        Self {
            rt,
            series,
            range,
            no_more: false,
            state: State::BckAndFirstFwd(BckAndFirstFwd {
                fut_bck: Resolvable::Future(Box::pin(fut_bck)),
                fut_fwd: Resolvable::Future(Box::pin(fut_fwd)),
            }),
            out: VecDeque::new(),
            scyqueue,
            do_trace_detail,
        }
    }

    fn next_fwd_fut(
        &mut self,
    ) -> Resolvable<Pin<Box<dyn Future<Output = Result<VecDeque<TsMs>, crate::worker::Error>> + Send>>> {
        let range = if let Some(msp) = self.out.back() {
            let x = ScyllaSeriesRange::new(msp.bump_epsilon().ns(), self.range.end());
            trace_emit!(self.do_trace_detail, "next_fwd_fut  {}", x.fmt());
            x
        } else {
            // should not get here
            let x = ScyllaSeriesRange::new(self.range.end(), self.range.end());
            trace_emit!(self.do_trace_detail, "next_fwd_fut  NOTHING IN BUFFER  {}", x.fmt());
            x
        };
        let fut_fwd = {
            let scyqueue = self.scyqueue.clone();
            let rt = self.rt.clone();
            let series = self.series.clone();
            async move { scyqueue.find_ts_msp(rt, series.id(), range, false).await }
        };
        Resolvable::Future(Box::pin(fut_fwd))
    }
}

impl Stream for MspStreamRt {
    type Item = Result<TsMs, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let trdet = self.do_trace_detail;
        loop {
            break match &mut self.state {
                State::BckAndFirstFwd(st) => {
                    let mut have_pending = false;
                    let rsv = &mut st.fut_bck;
                    match rsv {
                        Resolvable::Future(fut) => match fut.poll_unpin(cx) {
                            Ready(x) => {
                                trace_emit!(trdet, "bck resolved {x:?}");
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
                                trace_emit!(trdet, "fwd resolved {x:?}");
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
                                        trace_emit!(trdet, "ready out {}", TsMsVecFmt(self.out.iter()));
                                        self.state = State::Fwd(Fwd {
                                            fut_fwd: self.next_fwd_fut(),
                                        });
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
                State::Fwd(st) => {
                    // TODO check if more input is coming
                    let mut have_pending = false;
                    let mut have_progress = false;
                    let rsv = &mut st.fut_fwd;
                    match rsv {
                        Resolvable::Future(fut) => match fut.poll_unpin(cx) {
                            Ready(Ok(x)) => {
                                *rsv = Resolvable::Taken;
                                trace_emit!(trdet, "bulk fwd resolved {x:?}");
                                if x.len() == 0 {
                                    self.no_more = true;
                                }
                                for e in x {
                                    self.out.push_back(e)
                                }
                            }
                            Ready(Err(e)) => {
                                trace_emit!(trdet, "bulk fwd error {e}");
                                *rsv = Resolvable::Taken;
                                error!("{e}");
                            }
                            Pending => {
                                trace_emit!(trdet, "bulk fwd Pending");
                                have_pending = true;
                            }
                        },
                        Resolvable::Output(..) => {
                            trace_emit!(trdet, "bulk fwd Output");
                        }
                        Resolvable::Taken => {
                            trace_emit!(trdet, "bulk fwd Taken");
                        }
                    }
                    let is_taken = if let State::Fwd(st2) = &self.state {
                        st2.fut_fwd.is_taken()
                    } else {
                        panic!("logic");
                    };
                    if self.out.len() < 3 && self.no_more == false && is_taken {
                        trace_emit!(trdet, "bulk fwd  low in out  make next fut   ++++++++++++++++++++");
                        self.state = State::Fwd(Fwd {
                            fut_fwd: self.next_fwd_fut(),
                        });
                        have_progress = true;
                    }
                    if let Some(x) = self.out.pop_front() {
                        trace_emit!(trdet, "State::Fwd => Some  emit {}", x.fmt());
                        Ready(Some(Ok(x)))
                    } else if have_progress {
                        continue;
                    } else if have_pending {
                        trace_emit!(trdet, "State::Fwd => Pending      !!!!!!!!!!!!!!!!!!!!!!!");
                        Pending
                    } else {
                        trace_emit!(trdet, "State::Fwd => None       !!!!!!!!!!!!!!!!!!!!!!");
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

pub async fn find_ts_msp(
    rt: &RetentionTime,
    series: u64,
    range: ScyllaSeriesRange,
    bck: bool,
    stmts: &StmtsEvents,
    scy: &Session,
) -> Result<VecDeque<TsMs>, Error> {
    trace!(
        "find_ts_msp  series  {:?}  {:?}  {}  bck {}",
        rt,
        series,
        range.fmt(),
        bck
    );
    if bck {
        find_ts_msp_bck(rt, series, range, stmts, scy).await
    } else {
        find_ts_msp_fwd(rt, series, range, stmts, scy).await
    }
}

async fn find_ts_msp_fwd(
    rt: &RetentionTime,
    series: u64,
    range: ScyllaSeriesRange,
    stmts: &StmtsEvents,
    scy: &Session,
) -> Result<VecDeque<TsMs>, Error> {
    let mut ret = VecDeque::new();
    // TODO time range truncation can be handled better
    let params = (series as i64, range.beg().ms() as i64, 1 + range.end().ms() as i64);
    let mut res = scy
        .execute_iter(stmts.rt(rt).ts_msp_fwd().clone(), params)
        .await?
        .into_typed::<(i64,)>();
    while let Some(x) = res.next().await {
        let row = x?;
        let ts = TsMs::from_ms_u64(row.0 as u64);
        ret.push_back(ts);
    }
    Ok(ret)
}

async fn find_ts_msp_bck(
    rt: &RetentionTime,
    series: u64,
    range: ScyllaSeriesRange,
    stmts: &StmtsEvents,
    scy: &Session,
) -> Result<VecDeque<TsMs>, Error> {
    let mut ret = VecDeque::new();
    let params = (series as i64, range.beg().ms() as i64);
    let mut res = scy
        .execute_iter(stmts.rt(rt).ts_msp_bck().clone(), params)
        .await?
        .into_typed::<(i64,)>();
    while let Some(x) = res.next().await {
        let row = x?;
        let ts = TsMs::from_ms_u64(row.0 as u64);
        ret.push_front(ts);
    }
    Ok(ret)
}
