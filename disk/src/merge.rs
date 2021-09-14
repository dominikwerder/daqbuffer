use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::{
    Appendable, EventsNodeProcessor, LogItem, PushableIndex, RangeCompletableItem, Sitemty, StatsItem, StreamItem,
    WithLen, WithTimestamps,
};
use netpod::log::*;
use netpod::EventDataReadStats;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

pub mod mergedblobsfromremotes;
pub mod mergedfromremotes;

enum MergedCurVal<T> {
    None,
    Finish,
    Val(T),
}

pub struct MergedStream<S, ENP>
where
    S: Stream<Item = Sitemty<<ENP as EventsNodeProcessor>::Output>>,
    ENP: EventsNodeProcessor,
{
    inps: Vec<S>,
    current: Vec<MergedCurVal<<ENP as EventsNodeProcessor>::Output>>,
    ixs: Vec<usize>,
    errored: bool,
    completed: bool,
    batch: <ENP as EventsNodeProcessor>::Output,
    ts_last_emit: u64,
    range_complete_observed: Vec<bool>,
    range_complete_observed_all: bool,
    range_complete_observed_all_emitted: bool,
    data_emit_complete: bool,
    batch_size: usize,
    logitems: VecDeque<LogItem>,
    event_data_read_stats_items: VecDeque<EventDataReadStats>,
}

impl<S, ENP> MergedStream<S, ENP>
where
    S: Stream<Item = Sitemty<<ENP as EventsNodeProcessor>::Output>> + Unpin,
    ENP: EventsNodeProcessor,
    <ENP as EventsNodeProcessor>::Output: Appendable,
{
    pub fn new(inps: Vec<S>) -> Self {
        let n = inps.len();
        let current = (0..n).into_iter().map(|_| MergedCurVal::None).collect();
        Self {
            inps,
            current: current,
            ixs: vec![0; n],
            errored: false,
            completed: false,
            batch: <<ENP as EventsNodeProcessor>::Output as Appendable>::empty(),
            ts_last_emit: 0,
            range_complete_observed: vec![false; n],
            range_complete_observed_all: false,
            range_complete_observed_all_emitted: false,
            data_emit_complete: false,
            batch_size: 1,
            logitems: VecDeque::new(),
            event_data_read_stats_items: VecDeque::new(),
        }
    }

    fn replenish(self: &mut Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        use Poll::*;
        let mut pending = 0;
        for i1 in 0..self.inps.len() {
            match self.current[i1] {
                MergedCurVal::None => {
                    'l1: loop {
                        break match self.inps[i1].poll_next_unpin(cx) {
                            Ready(Some(Ok(k))) => match k {
                                StreamItem::Log(item) => {
                                    self.logitems.push_back(item);
                                    continue 'l1;
                                }
                                StreamItem::Stats(item) => {
                                    match item {
                                        StatsItem::EventDataReadStats(item) => {
                                            self.event_data_read_stats_items.push_back(item);
                                        }
                                    }
                                    continue 'l1;
                                }
                                StreamItem::DataItem(item) => match item {
                                    RangeCompletableItem::RangeComplete => {
                                        self.range_complete_observed[i1] = true;
                                        let d = self.range_complete_observed.iter().filter(|&&k| k).count();
                                        if d == self.range_complete_observed.len() {
                                            self.range_complete_observed_all = true;
                                            debug!("MergedStream  range_complete  d  {}  COMPLETE", d);
                                        } else {
                                            trace!("MergedStream  range_complete  d  {}", d);
                                        }
                                        continue 'l1;
                                    }
                                    RangeCompletableItem::Data(item) => {
                                        self.ixs[i1] = 0;
                                        self.current[i1] = MergedCurVal::Val(item);
                                    }
                                },
                            },
                            Ready(Some(Err(e))) => {
                                // TODO emit this error, consider this stream as done, anything more to do here?
                                //self.current[i1] = CurVal::Err(e);
                                self.errored = true;
                                return Ready(Err(e));
                            }
                            Ready(None) => {
                                self.current[i1] = MergedCurVal::Finish;
                            }
                            Pending => {
                                pending += 1;
                            }
                        };
                    }
                }
                _ => (),
            }
        }
        if pending > 0 {
            Pending
        } else {
            Ready(Ok(()))
        }
    }
}

impl<S, ENP> Stream for MergedStream<S, ENP>
where
    S: Stream<Item = Sitemty<<ENP as EventsNodeProcessor>::Output>> + Unpin,
    ENP: EventsNodeProcessor,
    <ENP as EventsNodeProcessor>::Output: PushableIndex + Appendable,
{
    type Item = Sitemty<<ENP as EventsNodeProcessor>::Output>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("poll_next on completed");
            } else if self.errored {
                self.completed = true;
                Ready(None)
            } else if let Some(item) = self.logitems.pop_front() {
                Ready(Some(Ok(StreamItem::Log(item))))
            } else if let Some(item) = self.event_data_read_stats_items.pop_front() {
                Ready(Some(Ok(StreamItem::Stats(StatsItem::EventDataReadStats(item)))))
            } else if self.data_emit_complete {
                if self.range_complete_observed_all {
                    if self.range_complete_observed_all_emitted {
                        self.completed = true;
                        Ready(None)
                    } else {
                        self.range_complete_observed_all_emitted = true;
                        Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                    }
                } else {
                    self.completed = true;
                    Ready(None)
                }
            } else {
                // Can only run logic if all streams are either finished, errored or have some current value.
                match self.replenish(cx) {
                    Ready(Ok(_)) => {
                        let mut lowest_ix = usize::MAX;
                        let mut lowest_ts = u64::MAX;
                        for i1 in 0..self.inps.len() {
                            if let MergedCurVal::Val(val) = &self.current[i1] {
                                let u = self.ixs[i1];
                                if u >= val.len() {
                                    self.ixs[i1] = 0;
                                    self.current[i1] = MergedCurVal::None;
                                    continue 'outer;
                                } else {
                                    let ts = val.ts(u);
                                    if ts < lowest_ts {
                                        lowest_ix = i1;
                                        lowest_ts = ts;
                                    }
                                }
                            }
                        }
                        if lowest_ix == usize::MAX {
                            if self.batch.len() != 0 {
                                let emp = <<ENP as EventsNodeProcessor>::Output>::empty();
                                let ret = std::mem::replace(&mut self.batch, emp);
                                self.data_emit_complete = true;
                                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(ret)))))
                            } else {
                                self.data_emit_complete = true;
                                continue 'outer;
                            }
                        } else {
                            assert!(lowest_ts >= self.ts_last_emit);
                            let emp = <<ENP as EventsNodeProcessor>::Output>::empty();
                            let mut local_batch = std::mem::replace(&mut self.batch, emp);
                            self.ts_last_emit = lowest_ts;
                            let rix = self.ixs[lowest_ix];
                            match &self.current[lowest_ix] {
                                MergedCurVal::Val(val) => {
                                    local_batch.push_index(val, rix);
                                }
                                MergedCurVal::None => panic!(),
                                MergedCurVal::Finish => panic!(),
                            }
                            self.batch = local_batch;
                            self.ixs[lowest_ix] += 1;
                            let curlen = match &self.current[lowest_ix] {
                                MergedCurVal::Val(val) => val.len(),
                                MergedCurVal::None => panic!(),
                                MergedCurVal::Finish => panic!(),
                            };
                            if self.ixs[lowest_ix] >= curlen {
                                self.ixs[lowest_ix] = 0;
                                self.current[lowest_ix] = MergedCurVal::None;
                            }
                            if self.batch.len() >= self.batch_size {
                                let emp = <<ENP as EventsNodeProcessor>::Output>::empty();
                                let ret = std::mem::replace(&mut self.batch, emp);
                                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(ret)))))
                            } else {
                                continue 'outer;
                            }
                        }
                    }
                    Ready(Err(e)) => {
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                }
            };
        }
    }
}
