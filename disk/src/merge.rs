use crate::agg::eventbatch::{MinMaxAvgScalarEventBatch, MinMaxAvgScalarEventBatchStreamItem};
use crate::agg::{Dim1F32Stream, Dim1F32StreamItem, ValuesDim1};
use crate::eventchunker::EventChunkerItem;
use crate::streamlog::LogItem;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

pub struct MergeDim1F32Stream<S> {
    // yields Dim1F32StreamItem
    inps: Vec<Dim1F32Stream<S>>,
    current: Vec<CurVal>,
    ixs: Vec<usize>,
    errored: bool,
    completed: bool,
    batch: ValuesDim1,
}

impl<S> MergeDim1F32Stream<S> {
    pub fn new(inps: Vec<Dim1F32Stream<S>>) -> Self {
        let n = inps.len();
        let mut current = vec![];
        for _ in 0..n {
            current.push(CurVal::None);
        }
        Self {
            inps,
            current: current,
            ixs: vec![0; n],
            batch: ValuesDim1::empty(),
            errored: false,
            completed: false,
        }
    }
}

impl<S> Stream for MergeDim1F32Stream<S>
where
    S: Stream<Item = Result<EventChunkerItem, Error>> + Unpin,
{
    type Item = Result<Dim1F32StreamItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        // TODO  rewrite making the break the default and explicit continue.
        'outer: loop {
            if self.completed {
                panic!("poll on complete stream");
            }
            if self.errored {
                self.completed = true;
                return Ready(None);
            }
            // can only run logic if all streams are either finished, errored or have some current value.
            for i1 in 0..self.inps.len() {
                match self.current[i1] {
                    CurVal::None => {
                        match self.inps[i1].poll_next_unpin(cx) {
                            Ready(Some(Ok(k))) => {
                                // TODO do I keep only the values as "current" or also the other kinds of items?
                                // Can I process the other kinds instantly?
                                match k {
                                    Dim1F32StreamItem::Values(vals) => {
                                        self.current[i1] = CurVal::Val(vals);
                                    }
                                    Dim1F32StreamItem::RangeComplete => {
                                        todo!();
                                    }
                                    Dim1F32StreamItem::EventDataReadStats(_stats) => {
                                        todo!();
                                    }
                                }
                            }
                            Ready(Some(Err(e))) => {
                                self.current[i1] = CurVal::Err(Error::with_msg(format!(
                                    "MergeDim1F32Stream  error from upstream {:?}",
                                    e
                                )));
                                return Ready(Some(Err(e)));
                            }
                            Ready(None) => {
                                self.current[i1] = CurVal::Finish;
                            }
                            Pending => {
                                // TODO is this behaviour correct?
                                return Pending;
                            }
                        }
                    }
                    _ => (),
                }
            }
            let mut lowest_ix = usize::MAX;
            let mut lowest_ts = u64::MAX;
            for i1 in 0..self.inps.len() {
                match &self.current[i1] {
                    CurVal::Finish => {}
                    CurVal::Val(val) => {
                        let u = self.ixs[i1];
                        if u >= val.tss.len() {
                            self.ixs[i1] = 0;
                            self.current[i1] = CurVal::None;
                            continue 'outer;
                        } else {
                            let ts = val.tss[u];
                            if ts < lowest_ts {
                                lowest_ix = i1;
                                lowest_ts = ts;
                            }
                        }
                    }
                    _ => panic!(),
                }
            }
            if lowest_ix == usize::MAX {
                // TODO all inputs in finished state
                break Ready(None);
            } else {
                //trace!("decided on next lowest ts  {}  ix {}", lowest_ts, lowest_ix);
                self.batch.tss.push(lowest_ts);
                let rix = self.ixs[lowest_ix];
                match &mut self.current[lowest_ix] {
                    CurVal::Val(ref mut k) => {
                        let k = std::mem::replace(&mut k.values[rix], vec![]);
                        self.batch.values.push(k);
                    }
                    _ => panic!(),
                }
                self.ixs[lowest_ix] += 1;
            }
            if self.batch.tss.len() >= 64 {
                let k = std::mem::replace(&mut self.batch, ValuesDim1::empty());
                let ret = Dim1F32StreamItem::Values(k);
                break Ready(Some(Ok(ret)));
            }
        }
    }
}

enum CurVal {
    None,
    Finish,
    Err(Error),
    Val(ValuesDim1),
}

pub struct MergedMinMaxAvgScalarStream<S>
where
    S: Stream<Item = Result<MinMaxAvgScalarEventBatchStreamItem, Error>>,
{
    inps: Vec<S>,
    current: Vec<MergedMinMaxAvgScalarStreamCurVal>,
    ixs: Vec<usize>,
    errored: bool,
    completed: bool,
    batch: MinMaxAvgScalarEventBatch,
    ts_last_emit: u64,
    range_complete_observed: Vec<bool>,
    range_complete_observed_all: bool,
    range_complete_observed_all_emitted: bool,
    data_emit_complete: bool,
    batch_size: usize,
    logitems: VecDeque<LogItem>,
}

impl<S> MergedMinMaxAvgScalarStream<S>
where
    S: Stream<Item = Result<MinMaxAvgScalarEventBatchStreamItem, Error>> + Unpin,
{
    pub fn new(inps: Vec<S>) -> Self {
        let n = inps.len();
        let current = (0..n)
            .into_iter()
            .map(|_| MergedMinMaxAvgScalarStreamCurVal::None)
            .collect();
        Self {
            inps,
            current: current,
            ixs: vec![0; n],
            errored: false,
            completed: false,
            batch: MinMaxAvgScalarEventBatch::empty(),
            ts_last_emit: 0,
            range_complete_observed: vec![false; n],
            range_complete_observed_all: false,
            range_complete_observed_all_emitted: false,
            data_emit_complete: false,
            batch_size: 64,
            logitems: VecDeque::new(),
        }
    }

    // This can:
    // Do nothing if all have Val or Finished.
    // But if some is None:
    // We might get some Pending from upstream. In that case, caller also wants to abort here.
    fn replenish(self: &mut Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        use Poll::*;
        let mut pending = 0;
        for i1 in 0..self.inps.len() {
            match self.current[i1] {
                MergedMinMaxAvgScalarStreamCurVal::None => {
                    'l1: loop {
                        break match self.inps[i1].poll_next_unpin(cx) {
                            Ready(Some(Ok(k))) => match k {
                                MinMaxAvgScalarEventBatchStreamItem::Values(vals) => {
                                    self.ixs[i1] = 0;
                                    self.current[i1] = MergedMinMaxAvgScalarStreamCurVal::Val(vals);
                                }
                                MinMaxAvgScalarEventBatchStreamItem::RangeComplete => {
                                    self.range_complete_observed[i1] = true;
                                    let d = self.range_complete_observed.iter().filter(|&&k| k).count();
                                    if d == self.range_complete_observed.len() {
                                        self.range_complete_observed_all = true;
                                        info!("MergedMinMaxAvgScalarStream  range_complete  d  {}  COMPLETE", d);
                                    } else {
                                        trace!("MergedMinMaxAvgScalarStream  range_complete  d  {}", d);
                                    }
                                    continue 'l1;
                                }
                                MinMaxAvgScalarEventBatchStreamItem::Log(item) => {
                                    self.logitems.push_back(item);
                                    continue 'l1;
                                }
                                MinMaxAvgScalarEventBatchStreamItem::EventDataReadStats(_stats) => {
                                    // TODO merge also the stats: either just sum, or sum up by input index.
                                    todo!();
                                }
                            },
                            Ready(Some(Err(e))) => {
                                // TODO emit this error, consider this stream as done, anything more to do here?
                                //self.current[i1] = CurVal::Err(e);
                                self.errored = true;
                                return Ready(Err(e));
                            }
                            Ready(None) => {
                                self.current[i1] = MergedMinMaxAvgScalarStreamCurVal::Finish;
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

impl<S> Stream for MergedMinMaxAvgScalarStream<S>
where
    S: Stream<Item = Result<MinMaxAvgScalarEventBatchStreamItem, Error>> + Unpin,
{
    type Item = Result<MinMaxAvgScalarEventBatchStreamItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("MergedMinMaxAvgScalarStream  poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        if let Some(item) = self.logitems.pop_front() {
            return Ready(Some(Ok(MinMaxAvgScalarEventBatchStreamItem::Log(item))));
        }
        'outer: loop {
            break if self.data_emit_complete {
                if self.range_complete_observed_all {
                    if self.range_complete_observed_all_emitted {
                        self.completed = true;
                        Ready(None)
                    } else {
                        self.range_complete_observed_all_emitted = true;
                        Ready(Some(Ok(MinMaxAvgScalarEventBatchStreamItem::RangeComplete)))
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
                            if let MergedMinMaxAvgScalarStreamCurVal::Val(val) = &self.current[i1] {
                                let u = self.ixs[i1];
                                if u >= val.tss.len() {
                                    self.ixs[i1] = 0;
                                    self.current[i1] = MergedMinMaxAvgScalarStreamCurVal::None;
                                    continue 'outer;
                                } else {
                                    let ts = val.tss[u];
                                    if ts < lowest_ts {
                                        lowest_ix = i1;
                                        lowest_ts = ts;
                                    }
                                }
                            }
                        }
                        if lowest_ix == usize::MAX {
                            if self.batch.tss.len() != 0 {
                                let k = std::mem::replace(&mut self.batch, MinMaxAvgScalarEventBatch::empty());
                                let ret = MinMaxAvgScalarEventBatchStreamItem::Values(k);
                                self.data_emit_complete = true;
                                Ready(Some(Ok(ret)))
                            } else {
                                self.data_emit_complete = true;
                                continue 'outer;
                            }
                        } else {
                            assert!(lowest_ts >= self.ts_last_emit);
                            self.ts_last_emit = lowest_ts;
                            self.batch.tss.push(lowest_ts);
                            let rix = self.ixs[lowest_ix];
                            let z = match &self.current[lowest_ix] {
                                MergedMinMaxAvgScalarStreamCurVal::Val(k) => {
                                    (k.mins[rix], k.maxs[rix], k.avgs[rix], k.tss.len())
                                }
                                _ => panic!(),
                            };
                            self.batch.mins.push(z.0);
                            self.batch.maxs.push(z.1);
                            self.batch.avgs.push(z.2);
                            self.ixs[lowest_ix] += 1;
                            if self.ixs[lowest_ix] >= z.3 {
                                self.ixs[lowest_ix] = 0;
                                self.current[lowest_ix] = MergedMinMaxAvgScalarStreamCurVal::None;
                            }
                            if self.batch.tss.len() >= self.batch_size {
                                let k = std::mem::replace(&mut self.batch, MinMaxAvgScalarEventBatch::empty());
                                let ret = MinMaxAvgScalarEventBatchStreamItem::Values(k);
                                Ready(Some(Ok(ret)))
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

enum MergedMinMaxAvgScalarStreamCurVal {
    None,
    Finish,
    Val(MinMaxAvgScalarEventBatch),
}
