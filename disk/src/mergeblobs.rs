use crate::HasSeenBeforeRangeCount;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::ByteEstimate;
use items::{
    Appendable, LogItem, PushableIndex, RangeCompletableItem, Sitemty, StatsItem, StreamItem, WithLen, WithTimestamps,
};
use netpod::histo::HistoLog2;
use netpod::EventDataReadStats;
use netpod::{log::*, ByteSize};
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

const LOG_EMIT_ITEM: bool = false;

enum MergedCurVal<T> {
    None,
    Finish,
    Val(T),
}

pub struct MergedBlobsStream<S, I>
where
    S: Stream<Item = Sitemty<I>> + Unpin,
    I: Unpin + Appendable + WithTimestamps + PushableIndex + WithLen + ByteEstimate,
{
    inps: Vec<S>,
    current: Vec<MergedCurVal<I>>,
    ixs: Vec<usize>,
    errored: bool,
    completed: bool,
    batch: I,
    ts_last_emit: u64,
    range_complete_observed: Vec<bool>,
    range_complete_observed_all: bool,
    range_complete_observed_all_emitted: bool,
    data_emit_complete: bool,
    batch_size: ByteSize,
    batch_len_emit_histo: HistoLog2,
    logitems: VecDeque<LogItem>,
    event_data_read_stats_items: VecDeque<EventDataReadStats>,
}

// TODO get rid, log info explicitly.
impl<S, I> Drop for MergedBlobsStream<S, I>
where
    S: Stream<Item = Sitemty<I>> + Unpin,
    I: Unpin + Appendable + WithTimestamps + PushableIndex + WithLen + ByteEstimate,
{
    fn drop(&mut self) {
        info!(
            "MergedBlobsStream  Drop Stats:\nbatch_len_emit_histo: {:?}",
            self.batch_len_emit_histo
        );
    }
}

impl<S, I> MergedBlobsStream<S, I>
where
    S: Stream<Item = Sitemty<I>> + Unpin,
    I: Unpin + Appendable + WithTimestamps + PushableIndex + WithLen + ByteEstimate,
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
            batch: I::empty(),
            ts_last_emit: 0,
            range_complete_observed: vec![false; n],
            range_complete_observed_all: false,
            range_complete_observed_all_emitted: false,
            data_emit_complete: false,
            batch_size: ByteSize::kb(128),
            batch_len_emit_histo: HistoLog2::new(0),
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

impl<S, I> Stream for MergedBlobsStream<S, I>
where
    S: Stream<Item = Sitemty<I>> + Unpin,
    I: Unpin + Appendable + WithTimestamps + PushableIndex + WithLen + ByteEstimate,
{
    type Item = Sitemty<I>;

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
                                let emp = I::empty();
                                let ret = std::mem::replace(&mut self.batch, emp);
                                self.batch_len_emit_histo.ingest(ret.len() as u32);
                                self.data_emit_complete = true;
                                if LOG_EMIT_ITEM {
                                    let mut aa = vec![];
                                    for ii in 0..ret.len() {
                                        aa.push(ret.ts(ii));
                                    }
                                    info!("MergedBlobsStream  A emits {} events  tss {:?}", ret.len(), aa);
                                };
                                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(ret)))))
                            } else {
                                self.data_emit_complete = true;
                                continue 'outer;
                            }
                        } else {
                            assert!(lowest_ts >= self.ts_last_emit);
                            let emp = I::empty();
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
                            if self.batch.byte_estimate() >= self.batch_size.bytes() as u64 {
                                trace!("emit item because over threshold  len {}", self.batch.len());
                                let emp = I::empty();
                                let ret = std::mem::replace(&mut self.batch, emp);
                                self.batch_len_emit_histo.ingest(ret.len() as u32);
                                if LOG_EMIT_ITEM {
                                    let mut aa = vec![];
                                    for ii in 0..ret.len() {
                                        aa.push(ret.ts(ii));
                                    }
                                    info!("MergedBlobsStream  B emits {} events  tss {:?}", ret.len(), aa);
                                };
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

impl<S, I> HasSeenBeforeRangeCount for MergedBlobsStream<S, I>
where
    S: Stream<Item = Sitemty<I>> + Unpin,
    I: Unpin + Appendable + WithTimestamps + PushableIndex + WithLen + ByteEstimate,
{
    fn seen_before_range_count(&self) -> usize {
        // TODO (only for debug)
        0
    }
}
