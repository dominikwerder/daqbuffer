use err::Error;
use futures_util::{Stream, StreamExt};
use items::ByteEstimate;
use items::{Appendable, LogItem, PushableIndex, RangeCompletableItem, Sitemty, StatsItem, StreamItem, WithTimestamps};
use netpod::histo::HistoLog2;
use netpod::log::*;
use netpod::ByteSize;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

// TODO compare with items_2::merger::*

const LOG_EMIT_ITEM: bool = false;

enum MergedCurVal<T> {
    None,
    Finish,
    Val(T),
}

pub struct MergedStream<S, ITY> {
    inps: Vec<S>,
    current: Vec<MergedCurVal<ITY>>,
    ixs: Vec<usize>,
    errored: bool,
    completed: bool,
    batch: Option<ITY>,
    ts_last_emit: u64,
    range_complete_observed: Vec<bool>,
    range_complete_observed_all: bool,
    range_complete_observed_all_emitted: bool,
    data_emit_complete: bool,
    batch_size: ByteSize,
    batch_len_emit_histo: HistoLog2,
    logitems: VecDeque<LogItem>,
    stats_items: VecDeque<StatsItem>,
}

impl<S, ITY> Drop for MergedStream<S, ITY> {
    fn drop(&mut self) {
        // TODO collect somewhere
        debug!(
            "MergedStream  Drop Stats:\nbatch_len_emit_histo: {:?}",
            self.batch_len_emit_histo
        );
    }
}

impl<S, ITY> MergedStream<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Appendable + Unpin,
{
    pub fn new(inps: Vec<S>) -> Self {
        trace!("MergedStream::new");
        let n = inps.len();
        let current = (0..n).into_iter().map(|_| MergedCurVal::None).collect();
        Self {
            inps,
            current: current,
            ixs: vec![0; n],
            errored: false,
            completed: false,
            batch: None,
            ts_last_emit: 0,
            range_complete_observed: vec![false; n],
            range_complete_observed_all: false,
            range_complete_observed_all_emitted: false,
            data_emit_complete: false,
            batch_size: ByteSize::kb(128),
            batch_len_emit_histo: HistoLog2::new(0),
            logitems: VecDeque::new(),
            stats_items: VecDeque::new(),
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
                                    self.stats_items.push_back(item);
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

impl<S, ITY> Stream for MergedStream<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: PushableIndex + Appendable + ByteEstimate + WithTimestamps + Unpin,
{
    type Item = Sitemty<ITY>;

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
            } else if let Some(item) = self.stats_items.pop_front() {
                Ready(Some(Ok(StreamItem::Stats(item))))
            } else if self.range_complete_observed_all_emitted {
                self.completed = true;
                Ready(None)
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
                            if let Some(batch) = self.batch.take() {
                                if batch.len() != 0 {
                                    self.batch_len_emit_histo.ingest(batch.len() as u32);
                                    self.data_emit_complete = true;
                                    if LOG_EMIT_ITEM {
                                        let mut aa = vec![];
                                        for ii in 0..batch.len() {
                                            aa.push(batch.ts(ii));
                                        }
                                        debug!("MergedBlobsStream  A emits {} events  tss {:?}", batch.len(), aa);
                                    };
                                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(batch)))))
                                } else {
                                    self.data_emit_complete = true;
                                    continue 'outer;
                                }
                            } else {
                                self.data_emit_complete = true;
                                continue 'outer;
                            }
                        } else {
                            // TODO unordered cases
                            if lowest_ts < self.ts_last_emit {
                                self.errored = true;
                                let msg = format!(
                                    "unordered event at  lowest_ts {}  ts_last_emit {}",
                                    lowest_ts, self.ts_last_emit
                                );
                                return Ready(Some(Err(Error::with_public_msg(msg))));
                            } else {
                                self.ts_last_emit = self.ts_last_emit.max(lowest_ts);
                            }
                            {
                                let batch = self.batch.take();
                                let rix = self.ixs[lowest_ix];
                                match &self.current[lowest_ix] {
                                    MergedCurVal::Val(val) => {
                                        let mut ldst = batch.unwrap_or_else(|| val.empty_like_self());
                                        if false {
                                            info!(
                                                "Push event  rix {}  lowest_ix {}  lowest_ts {}",
                                                rix, lowest_ix, lowest_ts
                                            );
                                        }
                                        ldst.push_index(val, rix);
                                        self.batch = Some(ldst);
                                    }
                                    MergedCurVal::None => panic!(),
                                    MergedCurVal::Finish => panic!(),
                                }
                            }
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
                            let emit_packet_now = if let Some(batch) = &self.batch {
                                if batch.byte_estimate() >= self.batch_size.bytes() as u64 {
                                    true
                                } else {
                                    false
                                }
                            } else {
                                false
                            };
                            if emit_packet_now {
                                if let Some(batch) = self.batch.take() {
                                    trace!("emit item because over threshold  len {}", batch.len());
                                    self.batch_len_emit_histo.ingest(batch.len() as u32);
                                    if LOG_EMIT_ITEM {
                                        let mut aa = vec![];
                                        for ii in 0..batch.len() {
                                            aa.push(batch.ts(ii));
                                        }
                                        debug!("MergedBlobsStream  B emits {} events  tss {:?}", batch.len(), aa);
                                    };
                                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(batch)))))
                                } else {
                                    continue 'outer;
                                }
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

#[cfg(test)]
mod test {
    use items_2::channelevents::ChannelEvents;
    use items_2::Empty;

    #[test]
    fn merge_channel_events() {
        let mut evs = items_2::eventsdim0::EventsDim0::empty();
        evs.push(1, 100, 17u8);
        evs.push(3, 300, 16);
        let _cevs = ChannelEvents::Events(Box::new(evs));
    }
}
