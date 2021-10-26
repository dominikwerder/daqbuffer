use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::{
    inspect_timestamps, Appendable, LogItem, PushableIndex, RangeCompletableItem, Sitemty, StatsItem, StreamItem,
};
use netpod::log::*;
use netpod::{NanoRange, Nanos};
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::eventsitem::EventsItem;

/**
Priority-Merge events from different candidate sources.

Backends like Channel Archiver store the compacted "medium/long-term" data of a channel
in logically unrelated locations on disk with unspecified semantics and without a
common index over "short+medium+long term" data.
In order to deliver data even over the edge of such (possibly overlapping) datasources
without common look tables, the best we can do is fetch data from all sources and
combine them. StorageMerge is doing this combination.
*/
pub struct StorageMerge {
    inps: Vec<Pin<Box<dyn Stream<Item = Sitemty<EventsItem>> + Send>>>,
    names: Vec<String>,
    range: NanoRange,
    completed_inps: Vec<bool>,
    range_complete: Vec<bool>,
    current_inp_item: Vec<Option<EventsItem>>,
    error_items: VecDeque<Error>,
    log_items: VecDeque<LogItem>,
    stats_items: VecDeque<StatsItem>,
    ourname: String,
    inprng: usize,
    data_done: bool,
    done: bool,
    complete: bool,
}

impl StorageMerge {
    pub fn new(
        inps: Vec<Pin<Box<dyn Stream<Item = Sitemty<EventsItem>> + Send>>>,
        names: Vec<String>,
        range: NanoRange,
    ) -> Self {
        assert_eq!(inps.len(), names.len());
        let n = inps.len();
        let mut h = crc32fast::Hasher::new();
        h.update(
            &SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
                .to_le_bytes(),
        );
        let ourname = format!("{:08x}", h.finalize());
        for (i, n) in names.iter().enumerate() {
            debug!("[{}] {} {}", ourname, i, n);
        }
        Self {
            inps,
            names,
            range,
            completed_inps: vec![false; n],
            range_complete: vec![false; n],
            current_inp_item: (0..n).into_iter().map(|_| None).collect(),
            error_items: VecDeque::new(),
            log_items: VecDeque::new(),
            stats_items: VecDeque::new(),
            inprng: n - 1,
            data_done: false,
            done: false,
            complete: false,
            ourname,
        }
    }

    fn refill_if_needed(self: &mut Pin<&mut Self>, cx: &mut Context) -> Result<bool, Error> {
        use Poll::*;
        let mut is_pending = false;
        for i in 0..self.inps.len() {
            while self.current_inp_item[i].is_none() && self.completed_inps[i] == false {
                match self.inps[i].poll_next_unpin(cx) {
                    Ready(j) => match j {
                        Some(j) => match j {
                            Ok(j) => match j {
                                StreamItem::DataItem(j) => match j {
                                    RangeCompletableItem::Data(j) => {
                                        self.current_inp_item[i] = Some(j);
                                    }
                                    RangeCompletableItem::RangeComplete => {
                                        self.range_complete[i] = true;
                                    }
                                },
                                StreamItem::Log(k) => {
                                    self.log_items.push_back(k);
                                }
                                StreamItem::Stats(k) => {
                                    self.stats_items.push_back(k);
                                }
                            },
                            Err(e) => {
                                error!("inp err  input {}  {:?}", i, e);
                                self.error_items.push_back(e);
                            }
                        },
                        None => {
                            self.completed_inps[i] = true;
                        }
                    },
                    Pending => {
                        is_pending = true;
                        break;
                    }
                }
            }
        }
        Ok(is_pending)
    }

    fn decide_next_item(&mut self) -> Result<Option<Sitemty<EventsItem>>, Error> {
        let not_found = 99999;
        let mut i1 = self.inprng;
        let mut j1 = not_found;
        let mut tsmin = u64::MAX;
        let mut tsend = u64::MAX;
        #[allow(unused)]
        use items::{WithLen, WithTimestamps};
        loop {
            if self.completed_inps[i1] {
            } else {
                match self.current_inp_item[i1].as_ref() {
                    None => panic!(),
                    Some(j) => {
                        if j.len() == 0 {
                            j1 = i1;
                            break;
                        } else {
                            let ts1 = j.ts(0);
                            let ts2 = j.ts(j.len() - 1);
                            if ts1 == u64::MAX || ts2 == u64::MAX {
                                panic!();
                            }
                            trace!("[{}] consider  {}  {:?}", self.ourname, i1, Nanos::from_ns(ts1));
                            if ts1 <= tsmin {
                                tsmin = ts1;
                                tsend = ts2;
                                j1 = i1;
                                trace!(
                                    "[{}] switch to source {} / {}  {}",
                                    self.ourname,
                                    i1,
                                    self.inps.len(),
                                    self.names[i1]
                                );
                                self.inprng = i1;
                            } else {
                            }
                        }
                    }
                }
            }
            if i1 == 0 {
                break;
            }
            i1 -= 1;
        }
        let i1 = ();
        let _ = i1;
        if j1 >= not_found {
            Ok(None)
        } else {
            trace!("[{}] decide for source {}", self.ourname, j1);
            trace!("[{}] decided tsmin {:?}", self.ourname, Nanos::from_ns(tsmin));
            trace!("[{}] decided tsend {:?}", self.ourname, Nanos::from_ns(tsend));
            let mut j5 = not_found;
            let mut tsmin2 = u64::MAX;
            if self.inprng > 0 {
                trace!("[{}] locate the next earliest timestamp", self.ourname);
                let mut i5 = self.inprng - 1;
                loop {
                    if self.completed_inps[i5] {
                    } else {
                        let j = self.current_inp_item[i5].as_ref().unwrap();
                        if j.len() != 0 {
                            let ts1 = j.ts(0);
                            if ts1 == u64::MAX {
                                panic!();
                            }
                            trace!(
                                "[{}] consider  {}  {:?}  for next earliest",
                                self.ourname,
                                i5,
                                Nanos::from_ns(ts1)
                            );
                            if ts1 <= tsmin2 {
                                tsmin2 = ts1;
                                j5 = i5;
                            }
                        }
                    }
                    if i5 == 0 {
                        break;
                    }
                    i5 -= 1;
                }
            }
            trace!(
                "[{}] decided tsmin2 {:?}  next earliest timestamp  source {}",
                self.ourname,
                Nanos::from_ns(tsmin2),
                j5
            );
            let item = self.current_inp_item[j1].take().unwrap();
            let item = if j5 != not_found && tsmin2 != u64::MAX {
                if tsend >= tsmin2 {
                    {
                        let tsmin = Nanos::from_ns(tsmin);
                        let tsend = Nanos::from_ns(tsend);
                        let tsmin2 = Nanos::from_ns(tsmin2);
                        trace!(
                            "[{}] NEED TO TRUNCATE THE BLOCK  tsmin {:?}  tsend {:?}  tsmin2 {:?}",
                            self.ourname,
                            tsmin,
                            tsend,
                            tsmin2
                        );
                    }
                    let mut out = item.empty_like_self();
                    for i in 0..item.len() {
                        let ts = item.ts(i);
                        if ts < tsmin2 {
                            out.push_index(&item, i);
                        }
                    }
                    out
                } else {
                    item
                }
            } else {
                item
            };
            trace!("[{}] emit {} events", self.ourname, item.len());
            if false {
                let s = inspect_timestamps(&item, self.range.clone());
                trace!("[{}] timestamps:\n{}", self.ourname, s);
            }
            Ok(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))))
        }
    }
}

impl Stream for StorageMerge {
    type Item = Sitemty<EventsItem>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!()
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if let Some(k) = self.error_items.pop_front() {
                Ready(Some(Err(k)))
            } else if let Some(k) = self.log_items.pop_front() {
                Ready(Some(Ok(StreamItem::Log(k))))
            } else if let Some(k) = self.stats_items.pop_front() {
                Ready(Some(Ok(StreamItem::Stats(k))))
            } else if self.data_done {
                self.done = true;
                continue;
            } else {
                match self.refill_if_needed(cx) {
                    Ok(is_pending) => {
                        if is_pending {
                            if self.log_items.len() == 0 && self.stats_items.len() == 0 {
                                Pending
                            } else {
                                continue;
                            }
                        } else if self.error_items.len() != 0 {
                            continue;
                        } else {
                            match self.decide_next_item() {
                                Ok(Some(j)) => Ready(Some(j)),
                                Ok(None) => {
                                    self.data_done = true;
                                    continue;
                                }
                                Err(e) => {
                                    error!("impl Stream for StorageMerge  {:?}", e);
                                    Ready(Some(Err(e)))
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("{}", e);
                        self.done = true;
                        Ready(Some(Err(e)))
                    }
                }
            };
        }
    }
}
