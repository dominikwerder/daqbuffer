use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StatsItem;
use items_0::streamitem::StreamItem;
use items_0::MergeError;
use items_2::merger::Mergeable;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::RangeFilterStats;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct RangeFilter2<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Mergeable,
{
    inp: S,
    range: NanoRange,
    range_str: String,
    one_before_range: bool,
    stats: RangeFilterStats,
    slot1: Option<ITY>,
    slot2: Option<ITY>,
    have_range_complete: bool,
    data_done: bool,
    raco_done: bool,
    done: bool,
    complete: bool,
}

impl<S, ITY> RangeFilter2<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Mergeable,
{
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(inp: S, range: NanoRange, one_before_range: bool) -> Self {
        trace!(
            "{}::new  range: {:?}  one_before_range {:?}",
            Self::type_name(),
            range,
            one_before_range
        );
        Self {
            inp,
            range_str: format!("{:?}", range),
            range,
            one_before_range,
            stats: RangeFilterStats::new(),
            slot1: None,
            slot2: None,
            have_range_complete: false,
            data_done: false,
            raco_done: false,
            done: false,
            complete: false,
        }
    }

    fn prune_high(&mut self, mut item: ITY, ts: u64) -> Result<ITY, Error> {
        let ret = match item.find_highest_index_lt(ts) {
            Some(ihlt) => {
                let n = item.len();
                if ihlt + 1 == n {
                    // TODO gather stats, this should be the most common case.
                    self.stats.items_no_prune_high += 1;
                    item
                } else {
                    self.stats.items_part_prune_high += 1;
                    let mut dummy = item.new_empty();
                    match item.drain_into(&mut dummy, (ihlt + 1, n)) {
                        Ok(_) => {}
                        Err(e) => match e {
                            MergeError::NotCompatible => {
                                error!("logic error")
                            }
                            MergeError::Full => error!("full, logic error"),
                        },
                    }
                    item
                }
            }
            None => {
                self.stats.items_all_prune_high += 1;
                item.new_empty()
            }
        };
        Ok(ret)
    }

    fn handle_item(&mut self, item: ITY) -> Result<ITY, Error> {
        let mut item = self.prune_high(item, self.range.end)?;
        let ret = if self.one_before_range {
            match item.find_lowest_index_ge(self.range.beg) {
                Some(ilge) => {
                    if ilge == 0 {
                        if let Some(sl1) = self.slot1.take() {
                            self.slot2 = Some(item);
                            sl1
                        } else {
                            item
                        }
                    } else {
                        let mut dummy = item.new_empty();
                        item.drain_into(&mut dummy, (0, ilge - 1))
                            .map_err(|e| format!("{e} unexpected MergeError while remove of items"))?;
                        self.slot1 = None;
                        item
                    }
                }
                None => {
                    // TODO keep stats about this case
                    debug!("drain into to keep one before");
                    let n = item.len();
                    let mut keep = item.new_empty();
                    item.drain_into(&mut keep, (n.max(1) - 1, n))
                        .map_err(|e| format!("{e} unexpected MergeError while remove of items"))?;
                    self.slot1 = Some(keep);
                    item.new_empty()
                }
            }
        } else {
            match item.find_lowest_index_ge(self.range.beg) {
                Some(ilge) => {
                    let mut dummy = item.new_empty();
                    item.drain_into(&mut dummy, (0, ilge))?;
                    item
                }
                None => {
                    // TODO count case for stats
                    item.new_empty()
                }
            }
        };
        Ok(ret)
    }
}

impl<S, ITY> RangeFilter2<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Mergeable,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<<Self as Stream>::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                error!("{} poll_next on complete", Self::type_name());
                Ready(Some(Err(Error::with_msg_no_trace(format!(
                    "{} poll_next on complete",
                    Self::type_name()
                )))))
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if self.raco_done {
                self.done = true;
                let k = std::mem::replace(&mut self.stats, RangeFilterStats::new());
                let k = StatsItem::RangeFilterStats(k);
                Ready(Some(Ok(StreamItem::Stats(k))))
            } else if self.data_done {
                self.raco_done = true;
                if self.have_range_complete {
                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                } else {
                    continue;
                }
            } else if let Some(sl2) = self.slot2.take() {
                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(sl2)))))
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(item)) => match item {
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))) => match self.handle_item(item) {
                            Ok(item) => Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))))),
                            Err(e) => {
                                error!("sees: {e}");
                                self.data_done = true;
                                Ready(Some(Err(e)))
                            }
                        },
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)) => {
                            self.have_range_complete = true;
                            continue;
                        }
                        k => Ready(Some(k)),
                    },
                    Ready(None) => {
                        self.data_done = true;
                        continue;
                    }
                    Pending => Pending,
                }
            };
        }
    }
}

impl<S, ITY> Stream for RangeFilter2<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Mergeable,
{
    type Item = Sitemty<ITY>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let span1 = span!(Level::INFO, "RangeFilter2", range = tracing::field::Empty);
        span1.record("range", &self.range_str.as_str());
        let _spg = span1.enter();
        Self::poll_next(self, cx)
    }
}

impl<S, ITY> fmt::Debug for RangeFilter2<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Mergeable,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RangeFilter2").field("stats", &self.stats).finish()
    }
}

impl<S, ITY> Drop for RangeFilter2<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Mergeable,
{
    fn drop(&mut self) {
        // Self::type_name()
        debug!("drop {:?}", self);
    }
}
