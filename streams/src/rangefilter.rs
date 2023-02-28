use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items::Appendable;
use items::Clearable;
use items::PushableIndex;
use items::WithTimestamps;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StatsItem;
use items_0::streamitem::StreamItem;
use netpod::log::*;
use netpod::NanoRange;
use netpod::Nanos;
use netpod::RangeFilterStats;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct RangeFilter<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: WithTimestamps + PushableIndex + Appendable + Clearable + Unpin,
{
    inp: S,
    range: NanoRange,
    range_str: String,
    expand: bool,
    ts_max: u64,
    stats: RangeFilterStats,
    prerange: Option<ITY>,
    have_pre: bool,
    have_range_complete: bool,
    emitted_post: bool,
    data_done: bool,
    raco_done: bool,
    done: bool,
    complete: bool,
    items_with_pre: usize,
    items_with_post: usize,
    items_with_unordered: usize,
}

impl<S, ITY> RangeFilter<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: WithTimestamps + PushableIndex + Appendable + Clearable + Unpin,
{
    pub fn new(inp: S, range: NanoRange, expand: bool) -> Self {
        trace!("RangeFilter::new  range: {:?}  expand: {:?}", range, expand);
        Self {
            inp,
            range_str: format!("{:?}", range),
            range,
            expand,
            ts_max: 0,
            stats: RangeFilterStats::new(),
            prerange: None,
            have_pre: false,
            have_range_complete: false,
            emitted_post: false,
            data_done: false,
            raco_done: false,
            done: false,
            complete: false,
            items_with_pre: 0,
            items_with_post: 0,
            items_with_unordered: 0,
        }
    }
}

impl<S, ITY> RangeFilter<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: WithTimestamps + PushableIndex + Appendable + Clearable + Unpin,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<<Self as Stream>::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll_next on complete");
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
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(item)) => match item {
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))) => {
                            let mut contains_pre = false;
                            let mut contains_post = false;
                            let mut contains_unordered = false;
                            let mut ret = item.empty_like_self();
                            for i1 in 0..item.len() {
                                let ts = item.ts(i1);
                                if ts < self.ts_max {
                                    contains_unordered = true;
                                    if false {
                                        self.done = true;
                                        let msg = format!(
                                            "unordered event  i1 {} / {}  ts {:?}  ts_max {:?}",
                                            i1,
                                            item.len(),
                                            Nanos::from_ns(ts),
                                            Nanos::from_ns(self.ts_max)
                                        );
                                        error!("{}", msg);
                                        return Ready(Some(Err(Error::with_msg(msg))));
                                    }
                                } else {
                                    self.ts_max = ts;
                                    if ts < self.range.beg {
                                        contains_pre = true;
                                        if self.expand {
                                            let mut prerange = if let Some(prerange) = self.prerange.take() {
                                                prerange
                                            } else {
                                                item.empty_like_self()
                                            };
                                            prerange.clear();
                                            prerange.push_index(&item, i1);
                                            self.prerange = Some(prerange);
                                            self.have_pre = true;
                                        }
                                    } else if ts >= self.range.end {
                                        contains_post = true;
                                        self.have_range_complete = true;
                                        if self.expand {
                                            if self.have_pre {
                                                let prerange = if let Some(prerange) = &mut self.prerange {
                                                    prerange
                                                } else {
                                                    panic!()
                                                };
                                                ret.push_index(prerange, 0);
                                                prerange.clear();
                                                self.have_pre = false;
                                            }
                                            if !self.emitted_post {
                                                self.emitted_post = true;
                                                ret.push_index(&item, i1);
                                                //self.data_done = true;
                                            }
                                        } else {
                                            //self.data_done = true;
                                        }
                                    } else {
                                        if self.expand {
                                            if self.have_pre {
                                                let prerange = if let Some(prerange) = &mut self.prerange {
                                                    prerange
                                                } else {
                                                    panic!()
                                                };
                                                ret.push_index(prerange, 0);
                                                prerange.clear();
                                                self.have_pre = false;
                                            }
                                        }
                                        ret.push_index(&item, i1);
                                    }
                                }
                            }
                            if contains_pre {
                                self.items_with_pre += 1;
                            }
                            if contains_post {
                                self.items_with_post += 1;
                            }
                            if contains_unordered {
                                self.items_with_unordered += 1;
                            }
                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(ret)))))
                        }
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)) => {
                            self.have_range_complete = true;
                            continue;
                        }
                        k => Ready(Some(k)),
                    },
                    Ready(None) => {
                        self.data_done = true;
                        if self.have_pre {
                            let prerange = if let Some(prerange) = &mut self.prerange {
                                prerange
                            } else {
                                panic!()
                            };
                            let mut ret = prerange.empty_like_self();
                            ret.push_index(&prerange, 0);
                            prerange.clear();
                            self.have_pre = false;
                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(ret)))))
                        } else {
                            continue;
                        }
                    }
                    Pending => Pending,
                }
            };
        }
    }
}

impl<S, ITY> Stream for RangeFilter<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: WithTimestamps + PushableIndex + Appendable + Clearable + Unpin,
{
    type Item = Sitemty<ITY>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let span1 = span!(Level::INFO, "RangeFilter", range = tracing::field::Empty);
        span1.record("range", &self.range_str.as_str());
        span1.in_scope(|| Self::poll_next(self, cx))
    }
}

impl<S, ITY> fmt::Debug for RangeFilter<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: WithTimestamps + PushableIndex + Appendable + Clearable + Unpin,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RangeFilter")
            .field("items_with_pre", &self.items_with_pre)
            .field("items_with_post", &self.items_with_post)
            .field("items_with_unordered", &self.items_with_unordered)
            .finish()
    }
}

impl<S, ITY> Drop for RangeFilter<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: WithTimestamps + PushableIndex + Appendable + Clearable + Unpin,
{
    fn drop(&mut self) {
        debug!("Drop {:?}", self);
    }
}
