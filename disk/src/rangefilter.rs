use futures_core::Stream;
use futures_util::StreamExt;
use items::{Appendable, Clearable, PushableIndex, RangeCompletableItem, Sitemty, StreamItem, WithTimestamps};
use netpod::log::*;
use netpod::NanoRange;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct RangeFilter<S, ITY> {
    inp: S,
    range: NanoRange,
    range_str: String,
    expand: bool,
    prerange: ITY,
    have_pre: bool,
    have_range_complete: bool,
    emitted_post: bool,
    data_done: bool,
    done: bool,
    complete: bool,
}

impl<S, ITY> RangeFilter<S, ITY>
where
    ITY: Appendable,
{
    pub fn new(inp: S, range: NanoRange, expand: bool) -> Self {
        trace!("RangeFilter::new  range: {:?}  expand: {:?}", range, expand);
        Self {
            inp,
            range_str: format!("{:?}", range),
            range,
            expand,
            prerange: ITY::empty(),
            have_pre: false,
            have_range_complete: false,
            emitted_post: false,
            data_done: false,
            done: false,
            complete: false,
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
            } else if self.data_done {
                self.done = true;
                if self.have_range_complete {
                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                } else {
                    continue;
                }
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(item)) => match item {
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))) => {
                            let mut ret = ITY::empty();
                            for i1 in 0..item.len() {
                                let ts = item.ts(i1);
                                if ts < self.range.beg {
                                    if self.expand {
                                        self.prerange.clear();
                                        self.prerange.push_index(&item, i1);
                                        self.have_pre = true;
                                    }
                                } else if ts >= self.range.end {
                                    self.have_range_complete = true;
                                    if self.expand {
                                        if self.have_pre {
                                            ret.push_index(&self.prerange, 0);
                                            self.prerange.clear();
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
                                            ret.push_index(&self.prerange, 0);
                                            self.prerange.clear();
                                            self.have_pre = false;
                                        }
                                    }
                                    ret.push_index(&item, i1);
                                };
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
                            let mut ret = ITY::empty();
                            ret.push_index(&self.prerange, 0);
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
