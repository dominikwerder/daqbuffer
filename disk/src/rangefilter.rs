use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use futures_util::StreamExt;
use items::{Appendable, Clearable, PushableIndex, RangeCompletableItem, Sitemty, StreamItem, WithTimestamps};
use netpod::NanoRange;

pub struct RangeFilter<S, ITY> {
    inp: S,
    range: NanoRange,
    expand: bool,
    prerange: ITY,
    have_pre: bool,
    emitted_post: bool,
    done: bool,
    complete: bool,
}

impl<S, ITY> RangeFilter<S, ITY>
where
    ITY: Appendable,
{
    pub fn new(inp: S, range: NanoRange, expand: bool) -> Self {
        Self {
            inp,
            range,
            expand,
            prerange: ITY::empty(),
            have_pre: false,
            emitted_post: false,
            done: false,
            complete: false,
        }
    }
}

impl<S, ITY> Stream for RangeFilter<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: WithTimestamps + PushableIndex + Appendable + Clearable + Unpin,
{
    type Item = Sitemty<ITY>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.complete {
            panic!("poll_next on complete");
        } else if self.done {
            self.complete = true;
            Ready(None)
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
                                } else {
                                };
                            } else if ts >= self.range.end {
                                if self.expand {
                                    if self.have_pre {
                                        ret.push_index(&self.prerange, 0);
                                        self.prerange.clear();
                                        self.have_pre = false;
                                    };
                                    if !self.emitted_post {
                                        self.emitted_post = true;
                                        ret.push_index(&item, i1);
                                        self.done = true;
                                    } else {
                                        panic!();
                                    };
                                } else {
                                    self.done = true;
                                };
                            } else {
                                if self.expand {
                                    if self.have_pre {
                                        ret.push_index(&self.prerange, 0);
                                        self.prerange.clear();
                                        self.have_pre = false;
                                    }
                                    ret.push_index(&item, i1);
                                } else {
                                    ret.push_index(&item, i1);
                                };
                            };
                        }
                        Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(ret)))))
                    }
                    k => Ready(Some(k)),
                },
                Ready(None) => {
                    if self.have_pre {
                        let mut ret = ITY::empty();
                        ret.push_index(&self.prerange, 0);
                        self.have_pre = false;
                        self.done = true;
                        Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(ret)))))
                    } else {
                        self.done = true;
                        self.complete = true;
                        Ready(None)
                    }
                }
                Pending => Pending,
            }
        }
    }
}
