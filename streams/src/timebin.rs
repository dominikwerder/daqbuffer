use err::Error;
use futures_util::{Future, FutureExt, Stream, StreamExt};
use items::{sitem_data, RangeCompletableItem, Sitemty, StreamItem};
use items_2::timebin::{TimeBinnable, TimeBinner};
use netpod::log::*;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

#[allow(unused)]
macro_rules! trace2 {
    (D$($arg:tt)*) => ();
    ($($arg:tt)*) => (eprintln!($($arg)*));
}

#[allow(unused)]
macro_rules! trace3 {
    (D$($arg:tt)*) => ();
    ($($arg:tt)*) => (eprintln!($($arg)*));
}

#[allow(unused)]
macro_rules! trace4 {
    (D$($arg:tt)*) => ();
    ($($arg:tt)*) => (eprintln!($($arg)*));
}

type MergeInp<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;

pub struct TimeBinnedStream<T>
where
    T: TimeBinnable,
{
    inp: MergeInp<T>,
    edges: Vec<u64>,
    do_time_weight: bool,
    deadline: Instant,
    deadline_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    range_complete: bool,
    binner: Option<<T as TimeBinnable>::TimeBinner>,
    done_data: bool,
    done: bool,
    complete: bool,
}

impl<T> fmt::Debug for TimeBinnedStream<T>
where
    T: TimeBinnable,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("TimeBinnedStream")
            .field("edges", &self.edges)
            .field("deadline", &self.deadline)
            .field("range_complete", &self.range_complete)
            .field("binner", &self.binner)
            .finish()
    }
}

impl<T> TimeBinnedStream<T>
where
    T: TimeBinnable,
{
    pub fn new(inp: MergeInp<T>, edges: Vec<u64>, do_time_weight: bool, deadline: Instant) -> Self {
        let deadline_fut = tokio::time::sleep_until(deadline.into());
        let deadline_fut = Box::pin(deadline_fut);
        Self {
            inp,
            edges,
            do_time_weight,
            deadline,
            deadline_fut,
            range_complete: false,
            binner: None,
            done_data: false,
            done: false,
            complete: false,
        }
    }

    fn process_item(&mut self, mut item: T) -> () {
        if self.binner.is_none() {
            let binner = item.time_binner_new(self.edges.clone(), self.do_time_weight);
            self.binner = Some(binner);
        }
        let binner = self.binner.as_mut().unwrap();
        binner.ingest(&mut item);
    }
}

impl<T> Stream for TimeBinnedStream<T>
where
    T: TimeBinnable + Unpin,
{
    type Item = Sitemty<<<T as TimeBinnable>::TimeBinner as TimeBinner>::Output>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let span = tracing::span!(tracing::Level::TRACE, "poll");
        let _spg = span.enter();
        loop {
            break if self.complete {
                panic!("poll on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if self.done_data {
                self.done = true;
                if self.range_complete {
                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                } else {
                    continue;
                }
            } else {
                match self.deadline_fut.poll_unpin(cx) {
                    Ready(()) => {
                        trace2!("timeout");
                        let self_range_complete = self.range_complete;
                        if let Some(binner) = self.binner.as_mut() {
                            trace2!("bins ready count before finish {}", binner.bins_ready_count());
                            // TODO rework the finish logic
                            if self_range_complete {
                                binner.set_range_complete();
                            }
                            trace2!("bins ready count after finish  {}", binner.bins_ready_count());
                            if let Some(bins) = binner.bins_ready() {
                                self.done_data = true;
                                return Ready(Some(sitem_data(bins)));
                            } else {
                                self.done_data = true;
                                continue;
                            }
                        } else {
                            continue;
                        }
                    }
                    Pending => {}
                }
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(item)) => match item {
                        Ok(item) => match item {
                            StreamItem::DataItem(item) => match item {
                                RangeCompletableItem::RangeComplete => {
                                    debug!("see RangeComplete");
                                    self.range_complete = true;
                                    continue;
                                }
                                RangeCompletableItem::Data(item) => {
                                    self.process_item(item);
                                    if let Some(binner) = self.binner.as_mut() {
                                        trace3!("bins ready count {}", binner.bins_ready_count());
                                        if binner.bins_ready_count() > 0 {
                                            if let Some(bins) = binner.bins_ready() {
                                                Ready(Some(sitem_data(bins)))
                                            } else {
                                                trace2!("bins ready but got nothing");
                                                Pending
                                            }
                                        } else {
                                            trace3!("no bins ready yet");
                                            continue;
                                        }
                                    } else {
                                        trace2!("processed item, but no binner yet");
                                        continue;
                                    }
                                }
                            },
                            StreamItem::Log(item) => Ready(Some(Ok(StreamItem::Log(item)))),
                            StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                        },
                        Err(e) => {
                            self.done_data = true;
                            Ready(Some(Err(e)))
                        }
                    },
                    Ready(None) => {
                        trace2!("finish up");
                        let self_range_complete = self.range_complete;
                        if let Some(binner) = self.binner.as_mut() {
                            trace2!("bins ready count before finish {}", binner.bins_ready_count());
                            // TODO rework the finish logic
                            if self_range_complete {
                                binner.set_range_complete();
                            }
                            binner.push_in_progress(false);
                            trace2!("bins ready count after finish  {}", binner.bins_ready_count());
                            if binner.bins_ready_count() > 0 {
                                if let Some(bins) = binner.bins_ready() {
                                    self.done_data = true;
                                    Ready(Some(sitem_data(bins)))
                                } else {
                                    trace2!("bins ready but got nothing");
                                    self.done_data = true;
                                    let e = Error::with_msg_no_trace(format!("bins ready but got nothing"));
                                    Ready(Some(Err(e)))
                                }
                            } else {
                                trace2!("no bins ready yet");
                                self.done_data = true;
                                continue;
                            }
                        } else {
                            trace2!("input stream finished, still no binner");
                            self.done_data = true;
                            continue;
                        }
                    }
                    Pending => Pending,
                }
            };
        }
    }
}
