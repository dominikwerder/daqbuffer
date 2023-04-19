use err::Error;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::sitem_data;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::timebin::TimeBinnableTy;
use items_0::timebin::TimeBinner;
use items_0::timebin::TimeBinnerTy;
use items_0::transform::TimeBinnableStreamTrait;
use items_0::transform::WithTransformProperties;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::BinnedRangeEnum;
use netpod::Dim0Index;
use std::any;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

#[allow(unused)]
macro_rules! trace2 {
    (__$($arg:tt)*) => ();
    ($($arg:tt)*) => (trace!($($arg)*));
}

#[allow(unused)]
macro_rules! trace3 {
    (__$($arg:tt)*) => ();
    ($($arg:tt)*) => (trace!($($arg)*));
}

#[allow(unused)]
macro_rules! trace4 {
    (__$($arg:tt)*) => ();
    ($($arg:tt)*) => (trace!($($arg)*));
}

type MergeInp<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;

pub struct TimeBinnedStream<T>
where
    T: TimeBinnableTy,
{
    inp: MergeInp<T>,
    range: BinnedRangeEnum,
    do_time_weight: bool,
    deadline: Instant,
    deadline_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    range_final: bool,
    binner: Option<<T as TimeBinnableTy>::TimeBinner>,
    done_data: bool,
    done: bool,
    complete: bool,
}

impl<T> fmt::Debug for TimeBinnedStream<T>
where
    T: TimeBinnableTy,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct(any::type_name::<Self>())
            .field("range", &self.range)
            .field("deadline", &self.deadline)
            .field("range_final", &self.range_final)
            .field("binner", &self.binner)
            .finish()
    }
}

impl<T> TimeBinnedStream<T>
where
    T: TimeBinnableTy,
{
    pub fn new(inp: MergeInp<T>, range: BinnedRangeEnum, do_time_weight: bool, deadline: Instant) -> Self {
        let deadline_fut = tokio::time::sleep_until(deadline.into());
        let deadline_fut = Box::pin(deadline_fut);
        Self {
            inp,
            range,
            do_time_weight,
            deadline,
            deadline_fut,
            range_final: false,
            binner: None,
            done_data: false,
            done: false,
            complete: false,
        }
    }

    fn process_item(&mut self, mut item: T) -> () {
        trace!("process_item {item:?}");
        if self.binner.is_none() {
            trace!("process_item call time_binner_new");
            let binner = item.time_binner_new(self.range.clone(), self.do_time_weight);
            self.binner = Some(binner);
        }
        let binner = self.binner.as_mut().unwrap();
        trace!("process_item call binner ingest");
        binner.ingest(&mut item);
    }
}

impl<T> Stream for TimeBinnedStream<T>
where
    T: TimeBinnableTy + Unpin,
{
    type Item = Sitemty<<<T as TimeBinnableTy>::TimeBinner as TimeBinnerTy>::Output>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let span = span!(Level::INFO, "poll");
        let _spg = span.enter();
        loop {
            break if self.complete {
                panic!("poll on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if self.done_data {
                self.done = true;
                if self.range_final {
                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                } else {
                    continue;
                }
            } else {
                match self.deadline_fut.poll_unpin(cx) {
                    Ready(()) => {
                        trace2!("timeout");
                        let self_range_complete = self.range_final;
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
                                    self.range_final = true;
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
                        trace!("finish up");
                        let self_range_complete = self.range_final;
                        if let Some(binner) = self.binner.as_mut() {
                            trace!("bins ready count before finish {}", binner.bins_ready_count());
                            // TODO rework the finish logic
                            if self_range_complete {
                                binner.set_range_complete();
                            }
                            binner.push_in_progress(false);
                            trace!("bins ready count after finish  {}", binner.bins_ready_count());
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
                                if let Some(bins) = binner.empty() {
                                    trace!("at end of stream, bin count zero, return {bins:?}");
                                    self.done_data = true;
                                    Ready(Some(sitem_data(bins)))
                                } else {
                                    error!("at the end, no bins, can not get empty");
                                    self.done_data = true;
                                    let e = Error::with_msg_no_trace(format!("no bins"))
                                        .add_public_msg(format!("unable to produce bins"));
                                    Ready(Some(Err(e)))
                                }
                            }
                        } else {
                            trace!("input stream finished, still no binner");
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

//impl<T> WithTransformProperties for TimeBinnedStream<T> where T: TimeBinnableTy {}

//impl<T> TimeBinnableStreamTrait for TimeBinnedStream<T> where T: TimeBinnableTy {}
