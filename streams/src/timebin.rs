use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::sitem_data;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::timebin::TimeBinnableTy;
use items_0::timebin::TimeBinnerTy;
use netpod::log::*;
use netpod::BinnedRangeEnum;
use std::any;
use std::fmt;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => ();
    ($($arg:tt)*) => (trace!($($arg)*));
}

#[allow(unused)]
macro_rules! trace3 {
    ($($arg:tt)*) => ();
    ($($arg:tt)*) => (trace!($($arg)*));
}

#[allow(unused)]
macro_rules! trace4 {
    ($($arg:tt)*) => ();
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
    range_final: bool,
    binner: Option<<T as TimeBinnableTy>::TimeBinner>,
    done_first_input: bool,
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
            .field("range_final", &self.range_final)
            .field("binner", &self.binner)
            .finish()
    }
}

impl<T> TimeBinnedStream<T>
where
    T: TimeBinnableTy,
{
    pub fn new(inp: MergeInp<T>, range: BinnedRangeEnum, do_time_weight: bool) -> Self {
        Self {
            inp,
            range,
            do_time_weight,
            range_final: false,
            binner: None,
            done_first_input: false,
            done_data: false,
            done: false,
            complete: false,
        }
    }

    fn process_item(&mut self, mut item: T) -> () {
        info!("process_item {item:?}");
        if self.binner.is_none() {
            trace!("process_item call time_binner_new");
            let binner = item.time_binner_new(self.range.clone(), self.do_time_weight);
            self.binner = Some(binner);
        }
        let binner = self.binner.as_mut().unwrap();
        trace!("process_item call binner ingest");
        binner.ingest(&mut item);
    }

    fn handle_data_item(
        &mut self,
        item: T,
    ) -> Result<ControlFlow<Poll<Sitemty<<<T as TimeBinnableTy>::TimeBinner as TimeBinnerTy>::Output>>>, Error> {
        use ControlFlow::*;
        use Poll::*;
        info!("=================   handle_data_item");
        let item_len = item.len();
        self.process_item(item);
        let mut do_emit = false;
        if self.done_first_input == false {
            info!(
                "emit container after the first input  len {}  binner {}",
                item_len,
                self.binner.is_some()
            );
            if self.binner.is_none() {
                let e = Error::with_msg_no_trace("must emit on first input but no binner");
                self.done = true;
                return Err(e);
            }
            do_emit = true;
            self.done_first_input = true;
        }
        if let Some(binner) = self.binner.as_mut() {
            trace3!("bins ready count {}", binner.bins_ready_count());
            if binner.bins_ready_count() > 0 {
                do_emit = true
            }
            if do_emit {
                if let Some(bins) = binner.bins_ready() {
                    Ok(Break(Ready(sitem_data(bins))))
                } else {
                    warn!("bins ready but got nothing");
                    if let Some(bins) = binner.empty() {
                        Ok(Break(Ready(sitem_data(bins))))
                    } else {
                        let e = Error::with_msg_no_trace("bins ready, but nothing, can not even create empty A");
                        error!("{e}");
                        Err(e)
                    }
                }
            } else {
                trace3!("not emit");
                Ok(ControlFlow::Continue(()))
            }
        } else {
            warn!("processed item, but no binner yet");
            Ok(ControlFlow::Continue(()))
        }
    }

    fn handle_item(
        &mut self,
        item: Result<StreamItem<RangeCompletableItem<T>>, Error>,
    ) -> Result<ControlFlow<Poll<Sitemty<<<T as TimeBinnableTy>::TimeBinner as TimeBinnerTy>::Output>>>, Error> {
        use ControlFlow::*;
        use Poll::*;
        info!("=================   handle_item");
        match item {
            Ok(item) => match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => {
                        debug!("see RangeComplete");
                        self.range_final = true;
                        Ok(Continue(()))
                    }
                    RangeCompletableItem::Data(item) => self.handle_data_item(item),
                },
                StreamItem::Log(item) => Ok(Break(Ready(Ok(StreamItem::Log(item))))),
                StreamItem::Stats(item) => Ok(Break(Ready(Ok(StreamItem::Stats(item))))),
            },
            Err(e) => {
                self.done = true;
                Err(e)
            }
        }
    }

    fn handle_none(
        &mut self,
    ) -> Result<ControlFlow<Poll<Sitemty<<<T as TimeBinnableTy>::TimeBinner as TimeBinnerTy>::Output>>>, Error> {
        use ControlFlow::*;
        use Poll::*;
        trace2!("=================   handle_none");
        let self_range_complete = self.range_final;
        if let Some(binner) = self.binner.as_mut() {
            trace!("bins ready count before finish {}", binner.bins_ready_count());
            // TODO rework the finish logic
            if self_range_complete {
                binner.set_range_complete();
            }
            binner.push_in_progress(false);
            trace!("bins ready count after finish  {}", binner.bins_ready_count());
            if let Some(bins) = binner.bins_ready() {
                self.done_data = true;
                Ok(Break(Ready(sitem_data(bins))))
            } else {
                warn!("bins ready but got nothing");
                if let Some(bins) = binner.empty() {
                    Ok(Break(Ready(sitem_data(bins))))
                } else {
                    let e = Error::with_msg_no_trace("bins ready, but nothing, can not even create empty B");
                    error!("{e}");
                    self.done_data = true;
                    Err(e)
                }
            }
        } else {
            warn!("input stream finished, still no binner");
            self.done_data = true;
            let e = Error::with_msg_no_trace(format!("input stream finished, still no binner"));
            Err(e)
        }
    }

    // TODO
    // Original block inside the poll loop was able to:
    // continue
    // break with Poll<Option<Item>>
    fn poll_input(
        &mut self,
        cx: &mut Context,
    ) -> Result<ControlFlow<Poll<Sitemty<<<T as TimeBinnableTy>::TimeBinner as TimeBinnerTy>::Output>>>, Error> {
        use ControlFlow::*;
        use Poll::*;
        trace2!("=================   poll_input");
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => self.handle_item(item),
            Ready(None) => self.handle_none(),
            Pending => Ok(Break(Pending)),
        }
    }
}

impl<T> Stream for TimeBinnedStream<T>
where
    T: TimeBinnableTy + Unpin,
{
    type Item = Sitemty<<<T as TimeBinnableTy>::TimeBinner as TimeBinnerTy>::Output>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let span = span!(Level::INFO, "TimeBinner");
        let _spg = span.enter();
        trace2!("=================   POLL");
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
                match self.poll_input(cx) {
                    Ok(item) => match item {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(item) => match item {
                            Ready(item) => break Ready(Some(item)),
                            Pending => break Pending,
                        },
                    },
                    Err(e) => {
                        self.done = true;
                        break Ready(Some(Err(e)));
                    }
                }
            };
        }
    }
}

//impl<T> WithTransformProperties for TimeBinnedStream<T> where T: TimeBinnableTy {}

//impl<T> TimeBinnableStreamTrait for TimeBinnedStream<T> where T: TimeBinnableTy {}
