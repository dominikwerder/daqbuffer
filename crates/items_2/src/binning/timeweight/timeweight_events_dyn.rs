use super::timeweight_events::BinnedEventsTimeweight;
use crate::binning::container_bins::ContainerBins;
use crate::binning::container_events::EventValueType;
use crate::channelevents::ChannelEvents;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::Sitemty;
use items_0::timebin::BinnedEventsTimeweightTrait;
use items_0::timebin::BinningggBinnerDyn;
use items_0::timebin::BinningggContainerBinsDyn;
use items_0::timebin::BinningggContainerEventsDyn;
use items_0::timebin::BinningggError;
use items_0::timebin::BinsBoxed;
use items_0::timebin::EventsBoxed;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::TsNano;
use std::arch::x86_64;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug, ThisError)]
#[cstm(name = "BinnedEventsTimeweightDyn")]
pub enum Error {
    InnerDynMissing,
}

#[derive(Debug)]
pub struct BinnedEventsTimeweightDynbox<EVT>
where
    EVT: EventValueType,
{
    range: BinnedRange<TsNano>,
    binner: BinnedEventsTimeweight<EVT>,
}

impl<EVT> BinnedEventsTimeweightDynbox<EVT>
where
    EVT: EventValueType + 'static,
{
    pub fn new(range: BinnedRange<TsNano>) -> Box<dyn BinnedEventsTimeweightTrait> {
        let ret = Self {
            binner: BinnedEventsTimeweight::new(range.clone()),
            range,
        };
        Box::new(ret)
    }
}

impl<EVT> BinnedEventsTimeweightTrait for BinnedEventsTimeweightDynbox<EVT>
where
    EVT: EventValueType,
{
    fn ingest(&mut self, evs_all: EventsBoxed) -> Result<(), BinningggError> {
        todo!()
    }

    fn input_done_range_final(&mut self) -> Result<(), BinningggError> {
        // self.binner.input_done_range_final()
        todo!()
    }

    fn input_done_range_open(&mut self) -> Result<(), BinningggError> {
        // self.binner.input_done_range_open()
        todo!()
    }

    fn output(&mut self) -> Result<BinsBoxed, BinningggError> {
        // self.binner.output()
        todo!()
    }
}

#[derive(Debug)]
pub struct BinnedEventsTimeweightLazy {
    range: BinnedRange<TsNano>,
    binned_events: Option<Box<dyn BinnedEventsTimeweightTrait>>,
}

impl BinnedEventsTimeweightLazy {
    pub fn new(range: BinnedRange<TsNano>) -> Self {
        Self {
            range,
            binned_events: None,
        }
    }
}

impl BinnedEventsTimeweightTrait for BinnedEventsTimeweightLazy {
    fn ingest(&mut self, evs_all: EventsBoxed) -> Result<(), BinningggError> {
        self.binned_events
            .get_or_insert_with(|| evs_all.binned_events_timeweight_traitobj())
            .ingest(evs_all)
    }

    fn input_done_range_final(&mut self) -> Result<(), BinningggError> {
        debug!("TODO something to do if we miss the binner here?");
        self.binned_events
            .as_mut()
            .map(|x| x.input_done_range_final())
            .unwrap_or(Ok(()))
    }

    fn input_done_range_open(&mut self) -> Result<(), BinningggError> {
        debug!("TODO something to do if we miss the binner here?");
        self.binned_events
            .as_mut()
            .map(|x| x.input_done_range_open())
            .unwrap_or(Ok(()))
    }

    fn output(&mut self) -> Result<BinsBoxed, BinningggError> {
        debug!("TODO something to do if we miss the binner here?");
        // TODO change trait because without binner we can not produce any container here
        todo!()
    }
}

enum StreamState {
    Reading,
    Done,
}

pub struct BinnedEventsTimeweightStream {
    state: StreamState,
    inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>,
    binned_events: BinnedEventsTimeweightLazy,
    range_complete: bool,
}

impl BinnedEventsTimeweightStream {
    pub fn new(range: BinnedRange<TsNano>, inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>) -> Self {
        Self {
            state: StreamState::Reading,
            inp,
            binned_events: BinnedEventsTimeweightLazy::new(range),
            range_complete: false,
        }
    }

    fn handle_sitemty(
        mut self: Pin<&mut Self>,
        item: Sitemty<ChannelEvents>,
        cx: &mut Context,
    ) -> ControlFlow<Poll<Option<<Self as Stream>::Item>>> {
        use items_0::streamitem::RangeCompletableItem::*;
        use items_0::streamitem::StreamItem::*;
        use ControlFlow::*;
        use Poll::*;
        match item {
            Ok(x) => match x {
                DataItem(x) => match x {
                    Data(x) => match x {
                        ChannelEvents::Events(evs) => match self.binned_events.ingest(evs.to_container_events()) {
                            Ok(()) => {
                                match self.binned_events.output() {
                                    Ok(x) => {
                                        if x.len() == 0 {
                                            Continue(())
                                        } else {
                                            Break(Ready(Some(Ok(DataItem(Data(x))))))
                                        }
                                    }
                                    Err(e) => Break(Ready(Some(Err(::err::Error::from_string(e))))),
                                }
                                // Continue(())
                            }
                            Err(e) => Break(Ready(Some(Err(::err::Error::from_string(e))))),
                        },
                        ChannelEvents::Status(_) => {
                            // TODO use the status
                            Continue(())
                        }
                    },
                    RangeComplete => {
                        self.range_complete = true;
                        Continue(())
                    }
                },
                Log(x) => Break(Ready(Some(Ok(Log(x))))),
                Stats(x) => Break(Ready(Some(Ok(Stats(x))))),
            },
            Err(e) => {
                self.state = StreamState::Done;
                Break(Ready(Some(Err(e))))
            }
        }
    }

    fn handle_eos(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<<Self as Stream>::Item>> {
        use items_0::streamitem::RangeCompletableItem::*;
        use items_0::streamitem::StreamItem::*;
        use Poll::*;
        if self.range_complete {
            self.binned_events.input_done_range_final();
        } else {
            self.binned_events.input_done_range_open();
        }
        match self.binned_events.output() {
            Ok(x) => Ready(Some(Ok(DataItem(Data(x))))),
            Err(e) => Ready(Some(Err(::err::Error::from_string(e)))),
        }
    }
}

impl Stream for BinnedEventsTimeweightStream {
    type Item = Sitemty<Box<dyn BinningggContainerBinsDyn>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use ControlFlow::*;
        use Poll::*;
        loop {
            break match self.as_mut().inp.poll_next_unpin(cx) {
                Ready(Some(x)) => match self.as_mut().handle_sitemty(x, cx) {
                    Continue(()) => continue,
                    Break(x) => x,
                },
                Ready(None) => self.handle_eos(cx),
                Pending => Pending,
            };
        }
    }
}
