use super::timeweight_events::BinnedEventsTimeweight;
use crate::binning::container_events::EventValueType;
use crate::channelevents::ChannelEvents;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use items_0::streamitem::Sitemty;
use items_0::timebin::BinnedEventsTimeweightTrait;
use items_0::timebin::BinningggBinnerDyn;
use items_0::timebin::BinningggContainerBinsDyn;
use items_0::timebin::BinningggContainerEventsDyn;
use items_0::timebin::BinningggError;
use netpod::BinnedRange;
use netpod::TsNano;
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
    fn ingest(&mut self, evs_all: Box<dyn BinningggContainerEventsDyn>) -> Result<(), BinningggError> {
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

    fn output(&mut self) -> Result<Box<dyn BinningggContainerBinsDyn>, BinningggError> {
        // self.binner.output()
        todo!()
    }
}

pub struct BinnedEventsTimeweightStream {
    inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>,
}

impl Stream for BinnedEventsTimeweightStream {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
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
    fn ingest(&mut self, evs_all: Box<dyn BinningggContainerEventsDyn>) -> Result<(), BinningggError> {
        // TODO the container must provide a method to create the dyn binner.
        let binned_events = self.binned_events.get_or_insert_with(|| todo!());
        todo!()
    }

    fn input_done_range_final(&mut self) -> Result<(), BinningggError> {
        todo!()
    }

    fn input_done_range_open(&mut self) -> Result<(), BinningggError> {
        todo!()
    }

    fn output(&mut self) -> Result<Box<dyn BinningggContainerBinsDyn>, BinningggError> {
        todo!()
    }
}
