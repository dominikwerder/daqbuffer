use crate::channelevents::ChannelEvents;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use items_0::streamitem::Sitemty;
use items_0::timebin::BinningggBinnerDyn;
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

pub struct BinnedEventsTimeweightDyn {
    range: BinnedRange<TsNano>,
    binner: Option<Box<dyn BinningggBinnerDyn>>,
}

impl BinnedEventsTimeweightDyn {
    pub fn new(range: BinnedRange<TsNano>) -> Self {
        Self { range, binner: None }
    }

    pub fn ingest(&mut self, mut evs_all: ContainerEventsDyn) -> Result<(), BinningggError> {
        TODO;
        todo!()
    }

    pub fn input_done_range_final(&mut self) -> Result<(), BinningggError> {
        self.binner
            .as_mut()
            .ok_or(Error::InnerDynMissing)?
            .input_done_range_final()
    }

    pub fn input_done_range_open(&mut self) -> Result<(), BinningggError> {
        self.binner
            .as_mut()
            .ok_or(Error::InnerDynMissing)?
            .input_done_range_open()
    }

    pub fn output(&mut self) -> ContainerBinsDyn {
        TODO;
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
