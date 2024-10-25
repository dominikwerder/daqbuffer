use crate::timebin::cached::reader::EventsReadProvider;
use crate::timebin::cached::reader::EventsReading;
use futures_util::Stream;
use items_0::streamitem::Sitemty;
use items_2::channelevents::ChannelEvents;
use netpod::range::evrange::SeriesRange;
use query::api4::events::EventsSubQuery;
use rand_xoshiro::rand_core::SeedableRng;
use rand_xoshiro::Xoshiro128PlusPlus;
use std::pin::Pin;

fn make_stream(chname: &str, range: &SeriesRange) -> Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>> {
    if chname == "unittest;scylla;cont;scalar;f32" {
        let e = ::err::Error::with_msg_no_trace("unknown channel {chname}");
        let ret = futures_util::stream::iter([Err(e)]);
        Box::pin(ret)
    } else {
        let e = ::err::Error::with_msg_no_trace("unknown channel {chname}");
        let ret = futures_util::stream::iter([Err(e)]);
        Box::pin(ret)
    }
}

pub struct UnitTestStream {}

impl UnitTestStream {
    pub fn new() -> Self {
        Self {}
    }
}

impl EventsReadProvider for UnitTestStream {
    fn read(&self, evq: EventsSubQuery) -> EventsReading {
        let stream = make_stream(evq.name(), evq.range());
        EventsReading::new(stream)
    }
}
