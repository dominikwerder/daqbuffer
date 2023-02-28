#[cfg(test)]
mod collect;
#[cfg(test)]
mod timebin;

use err::Error;
use futures_util::stream;
use futures_util::Stream;
use items_0::streamitem::sitem_data;
use items_0::streamitem::Sitemty;
use items_0::Empty;
use items_2::channelevents::ChannelEvents;
use items_2::eventsdim0::EventsDim0;
use netpod::timeunits::SEC;
use std::pin::Pin;

type BoxedEventStream = Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>;

// TODO use some xorshift generator.

fn inmem_test_events_d0_i32_00() -> BoxedEventStream {
    let mut evs = EventsDim0::empty();
    evs.push(SEC * 1, 1, 10001);
    evs.push(SEC * 4, 4, 10004);
    let cev = ChannelEvents::Events(Box::new(evs));
    let item = sitem_data(cev);
    let stream = stream::iter(vec![item]);
    Box::pin(stream)
}

fn inmem_test_events_d0_i32_01() -> BoxedEventStream {
    let mut evs = EventsDim0::empty();
    evs.push(SEC * 2, 2, 10002);
    let cev = ChannelEvents::Events(Box::new(evs));
    let item = sitem_data(cev);
    let stream = stream::iter(vec![item]);
    Box::pin(stream)
}

#[test]
fn empty_input() -> Result<(), Error> {
    // TODO with a pipeline of x-binning, merging, t-binning and collection, how do I get a meaningful
    // result even if there is no input data at all?
    Err(Error::with_msg_no_trace("TODO"))
}

#[test]
fn merge_mergeable_00() -> Result<(), Error> {
    let fut = async {
        let inp0 = inmem_test_events_d0_i32_00();
        let inp1 = inmem_test_events_d0_i32_01();
        let _merger = items_2::merger::Merger::new(vec![inp0, inp1], 4);
        Ok(())
    };
    runfut(fut)
}

#[test]
fn timeout() -> Result<(), Error> {
    // TODO expand from items_2::test
    Err(Error::with_msg_no_trace("TODO"))
}

fn runfut<T, F>(fut: F) -> Result<T, err::Error>
where
    F: std::future::Future<Output = Result<T, Error>>,
{
    use futures_util::TryFutureExt;
    let fut = fut.map_err(|e| e.into());
    taskrun::run(fut)
}
