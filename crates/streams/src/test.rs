#[cfg(test)]
mod collect;
#[cfg(test)]
mod events;
#[cfg(test)]
mod timebin;

use err::Error;
use futures_util::stream;
use futures_util::Stream;
use items_0::streamitem::sitem_data;
use items_0::streamitem::Sitemty;
use items_0::Appendable;
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
    let stream = stream::iter([item]);
    Box::pin(stream)
}

fn inmem_test_events_d0_i32_01() -> BoxedEventStream {
    let mut evs = EventsDim0::empty();
    evs.push(SEC * 2, 2, 10002);
    let cev = ChannelEvents::Events(Box::new(evs));
    let item = sitem_data(cev);
    let stream = stream::iter([item]);
    Box::pin(stream)
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

fn runfut<T, F>(fut: F) -> Result<T, err::Error>
where
    F: std::future::Future<Output = Result<T, Error>>,
{
    use futures_util::TryFutureExt;
    let fut = fut.map_err(|e| e.into());
    taskrun::run(fut)
}
