use crate::test::runfut;
use futures_util::{stream, StreamExt};
use items::sitem_data;
use items_2::testgen::make_some_boxed_d0_f32;
use items_2::{ChannelEvents, ConnStatus, ConnStatusEvent};
use netpod::timeunits::{MS, SEC};
use std::time::{Duration, Instant};

#[test]
fn time_bin_00() {
    let fut = async {
        let edges = [0, 1, 2, 3, 4, 5, 6, 7, 8].into_iter().map(|x| SEC * x).collect();
        let evs0 = make_some_boxed_d0_f32(10, SEC * 1, MS * 500, 0, 1846713782);
        let v0 = ChannelEvents::Events(evs0);
        let v2 = ChannelEvents::Status(ConnStatusEvent::new(MS * 100, ConnStatus::Connect));
        let v4 = ChannelEvents::Status(ConnStatusEvent::new(MS * 6000, ConnStatus::Disconnect));
        let stream0 = Box::pin(stream::iter(vec![
            //
            sitem_data(v2),
            sitem_data(v0),
            sitem_data(v4),
        ]));
        let deadline = Instant::now() + Duration::from_millis(2000);
        let mut binned_stream = crate::timebin::TimeBinnedStream::new(stream0, edges, true, deadline);
        while let Some(item) = binned_stream.next().await {
            eprintln!("{item:?}");
        }
        Ok(())
    };
    runfut(fut).unwrap()
}
