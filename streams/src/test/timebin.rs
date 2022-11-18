use crate::test::runfut;
use err::Error;
use futures_util::{stream, StreamExt};
use items::{sitem_data, RangeCompletableItem, StreamItem};
use items_2::binsdim0::BinsDim0;
use items_2::testgen::make_some_boxed_d0_f32;
use items_2::{ChannelEvents, ConnStatus, ConnStatusEvent};
use netpod::timeunits::{MS, SEC};
use std::collections::VecDeque;
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
        let mut exps = {
            let mut d = VecDeque::new();
            let mut bins = BinsDim0::empty();
            bins.push(SEC * 0, SEC * 1, 0, 0.0, 0.0, 0.0);
            bins.push(SEC * 1, SEC * 2, 2, 0.0535830, 100.0589, 50.05624);
            bins.push(SEC * 2, SEC * 3, 2, 200.06143, 300.07645, 250.06894);
            bins.push(SEC * 3, SEC * 4, 2, 400.08554, 500.05222, 450.06888);
            bins.push(SEC * 4, SEC * 5, 2, 600.0025, 700.09094, 650.04675);
            d.push_back(bins);
            let mut bins = BinsDim0::empty();
            bins.push(SEC * 5, SEC * 6, 2, 800.0619, 900.02844, 850.04517);
            d.push_back(bins);
            d
        };
        let deadline = Instant::now() + Duration::from_millis(2000000);
        let mut binned_stream = crate::timebin::TimeBinnedStream::new(stream0, edges, true, deadline);
        while let Some(item) = binned_stream.next().await {
            eprintln!("{item:?}");
            match item {
                Ok(item) => match item {
                    StreamItem::DataItem(item) => match item {
                        RangeCompletableItem::Data(item) => {
                            if let Some(item) = item.as_any().downcast_ref::<BinsDim0<f32>>() {
                                let exp = exps.pop_front().unwrap();
                                if !item.equal_slack(&exp) {
                                    return Err(Error::with_msg_no_trace(format!("bad, content not equal")));
                                }
                            } else {
                                return Err(Error::with_msg_no_trace(format!("bad, got item with unexpected type")));
                            }
                        }
                        RangeCompletableItem::RangeComplete => {}
                    },
                    StreamItem::Log(_) => {}
                    StreamItem::Stats(_) => {}
                },
                Err(e) => Err(e).unwrap(),
            }
        }
        Ok(())
    };
    runfut(fut).unwrap()
}
