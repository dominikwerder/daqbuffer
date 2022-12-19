use crate::test::runfut;
use err::Error;
use futures_util::stream;
use items::sitem_data;
use items_2::eventsdim0::EventsDim0CollectorOutput;
use items_2::testgen::make_some_boxed_d0_f32;
use netpod::timeunits::SEC;
use std::time::{Duration, Instant};

#[test]
fn collect_channel_events() -> Result<(), Error> {
    let fut = async {
        let evs0 = make_some_boxed_d0_f32(20, SEC * 10, SEC * 1, 0, 28736487);
        let evs1 = make_some_boxed_d0_f32(20, SEC * 30, SEC * 1, 0, 882716583);
        let stream = stream::iter(vec![sitem_data(evs0), sitem_data(evs1)]);
        let deadline = Instant::now() + Duration::from_millis(4000);
        let events_max = 10000;
        let res = crate::collect::collect(stream, deadline, events_max, None, None).await?;
        //eprintln!("collected result: {res:?}");
        if let Some(res) = res.as_any_ref().downcast_ref::<EventsDim0CollectorOutput<f32>>() {
            eprintln!("Great, a match");
            eprintln!("{res:?}");
            assert_eq!(res.len(), 40);
        } else {
            return Err(Error::with_msg(format!("bad type of collected result")));
        }
        Ok(())
    };
    runfut(fut)
}
