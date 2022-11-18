use crate::binsdim0::BinsDim0CollectedResult;
use crate::eventsdim0::EventsDim0;
use crate::merger::{Mergeable, Merger};
use crate::merger_cev::ChannelEventsMerger;
use crate::testgen::make_some_boxed_d0_f32;
use crate::{binned_collected, runfut, ChannelEvents, Empty, Events, IsoDateTime};
use crate::{ConnStatus, ConnStatusEvent, Error};
use chrono::{TimeZone, Utc};
use futures_util::{stream, StreamExt};
use items::{sitem_data, RangeCompletableItem, Sitemty, StreamItem};
use netpod::log::*;
use netpod::timeunits::*;
use netpod::{AggKind, BinnedRange, NanoRange, ScalarType, Shape};
use std::time::Duration;

#[test]
fn items_move_events() {
    let evs = make_some_boxed_d0_f32(10, SEC, SEC, 0, 1846713782);
    let v0 = ChannelEvents::Events(evs);
    let mut v1 = v0.clone();
    eprintln!("{v1:?}");
    eprintln!("{}", v1.len());
    let mut v2 = v1.move_into_fresh(4);
    eprintln!("{}", v1.len());
    eprintln!("{}", v2.len());
    v1.move_into_existing(&mut v2, u64::MAX).unwrap();
    eprintln!("{}", v1.len());
    eprintln!("{}", v2.len());
    eprintln!("{v1:?}");
    eprintln!("{v2:?}");
    assert_eq!(v1.len(), 0);
    assert_eq!(v2.len(), 10);
    assert_eq!(v2, v0);
}

#[test]
fn items_merge_00() {
    let fut = async {
        use crate::merger::Merger;
        let evs0 = make_some_boxed_d0_f32(10, SEC * 1, SEC * 2, 0, 1846713782);
        let evs1 = make_some_boxed_d0_f32(10, SEC * 2, SEC * 2, 0, 828764893);
        let v0 = ChannelEvents::Events(evs0);
        let v1 = ChannelEvents::Events(evs1);
        let stream0 = Box::pin(stream::iter(vec![sitem_data(v0)]));
        let stream1 = Box::pin(stream::iter(vec![sitem_data(v1)]));
        let mut merger = Merger::new(vec![stream0, stream1], 8);
        while let Some(item) = merger.next().await {
            eprintln!("{item:?}");
        }
        Ok(())
    };
    runfut(fut).unwrap();
}

#[test]
fn items_merge_01() {
    let fut = async {
        use crate::merger::Merger;
        let evs0 = make_some_boxed_d0_f32(10, SEC * 1, SEC * 2, 0, 1846713782);
        let evs1 = make_some_boxed_d0_f32(10, SEC * 2, SEC * 2, 0, 828764893);
        let v0 = ChannelEvents::Events(evs0);
        let v1 = ChannelEvents::Events(evs1);
        let v2 = ChannelEvents::Status(ConnStatusEvent::new(MS * 100, ConnStatus::Connect));
        let v3 = ChannelEvents::Status(ConnStatusEvent::new(MS * 2300, ConnStatus::Disconnect));
        let v4 = ChannelEvents::Status(ConnStatusEvent::new(MS * 2800, ConnStatus::Connect));
        let stream0 = Box::pin(stream::iter(vec![sitem_data(v0)]));
        let stream1 = Box::pin(stream::iter(vec![sitem_data(v1)]));
        let stream2 = Box::pin(stream::iter(vec![sitem_data(v2), sitem_data(v3), sitem_data(v4)]));
        let mut merger = Merger::new(vec![stream0, stream1, stream2], 8);
        let mut total_event_count = 0;
        while let Some(item) = merger.next().await {
            eprintln!("{item:?}");
            let item = item?;
            match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => {}
                    RangeCompletableItem::Data(item) => {
                        total_event_count += item.len();
                    }
                },
                StreamItem::Log(_) => {}
                StreamItem::Stats(_) => {}
            }
        }
        assert_eq!(total_event_count, 23);
        Ok(())
    };
    runfut(fut).unwrap();
}

#[test]
fn items_merge_02() {
    let fut = async {
        let evs0 = make_some_boxed_d0_f32(100, SEC * 1, SEC * 2, 0, 1846713782);
        let evs1 = make_some_boxed_d0_f32(100, SEC * 2, SEC * 2, 0, 828764893);
        let v0 = ChannelEvents::Events(evs0);
        let v1 = ChannelEvents::Events(evs1);
        let v2 = ChannelEvents::Status(ConnStatusEvent::new(MS * 100, ConnStatus::Connect));
        let v3 = ChannelEvents::Status(ConnStatusEvent::new(MS * 2300, ConnStatus::Disconnect));
        let v4 = ChannelEvents::Status(ConnStatusEvent::new(MS * 2800, ConnStatus::Connect));
        let stream0 = Box::pin(stream::iter(vec![sitem_data(v0)]));
        let stream1 = Box::pin(stream::iter(vec![sitem_data(v1)]));
        let stream2 = Box::pin(stream::iter(vec![sitem_data(v2), sitem_data(v3), sitem_data(v4)]));
        let mut merger = Merger::new(vec![stream0, stream1, stream2], 8);
        let mut total_event_count = 0;
        while let Some(item) = merger.next().await {
            eprintln!("{item:?}");
            let item = item.unwrap();
            match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => {}
                    RangeCompletableItem::Data(item) => {
                        total_event_count += item.len();
                    }
                },
                StreamItem::Log(_) => {}
                StreamItem::Stats(_) => {}
            }
        }
        assert_eq!(total_event_count, 203);
        Ok(())
    };
    runfut(fut).unwrap();
}

#[test]
fn merge01() {
    let fut = async {
        let mut events_vec1: Vec<Sitemty<ChannelEvents>> = Vec::new();
        let mut events_vec2: Vec<Sitemty<ChannelEvents>> = Vec::new();
        {
            let mut events = EventsDim0::empty();
            for i in 0..10 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            let cev = ChannelEvents::Events(Box::new(events.clone()));
            events_vec1.push(Ok(StreamItem::DataItem(RangeCompletableItem::Data(cev))));
            let cev = ChannelEvents::Events(Box::new(events.clone()));
            events_vec2.push(Ok(StreamItem::DataItem(RangeCompletableItem::Data(cev))));
        }
        let inp1 = events_vec1;
        let inp1 = futures_util::stream::iter(inp1);
        let inp1 = Box::pin(inp1);
        let inp2: Vec<Sitemty<ChannelEvents>> = Vec::new();
        let inp2 = futures_util::stream::iter(inp2);
        let inp2 = Box::pin(inp2);
        let mut merger = ChannelEventsMerger::new(vec![inp1, inp2]);
        let item = merger.next().await;
        assert_eq!(item.as_ref(), events_vec2.get(0));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), None);
        Ok(())
    };
    runfut(fut).unwrap();
}

#[test]
fn merge02() {
    let fut = async {
        let events_vec1 = {
            let mut vec = Vec::new();
            let mut events = EventsDim0::empty();
            for i in 0..10 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events.clone()));
            let mut events = EventsDim0::empty();
            for i in 10..20 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events.clone()));
            vec
        };
        let exp = events_vec1.clone();
        let inp1 = events_vec1;
        let inp1 = futures_util::stream::iter(inp1);
        let inp1 = Box::pin(inp1);
        let inp2: Vec<Sitemty<ChannelEvents>> = Vec::new();
        let inp2 = futures_util::stream::iter(inp2);
        let inp2 = Box::pin(inp2);
        let mut merger = ChannelEventsMerger::new(vec![inp1, inp2]);
        let item = merger.next().await;
        assert_eq!(item.as_ref(), exp.get(0));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), exp.get(1));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), None);
        Ok(())
    };
    runfut(fut).unwrap();
}

fn push_evd0(vec: &mut Vec<Sitemty<ChannelEvents>>, events: Box<dyn Events>) {
    let cev = ChannelEvents::Events(events);
    vec.push(Ok(StreamItem::DataItem(RangeCompletableItem::Data(cev))));
}

#[test]
fn merge03() {
    let fut = async {
        let events_vec1 = {
            let mut vec = Vec::new();
            let mut events = EventsDim0::empty();
            for i in 0..10 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events));
            let mut events = EventsDim0::empty();
            for i in 10..20 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events));
            vec
        };
        let events_vec2 = {
            let mut vec = Vec::new();
            let mut events = EventsDim0::empty();
            for i in 0..10 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events));
            let mut events = EventsDim0::empty();
            for i in 10..12 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events));
            let mut events = EventsDim0::empty();
            for i in 12..20 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events));
            vec
        };

        let inp2_events_a = {
            let ev = ConnStatusEvent {
                ts: 1199,
                status: ConnStatus::Disconnect,
            };
            let item: Sitemty<ChannelEvents> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                ChannelEvents::Status(ev),
            )));
            vec![item]
        };

        let inp2_events_b = {
            let ev = ConnStatusEvent {
                ts: 1199,
                status: ConnStatus::Disconnect,
            };
            let item: Sitemty<ChannelEvents> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                ChannelEvents::Status(ev),
            )));
            vec![item]
        };

        let inp1 = events_vec1;
        let inp1 = futures_util::stream::iter(inp1);
        let inp1 = Box::pin(inp1);
        let inp2: Vec<Sitemty<ChannelEvents>> = inp2_events_a;
        let inp2 = futures_util::stream::iter(inp2);
        let inp2 = Box::pin(inp2);
        let mut merger = ChannelEventsMerger::new(vec![inp1, inp2]);
        let item = merger.next().await;
        assert_eq!(item.as_ref(), events_vec2.get(0));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), events_vec2.get(1));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), inp2_events_b.get(0));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), events_vec2.get(2));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), None);
        Ok(())
    };
    runfut(fut).unwrap();
}

#[test]
fn bin01() {
    let fut = async {
        let inp1 = {
            let mut vec = Vec::new();
            for j in 0..2 {
                let mut events = EventsDim0::empty();
                for i in 10 * j..10 * (1 + j) {
                    events.push(SEC * i, i, 17f32);
                }
                push_evd0(&mut vec, Box::new(events));
            }
            vec
        };
        let inp1 = futures_util::stream::iter(inp1);
        let inp1 = Box::pin(inp1);
        let inp2 = Box::pin(futures_util::stream::empty()) as _;
        let mut stream = ChannelEventsMerger::new(vec![inp1, inp2]);
        let mut coll = None;
        let mut binner = None;
        let edges: Vec<_> = (0..10).into_iter().map(|t| SEC * 10 * t).collect();
        // TODO implement continue-at [hcn2956jxhwsf]
        #[allow(unused)]
        let bin_count_exp = (edges.len() - 1) as u32;
        let do_time_weight = true;
        while let Some(item) = stream.next().await {
            let item = item?;
            match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => todo!(),
                    RangeCompletableItem::Data(item) => match item {
                        ChannelEvents::Events(events) => {
                            if binner.is_none() {
                                let bb = events.as_time_binnable().time_binner_new(edges.clone(), do_time_weight);
                                binner = Some(bb);
                            }
                            let binner = binner.as_mut().unwrap();
                            binner.ingest(events.as_time_binnable());
                            eprintln!("bins_ready_count: {}", binner.bins_ready_count());
                            if binner.bins_ready_count() > 0 {
                                let ready = binner.bins_ready();
                                match ready {
                                    Some(mut ready) => {
                                        eprintln!("ready {ready:?}");
                                        if coll.is_none() {
                                            coll = Some(ready.as_collectable_mut().new_collector());
                                        }
                                        let cl = coll.as_mut().unwrap();
                                        cl.ingest(ready.as_collectable_mut());
                                    }
                                    None => {
                                        return Err(format!("bins_ready_count but no result").into());
                                    }
                                }
                            }
                        }
                        ChannelEvents::Status(_) => {
                            eprintln!("TODO Status");
                        }
                    },
                },
                StreamItem::Log(_) => {
                    eprintln!("TODO Log");
                }
                StreamItem::Stats(_) => {
                    eprintln!("TODO Stats");
                }
            }
        }
        if let Some(mut binner) = binner {
            binner.cycle();
            // TODO merge with the same logic above in the loop.
            if binner.bins_ready_count() > 0 {
                let ready = binner.bins_ready();
                match ready {
                    Some(mut ready) => {
                        eprintln!("ready {ready:?}");
                        if coll.is_none() {
                            coll = Some(ready.as_collectable_mut().new_collector());
                        }
                        let cl = coll.as_mut().unwrap();
                        cl.ingest(ready.as_collectable_mut());
                    }
                    None => {
                        return Err(format!("bins_ready_count but no result").into());
                    }
                }
            }
        }
        match coll {
            Some(mut coll) => {
                let res = coll.result().map_err(|e| format!("{e}"))?;
                //let res = res.to_json_result().map_err(|e| format!("{e}"))?;
                //let res = res.to_json_bytes().map_err(|e| format!("{e}"))?;
                eprintln!("res {res:?}");
            }
            None => {
                panic!();
            }
        }
        Ok::<_, Error>(())
    };
    runfut(fut).unwrap();
}

#[test]
fn bin02() {
    const TSBASE: u64 = SEC * 1600000000;
    fn val(ts: u64) -> f32 {
        2f32 + ((ts / SEC) % 2) as f32 + 0.2 * ((ts / (MS * 100)) % 2) as f32
    }
    let fut = async {
        let mut events_vec1 = Vec::new();
        let mut t = TSBASE;
        for _ in 0..20 {
            let mut events = EventsDim0::empty();
            for _ in 0..10 {
                events.push(t, t, val(t));
                t += MS * 100;
            }
            let cev = ChannelEvents::Events(Box::new(events));
            events_vec1.push(Ok(StreamItem::DataItem(RangeCompletableItem::Data(cev))));
        }
        events_vec1.push(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)));
        let inp1 = events_vec1;
        let inp1 = futures_util::stream::iter(inp1);
        let inp1 = Box::pin(inp1);
        let inp2 = Box::pin(futures_util::stream::empty()) as _;
        let stream = ChannelEventsMerger::new(vec![inp1, inp2]);
        if false {
            // covering_range result is subject to adjustments, instead, manually choose bin edges
            let range = NanoRange {
                beg: TSBASE + SEC * 1,
                end: TSBASE + SEC * 10,
            };
            let covering = BinnedRange::covering_range(range, 3).map_err(|e| format!("{e}"))?;
            assert_eq!(covering.edges().len(), 6);
        }
        let edges = (0..10).into_iter().map(|x| TSBASE + SEC * 1 + SEC * x).collect();
        let stream = Box::pin(stream);
        let collected = binned_collected(
            ScalarType::F32,
            Shape::Scalar,
            AggKind::TimeWeightedScalar,
            edges,
            Duration::from_millis(2000),
            stream,
        )
        .await?;
        eprintln!("collected {:?}", collected);
        Ok::<_, Error>(())
    };
    runfut(fut).unwrap();
}

#[test]
fn binned_timeout_01() {
    trace!("binned_timeout_01 uses a delay");
    const TSBASE: u64 = SEC * 1600000000;
    fn val(ts: u64) -> f32 {
        2f32 + ((ts / SEC) % 2) as f32 + 0.2 * ((ts / (MS * 100)) % 2) as f32
    }
    eprintln!("binned_timeout_01  ENTER");
    let fut = async {
        eprintln!("binned_timeout_01  IN FUT");
        let mut events_vec1 = Vec::new();
        let mut t = TSBASE;
        for _ in 0..20 {
            let mut events = EventsDim0::empty();
            for _ in 0..10 {
                events.push(t, t, val(t));
                t += MS * 100;
            }
            let cev = ChannelEvents::Events(Box::new(events));
            events_vec1.push(Ok(StreamItem::DataItem(RangeCompletableItem::Data(cev))));
        }
        events_vec1.push(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)));
        let inp1 = events_vec1;
        let inp1 = futures_util::stream::iter(inp1).enumerate().then(|(i, k)| async move {
            if i == 5 {
                let _ = tokio::time::sleep(Duration::from_millis(10000)).await;
            }
            k
        });
        let edges = (0..10).into_iter().map(|x| TSBASE + SEC * (1 + x)).collect();
        let inp1 = Box::pin(inp1) as _;
        let timeout = Duration::from_millis(400);
        let res = binned_collected(
            ScalarType::F32,
            Shape::Scalar,
            AggKind::TimeWeightedScalar,
            edges,
            timeout,
            inp1,
        )
        .await?;
        let r2: &BinsDim0CollectedResult<f32> = res.as_any().downcast_ref().expect("res seems wrong type");
        assert_eq!(SEC * r2.ts_anchor_sec(), TSBASE + SEC);
        assert_eq!(r2.counts(), &[10, 10, 10]);
        assert_eq!(r2.mins(), &[3.0, 2.0, 3.0]);
        assert_eq!(r2.maxs(), &[3.2, 2.2, 3.2]);
        assert_eq!(r2.missing_bins(), 6);
        assert_eq!(
            r2.continue_at(),
            Some(IsoDateTime(Utc.timestamp_nanos((TSBASE + SEC * 4) as i64)))
        );
        Ok::<_, Error>(())
    };
    runfut(fut).unwrap();
}
