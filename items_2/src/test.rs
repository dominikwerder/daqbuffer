use crate::binsdim0::BinsDim0CollectedResult;
use crate::eventsdim0::EventsDim0;
use crate::merger::ChannelEventsMerger;
use crate::{binned_collected, ChannelEvents, Empty, Events, IsoDateTime};
use crate::{ConnStatus, ConnStatusEvent, Error};
use chrono::{TimeZone, Utc};
use futures_util::StreamExt;
use items::{RangeCompletableItem, Sitemty, StreamItem};
use netpod::timeunits::*;
use netpod::{AggKind, BinnedRange, NanoRange, ScalarType, Shape};
use std::time::Duration;

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
    };
    tokio::runtime::Runtime::new().unwrap().block_on(fut);
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
    };
    tokio::runtime::Runtime::new().unwrap().block_on(fut);
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
    };
    tokio::runtime::Runtime::new().unwrap().block_on(fut);
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
                                            coll = Some(ready.as_collectable_mut().new_collector(bin_count_exp));
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
                            coll = Some(ready.as_collectable_mut().new_collector(bin_count_exp));
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
    tokio::runtime::Runtime::new().unwrap().block_on(fut).unwrap();
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
    tokio::runtime::Runtime::new().unwrap().block_on(fut).unwrap();
}

#[test]
fn bin03() {
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
        let inp1 = futures_util::stream::iter(inp1).enumerate().then(|(i, k)| async move {
            if i == 4 {
                let _ = tokio::time::sleep(Duration::from_millis(10000)).await;
            }
            k
        });
        let edges = (0..10).into_iter().map(|x| TSBASE + SEC * 1 + SEC * x).collect();
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
    tokio::runtime::Runtime::new().unwrap().block_on(fut).unwrap();
}
