use crate::eventsdim0::EventsDim0;
use crate::{ChannelEvents, ChannelEventsMerger, ConnStatus, Empty};
use crate::{ConnStatusEvent, Error};
use futures_util::StreamExt;

#[test]
fn merge01() {
    let fut = async {
        let mut events_vec1 = Vec::new();
        let mut events_vec2 = Vec::new();
        {
            let mut events = EventsDim0::empty();
            for i in 0..10 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            events_vec1.push(Ok(ChannelEvents::Events(Box::new(events.clone()))));
            events_vec2.push(Ok(ChannelEvents::Events(Box::new(events.clone()))));
        }
        let inp1 = events_vec1;
        let inp1 = futures_util::stream::iter(inp1);
        let inp1 = Box::pin(inp1);
        let inp2: Vec<Result<ChannelEvents, Error>> = Vec::new();
        let inp2 = futures_util::stream::iter(inp2);
        let inp2 = Box::pin(inp2);
        let mut merger = ChannelEventsMerger::new(inp1, inp2);
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
        let mut events_vec1 = Vec::new();
        let mut events_vec2 = Vec::new();
        {
            let mut events = EventsDim0::empty();
            for i in 0..10 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            events_vec1.push(Ok(ChannelEvents::Events(Box::new(events.clone()))));
            events_vec2.push(Ok(ChannelEvents::Events(Box::new(events.clone()))));
        }
        {
            let mut events = EventsDim0::empty();
            for i in 10..20 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            events_vec1.push(Ok(ChannelEvents::Events(Box::new(events.clone()))));
            events_vec2.push(Ok(ChannelEvents::Events(Box::new(events.clone()))));
        }
        let inp1 = events_vec1;
        let inp1 = futures_util::stream::iter(inp1);
        let inp1 = Box::pin(inp1);
        let inp2: Vec<Result<ChannelEvents, Error>> = Vec::new();
        let inp2 = futures_util::stream::iter(inp2);
        let inp2 = Box::pin(inp2);
        let mut merger = ChannelEventsMerger::new(inp1, inp2);
        let item = merger.next().await;
        assert_eq!(item.as_ref(), events_vec2.get(0));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), events_vec2.get(1));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), None);
    };
    tokio::runtime::Runtime::new().unwrap().block_on(fut);
}

#[test]
fn merge03() {
    let fut = async {
        let mut events_vec1 = Vec::new();
        {
            let mut events = EventsDim0::empty();
            for i in 0..10 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            events_vec1.push(Ok(ChannelEvents::Events(Box::new(events))));
            let mut events = EventsDim0::empty();
            for i in 10..20 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            events_vec1.push(Ok(ChannelEvents::Events(Box::new(events.clone()))));
        }
        let events_vec1 = events_vec1;
        let mut events_vec2 = Vec::new();
        {
            let mut events = EventsDim0::empty();
            for i in 0..10 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            events_vec2.push(Ok(ChannelEvents::Events(Box::new(events.clone()))));
            let mut events = EventsDim0::empty();
            for i in 10..12 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            events_vec2.push(Ok(ChannelEvents::Events(Box::new(events.clone()))));
            let mut events = EventsDim0::empty();
            for i in 12..20 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            events_vec2.push(Ok(ChannelEvents::Events(Box::new(events.clone()))));
        }
        let events_vec2 = events_vec2;

        let inp2_events_a: Vec<Result<_, Error>> = vec![Ok(ChannelEvents::Status(ConnStatusEvent {
            ts: 1199,
            status: ConnStatus::Disconnect,
        }))];
        let inp2_events_b: Vec<Result<_, Error>> = vec![Ok(ChannelEvents::Status(ConnStatusEvent {
            ts: 1199,
            status: ConnStatus::Disconnect,
        }))];

        let inp1 = events_vec1;
        let inp1 = futures_util::stream::iter(inp1);
        let inp1 = Box::pin(inp1);
        let inp2: Vec<Result<ChannelEvents, Error>> = inp2_events_a;
        let inp2 = futures_util::stream::iter(inp2);
        let inp2 = Box::pin(inp2);
        let mut merger = ChannelEventsMerger::new(inp1, inp2);
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
