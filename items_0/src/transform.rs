use crate::collect_s::Collectable;
use crate::collect_s::Collected;
use crate::streamitem::RangeCompletableItem;
use crate::streamitem::Sitemty;
use crate::streamitem::StreamItem;
use crate::timebin::TimeBinnable;
use crate::Events;
use err::Error;
use futures_util::stream;
use futures_util::Future;
use futures_util::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub trait EventStreamTrait: Stream<Item = Sitemty<Box<dyn Events>>> + WithTransformProperties + Send {}

pub trait TimeBinnableStreamTrait:
    Stream<Item = Sitemty<Box<dyn TimeBinnable>>> + WithTransformProperties + Send
{
}

pub trait CollectableStreamTrait:
    Stream<Item = Sitemty<Box<dyn Collectable>>> + WithTransformProperties + Send
{
}

pub struct EventTransformProperties {
    pub needs_value: bool,
}

pub struct TransformProperties {
    pub needs_one_before_range: bool,
    pub needs_value: bool,
}

pub trait WithTransformProperties {
    fn query_transform_properties(&self) -> TransformProperties;
}

impl<T> WithTransformProperties for Box<T>
where
    T: WithTransformProperties,
{
    fn query_transform_properties(&self) -> TransformProperties {
        self.as_ref().query_transform_properties()
    }
}

impl<T> WithTransformProperties for Pin<Box<T>>
where
    T: WithTransformProperties,
{
    fn query_transform_properties(&self) -> TransformProperties {
        self.as_ref().query_transform_properties()
    }
}

pub trait EventTransform: WithTransformProperties + Send {
    fn transform(&mut self, src: Box<dyn Events>) -> Box<dyn Events>;
}

impl<T> EventTransform for Box<T>
where
    T: EventTransform,
{
    fn transform(&mut self, src: Box<dyn Events>) -> Box<dyn Events> {
        self.as_mut().transform(src)
    }
}

impl<T> EventTransform for Pin<Box<T>>
where
    T: EventTransform,
{
    fn transform(&mut self, src: Box<dyn Events>) -> Box<dyn Events> {
        todo!()
    }
}

pub struct IdentityTransform {}

impl IdentityTransform {
    pub fn default() -> Self {
        Self {}
    }
}

impl WithTransformProperties for IdentityTransform {
    fn query_transform_properties(&self) -> TransformProperties {
        todo!()
    }
}

impl EventTransform for IdentityTransform {
    fn transform(&mut self, src: Box<dyn Events>) -> Box<dyn Events> {
        src
    }
}

pub struct TransformEvent(pub Box<dyn EventTransform>);

impl WithTransformProperties for TransformEvent {
    fn query_transform_properties(&self) -> TransformProperties {
        self.0.query_transform_properties()
    }
}

impl EventTransform for TransformEvent {
    fn transform(&mut self, src: Box<dyn Events>) -> Box<dyn Events> {
        self.0.transform(src)
    }
}

impl<T> WithTransformProperties for stream::Iter<T> {
    fn query_transform_properties(&self) -> TransformProperties {
        todo!()
    }
}

impl<T> EventStreamTrait for stream::Iter<T> where
    T: core::iter::Iterator<Item = Sitemty<Box<(dyn Events + 'static)>>> + Send
{
}

pub struct EventStreamBox(pub Pin<Box<dyn EventStreamTrait>>);

impl<T> From<T> for EventStreamBox
where
    T: Events,
{
    fn from(value: T) -> Self {
        let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(Box::new(value) as _)));
        let x = stream::iter(vec![item]);
        Self(Box::pin(x))
    }
}

pub struct TimeBinnableStreamBox(pub Pin<Box<dyn TimeBinnableStreamTrait>>);

impl WithTransformProperties for TimeBinnableStreamBox {
    fn query_transform_properties(&self) -> TransformProperties {
        self.0.query_transform_properties()
    }
}

impl Stream for TimeBinnableStreamBox {
    type Item = <dyn TimeBinnableStreamTrait as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

impl TimeBinnableStreamTrait for TimeBinnableStreamBox {}

pub struct CollectableStreamBox(pub Pin<Box<dyn CollectableStreamTrait>>);

impl Stream for CollectableStreamBox {
    type Item = Sitemty<Box<dyn Collectable>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

impl WithTransformProperties for CollectableStreamBox {
    fn query_transform_properties(&self) -> TransformProperties {
        todo!()
    }
}

impl CollectableStreamTrait for CollectableStreamBox {}

impl<T> WithTransformProperties for stream::Empty<T> {
    fn query_transform_properties(&self) -> TransformProperties {
        todo!()
    }
}

impl<T> CollectableStreamTrait for stream::Empty<T>
where
    T: Send,
    stream::Empty<T>: Stream<Item = Sitemty<Box<dyn Collectable>>>,
{
}

impl<T> CollectableStreamTrait for Pin<Box<T>> where T: CollectableStreamTrait {}
