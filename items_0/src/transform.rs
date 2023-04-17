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
use std::pin::Pin;

pub trait EventStreamTrait: Stream<Item = Sitemty<Box<dyn Events>>> + WithTransformProperties + Send {}

pub trait TimeBinnableStreamTrait:
    Stream<Item = Sitemty<Box<dyn TimeBinnable>>> + WithTransformProperties + Send
{
}

pub trait CollectableStreamTrait:
    Stream<Item = Sitemty<Box<dyn Collectable>>> + WithTransformProperties + Send
{
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

pub trait EventTransform: WithTransformProperties {
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

pub struct EventStream(pub Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Events>>> + Send>>);

impl<T> From<T> for EventStream
where
    T: Events,
{
    fn from(value: T) -> Self {
        let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(Box::new(value) as _)));
        let x = stream::iter(vec![item]);
        EventStream(Box::pin(x))
    }
}

pub struct CollectableStream(pub Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Collectable>>> + Send>>);
