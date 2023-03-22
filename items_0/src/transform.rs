use std::pin;

use crate::Events;

pub struct TransformProperties {
    pub needs_one_before_range: bool,
    pub needs_value: bool,
}

pub trait EventTransform {
    fn query_transform_properties(&self) -> TransformProperties;
    fn transform(&mut self, src: Box<dyn Events>) -> Box<dyn Events>;
}

impl<T> EventTransform for Box<T>
where
    T: EventTransform,
{
    fn query_transform_properties(&self) -> TransformProperties {
        self.as_ref().query_transform_properties()
    }

    fn transform(&mut self, src: Box<dyn Events>) -> Box<dyn Events> {
        todo!()
    }
}

impl<T> EventTransform for pin::Pin<Box<T>>
where
    T: EventTransform,
{
    fn query_transform_properties(&self) -> TransformProperties {
        self.as_ref().query_transform_properties()
    }

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

impl EventTransform for IdentityTransform {
    fn transform(&mut self, src: Box<dyn Events>) -> Box<dyn Events> {
        src
    }

    fn query_transform_properties(&self) -> TransformProperties {
        todo!()
    }
}
