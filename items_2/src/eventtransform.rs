use items_0::Events;

pub trait EventTransform {
    fn transform(&mut self, src: Box<dyn Events>) -> Box<dyn Events>;
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
}
