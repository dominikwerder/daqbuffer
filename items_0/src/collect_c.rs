use crate::collect_s::ToJsonBytes;
use crate::collect_s::ToJsonResult;
use crate::AsAnyRef;
use crate::Events;
use err::Error;
use std::any::Any;
use std::fmt;

pub trait Collector: fmt::Debug + Send {
    // TODO should require here Collectable?
    type Input;
    type Output: Collected;

    fn len(&self) -> usize;

    fn ingest(&mut self, item: &mut Self::Input);

    fn set_range_complete(&mut self);

    fn set_timed_out(&mut self);

    fn result(&mut self) -> Result<Self::Output, Error>;
}

pub trait Collectable: fmt::Debug {
    type Collector: Collector<Input = Self>;

    fn new_collector(&self) -> Self::Collector;
}

pub trait Collected: fmt::Debug + ToJsonResult + AsAnyRef + Send {}

erased_serde::serialize_trait_object!(Collected);

impl AsAnyRef for Box<dyn Collected> {
    fn as_any_ref(&self) -> &dyn Any {
        self.as_ref().as_any_ref()
    }
}

impl ToJsonResult for Box<dyn Collected> {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        self.as_ref().to_json_result()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Collected for Box<dyn Collected> {}

#[derive(Debug)]
pub struct CollectorDynDefault {}

pub trait CollectorDyn: fmt::Debug + Send {
    fn len(&self) -> usize;

    fn ingest(&mut self, item: &mut dyn CollectableWithDefault);

    fn set_range_complete(&mut self);

    fn set_timed_out(&mut self);

    fn result(&mut self) -> Result<Box<dyn Collected>, Error>;
}

pub trait CollectableWithDefault {
    fn new_collector(&self) -> Box<dyn CollectorDyn>;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[derive(Debug)]
pub struct EventsCollector {
    coll: Box<dyn CollectorDyn>,
}

impl EventsCollector {
    pub fn new(coll: Box<dyn CollectorDyn>) -> Self {
        Self { coll }
    }
}

impl Collector for EventsCollector {
    type Input = Box<dyn Events>;

    // TODO this Output trait does not differentiate between e.g. collected events, collected bins, different aggs, etc...
    type Output = Box<dyn Collected>;

    fn len(&self) -> usize {
        self.coll.len()
    }

    fn ingest(&mut self, item: &mut Self::Input) {
        self.coll.ingest(item.as_collectable_with_default_mut());
    }

    fn set_range_complete(&mut self) {
        self.coll.set_range_complete()
    }

    fn set_timed_out(&mut self) {
        self.coll.set_timed_out()
    }

    fn result(&mut self) -> Result<Self::Output, Error> {
        self.coll.result()
    }
}

impl Collectable for Box<dyn Events> {
    type Collector = EventsCollector;

    fn new_collector(&self) -> Self::Collector {
        let coll = CollectableWithDefault::new_collector(self.as_ref());
        EventsCollector::new(coll)
    }
}
