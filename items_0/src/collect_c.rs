use crate::collect_s::ToJsonBytes;
use crate::collect_s::ToJsonResult;
use crate::AsAnyMut;
use crate::AsAnyRef;
use crate::Events;
use err::Error;
use std::fmt;

pub trait Collector: fmt::Debug + Send {
    fn len(&self) -> usize;
    fn ingest(&mut self, item: &mut dyn Collectable);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    fn result(&mut self) -> Result<Box<dyn Collected>, Error>;
}

pub trait Collectable: fmt::Debug + AsAnyMut {
    fn new_collector(&self) -> Box<dyn Collector>;
}

// TODO can this get removed?
pub trait Collected: fmt::Debug + ToJsonResult + AsAnyRef + Send {}

erased_serde::serialize_trait_object!(Collected);

impl ToJsonResult for Box<dyn Collected> {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        self.as_ref().to_json_result()
    }
}

impl Collected for Box<dyn Collected> {}

#[derive(Debug)]
pub struct CollectorDynDefault {}

// TODO remove?
pub trait CollectorDyn: fmt::Debug + Send {
    fn len(&self) -> usize;

    fn ingest(&mut self, item: &mut dyn CollectableWithDefault);

    fn set_range_complete(&mut self);

    fn set_timed_out(&mut self);

    fn result(&mut self) -> Result<Box<dyn Collected>, Error>;
}

pub trait CollectableWithDefault: AsAnyMut {
    fn new_collector(&self) -> Box<dyn CollectorDyn>;
}

impl Collectable for Box<dyn Events> {
    fn new_collector(&self) -> Box<dyn Collector> {
        todo!()
    }
}

#[derive(Debug)]
pub struct TimeBinnedCollector {}

impl Collector for TimeBinnedCollector {
    fn len(&self) -> usize {
        todo!()
    }

    fn ingest(&mut self, _item: &mut dyn Collectable) {
        todo!()
    }

    fn set_range_complete(&mut self) {
        todo!()
    }

    fn set_timed_out(&mut self) {
        todo!()
    }

    fn result(&mut self) -> Result<Box<dyn Collected>, Error> {
        todo!()
    }
}

impl Collectable for Box<dyn crate::TimeBinned> {
    fn new_collector(&self) -> Box<dyn Collector> {
        self.as_ref().new_collector()
    }
}
