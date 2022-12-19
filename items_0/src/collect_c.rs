use crate::collect_s::ToJsonBytes;
use crate::collect_s::ToJsonResult;
use crate::AsAnyMut;
use crate::AsAnyRef;
use crate::Events;
use crate::WithLen;
use err::Error;
use netpod::BinnedRange;
use netpod::NanoRange;
use std::fmt;

pub trait Collector: fmt::Debug + Send {
    fn len(&self) -> usize;
    fn ingest(&mut self, item: &mut dyn Collectable);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    fn result(&mut self, range: Option<NanoRange>, binrange: Option<BinnedRange>) -> Result<Box<dyn Collected>, Error>;
}

pub trait Collectable: fmt::Debug + AsAnyMut + crate::WithLen {
    fn new_collector(&self) -> Box<dyn Collector>;
}

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

    fn result(&mut self, range: Option<NanoRange>, binrange: Option<BinnedRange>) -> Result<Box<dyn Collected>, Error>;
}

pub trait CollectableWithDefault: AsAnyMut {
    fn new_collector(&self) -> Box<dyn CollectorDyn>;
}

impl crate::WithLen for Box<dyn Events> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
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

    fn result(
        &mut self,
        _range: Option<NanoRange>,
        _binrange: Option<BinnedRange>,
    ) -> Result<Box<dyn Collected>, Error> {
        todo!()
    }
}

impl WithLen for Box<dyn crate::TimeBinned> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

impl Collectable for Box<dyn crate::TimeBinned> {
    fn new_collector(&self) -> Box<dyn Collector> {
        self.as_ref().new_collector()
    }
}
