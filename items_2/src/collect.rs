use crate::AsAnyRef;
use crate::Error;
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

pub trait Collected: fmt::Debug + crate::streams::ToJsonResult + AsAnyRef + Send {}

erased_serde::serialize_trait_object!(Collected);

impl AsAnyRef for Box<dyn Collected> {
    fn as_any_ref(&self) -> &dyn Any {
        self.as_ref().as_any_ref()
    }
}

impl crate::streams::ToJsonResult for Box<dyn Collected> {
    fn to_json_result(&self) -> Result<Box<dyn crate::streams::ToJsonBytes>, err::Error> {
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
