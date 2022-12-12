use super::collect_c::Collected;
use crate::{AsAnyMut, AsAnyRef, WithLen};
use err::Error;
use serde::Serialize;
use std::any::Any;
use std::fmt;

// TODO rename to `Typed`
pub trait CollectorType: Send + Unpin + WithLen {
    type Input: Collectable;
    type Output: Collected + ToJsonResult + Serialize;

    fn ingest(&mut self, src: &mut Self::Input);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);

    // TODO use this crate's Error instead:
    fn result(&mut self) -> Result<Self::Output, Error>;
}

pub trait Collector: Send + Unpin + WithLen {
    fn ingest(&mut self, src: &mut dyn Collectable);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    fn result(&mut self) -> Result<Box<dyn ToJsonResult>, Error>;
}

// TODO rename to `Typed`
pub trait CollectableType: AsAnyRef + AsAnyMut {
    type Collector: CollectorType<Input = Self>;
    fn new_collector() -> Self::Collector;
}

pub trait Collectable: AsAnyRef + AsAnyMut + Any {
    fn new_collector(&self) -> Box<dyn Collector>;
}

impl<T: CollectorType + 'static> Collector for T {
    fn ingest(&mut self, src: &mut dyn Collectable) {
        let src: &mut <T as CollectorType>::Input = src.as_any_mut().downcast_mut().expect("can not downcast");
        T::ingest(self, src)
    }

    fn set_range_complete(&mut self) {
        T::set_range_complete(self)
    }

    fn set_timed_out(&mut self) {
        T::set_timed_out(self)
    }

    fn result(&mut self) -> Result<Box<dyn ToJsonResult>, Error> {
        let ret = T::result(self)?;
        Ok(Box::new(ret) as _)
    }
}

impl<T> Collectable for T
where
    T: CollectableType + 'static,
{
    fn new_collector(&self) -> Box<dyn Collector> {
        Box::new(T::new_collector()) as _
    }
}

// TODO check usage of this trait
pub trait ToJsonBytes {
    fn to_json_bytes(&self) -> Result<Vec<u8>, Error>;
}

// TODO check usage of this trait
pub trait ToJsonResult: erased_serde::Serialize + fmt::Debug + AsAnyRef + AsAnyMut + Send {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error>;
}

erased_serde::serialize_trait_object!(ToJsonResult);

impl AsAnyRef for serde_json::Value {
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl AsAnyMut for serde_json::Value {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl ToJsonResult for serde_json::Value {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        Ok(Box::new(self.clone()))
    }
}

impl ToJsonBytes for serde_json::Value {
    fn to_json_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(self)?)
    }
}

// TODO do this with some blanket impl:
impl Collectable for Box<dyn Collectable> {
    fn new_collector(&self) -> Box<dyn Collector> {
        Collectable::new_collector(self.as_ref())
    }
}
