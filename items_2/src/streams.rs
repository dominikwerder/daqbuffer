use crate::WithLen;
use err::Error;
use serde::Serialize;
use std::any::Any;
use std::fmt;

pub trait CollectorType: Send + Unpin + WithLen {
    type Input: Collectable;
    type Output: ToJsonResult + Serialize;

    fn ingest(&mut self, src: &mut Self::Input);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    fn result(&mut self) -> Result<Self::Output, Error>;
}

pub trait Collector: Send + Unpin + WithLen {
    fn ingest(&mut self, src: &mut dyn Collectable);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    fn result(&mut self) -> Result<Box<dyn ToJsonResult>, Error>;
}

pub trait CollectableType {
    type Collector: CollectorType<Input = Self>;

    fn new_collector(bin_count_exp: u32) -> Self::Collector;
}

pub trait Collectable: Any {
    fn new_collector(&self, bin_count_exp: u32) -> Box<dyn Collector>;
    fn as_any_mut(&mut self) -> &mut dyn Any;
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

impl<T: CollectableType + 'static> Collectable for T {
    fn new_collector(&self, bin_count_exp: u32) -> Box<dyn Collector> {
        Box::new(T::new_collector(bin_count_exp)) as _
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        // TODO interesting: why exactly does returning `&mut self` not work here?
        self
    }
}

// TODO check usage of this trait
pub trait ToJsonBytes {
    fn to_json_bytes(&self) -> Result<Vec<u8>, Error>;
}

// TODO check usage of this trait
pub trait ToJsonResult: fmt::Debug + Send {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error>;
    fn as_any(&self) -> &dyn Any;
}

impl ToJsonResult for serde_json::Value {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        Ok(Box::new(self.clone()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ToJsonBytes for serde_json::Value {
    fn to_json_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(self)?)
    }
}
