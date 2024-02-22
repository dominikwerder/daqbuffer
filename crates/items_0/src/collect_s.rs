use crate::timebin::TimeBinned;
use crate::AsAnyMut;
use crate::AsAnyRef;
use crate::Events;
use crate::TypeName;
use crate::WithLen;
use err::Error;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::BinnedRangeEnum;
use serde::Serialize;
use std::any;
use std::any::Any;
use std::fmt;

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

pub trait Collected: fmt::Debug + Send + AsAnyRef + WithLen + ToJsonResult {}

erased_serde::serialize_trait_object!(Collected);

impl ToJsonResult for Box<dyn Collected> {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        self.as_ref().to_json_result()
    }
}

impl WithLen for Box<dyn Collected> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

impl Collected for Box<dyn Collected> {}

// TODO rename to `Typed`
pub trait CollectorType: fmt::Debug + Send + Unpin + WithLen {
    type Input: Collectable;
    type Output: Collected + ToJsonResult + Serialize;

    fn ingest(&mut self, src: &mut Self::Input);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    fn set_continue_at_here(&mut self);

    // TODO use this crate's Error instead:
    fn result(&mut self, range: Option<SeriesRange>, binrange: Option<BinnedRangeEnum>) -> Result<Self::Output, Error>;
}

pub trait Collector: fmt::Debug + Send + Unpin + WithLen {
    fn ingest(&mut self, src: &mut dyn Collectable);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    fn set_continue_at_here(&mut self);
    // TODO factor the required parameters into new struct? Generic over events or binned?
    fn result(
        &mut self,
        range: Option<SeriesRange>,
        binrange: Option<BinnedRangeEnum>,
    ) -> Result<Box<dyn Collected>, Error>;
}

impl<T> Collector for T
where
    T: fmt::Debug + CollectorType + 'static,
{
    fn ingest(&mut self, src: &mut dyn Collectable) {
        if let Some(src) = src.as_any_mut().downcast_mut::<<T as CollectorType>::Input>() {
            trace!("sees incoming &mut ref");
            T::ingest(self, src)
        } else {
            if let Some(src) = src.as_any_mut().downcast_mut::<Box<<T as CollectorType>::Input>>() {
                trace!("sees incoming &mut Box");
                T::ingest(self, src)
            } else {
                error!(
                    "No idea what this is. Expect: {}  input {}  got: {} {:?}",
                    any::type_name::<T>(),
                    any::type_name::<<T as CollectorType>::Input>(),
                    src.type_name(),
                    src
                );
            }
        }
    }

    fn set_range_complete(&mut self) {
        T::set_range_complete(self)
    }

    fn set_timed_out(&mut self) {
        T::set_timed_out(self)
    }

    fn set_continue_at_here(&mut self) {
        T::set_continue_at_here(self)
    }

    fn result(
        &mut self,
        range: Option<SeriesRange>,
        binrange: Option<BinnedRangeEnum>,
    ) -> Result<Box<dyn Collected>, Error> {
        let ret = T::result(self, range, binrange)?;
        Ok(Box::new(ret))
    }
}

// TODO rename to `Typed`
pub trait CollectableType: fmt::Debug + WithLen + AsAnyRef + AsAnyMut + TypeName + Send {
    type Collector: CollectorType<Input = Self>;
    fn new_collector() -> Self::Collector;
}

pub trait Collectable: fmt::Debug + WithLen + AsAnyRef + AsAnyMut + TypeName + Send {
    fn new_collector(&self) -> Box<dyn Collector>;
}

impl TypeName for Box<dyn Events> {
    fn type_name(&self) -> String {
        self.as_ref().type_name()
    }
}

impl Collectable for Box<dyn Events> {
    fn new_collector(&self) -> Box<dyn Collector> {
        self.as_ref().new_collector()
    }
}

impl<T> Collectable for T
where
    T: CollectableType + 'static,
{
    fn new_collector(&self) -> Box<dyn Collector> {
        Box::new(T::new_collector())
    }
}

impl TypeName for Box<dyn Collectable> {
    fn type_name(&self) -> String {
        self.as_ref().type_name()
    }
}

// TODO do this with some blanket impl:
impl WithLen for Box<dyn Collectable> {
    fn len(&self) -> usize {
        WithLen::len(self.as_ref())
    }
}

// TODO do this with some blanket impl:
impl Collectable for Box<dyn Collectable> {
    fn new_collector(&self) -> Box<dyn Collector> {
        Collectable::new_collector(self.as_ref())
    }
}

impl WithLen for Box<dyn TimeBinned> {
    fn len(&self) -> usize {
        WithLen::len(self.as_ref())
    }
}

impl TypeName for Box<dyn TimeBinned> {
    fn type_name(&self) -> String {
        self.as_ref().type_name()
    }
}

impl Collectable for Box<dyn TimeBinned> {
    fn new_collector(&self) -> Box<dyn Collector> {
        self.as_ref().new_collector()
    }
}
