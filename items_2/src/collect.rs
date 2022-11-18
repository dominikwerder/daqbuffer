use crate::Error;
use std::fmt;

pub trait Collector: fmt::Debug {
    type Input;
    type Output;

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
