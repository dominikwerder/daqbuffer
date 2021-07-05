use crate::binned::WithLen;
use err::Error;
use serde::Serialize;

pub trait Collector: Send + Unpin + WithLen {
    type Input: Collectable;
    type Output: Serialize;
    fn ingest(&mut self, src: &Self::Input);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    fn result(self) -> Result<Self::Output, Error>;
}

pub trait Collectable {
    type Collector: Collector<Input = Self>;
    fn new_collector(bin_count_exp: u32) -> Self::Collector;
}

pub trait ToJsonBytes {
    fn to_json_bytes(&self) -> Result<Vec<u8>, Error>;
}

pub trait ToJsonResult {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error>;
}

pub trait Appendable: WithLen {
    fn empty() -> Self;
    fn append(&mut self, src: &Self);
}
