use crate::binned::WithLen;
use crate::streamlog::LogItem;
use err::Error;
use netpod::EventDataReadStats;
use serde::{Deserialize, Serialize};
use std::any::Any;

#[derive(Debug, Serialize, Deserialize)]
pub enum StatsItem {
    EventDataReadStats(EventDataReadStats),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StreamItem<T> {
    DataItem(T),
    Log(LogItem),
    Stats(StatsItem),
}

// TODO remove in favor of WithLen:
pub trait Bins {
    fn bin_count(&self) -> u32;
}

// TODO remove:
pub trait Collected {
    fn new(bin_count_exp: u32) -> Self;
    fn timed_out(&mut self, k: bool);
}

pub trait Collector {
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

// TODO can be removed?
pub trait Collectable2: Any {
    fn as_any_ref(&self) -> &dyn Any;
    fn append(&mut self, src: &dyn Any);
}

// TODO remove
pub trait CollectionSpec2 {
    // TODO Can I use here associated types and return concrete types?
    // Probably not object safe.
    fn empty(&self) -> Box<dyn Collectable2>;
}

// TODO rmove
pub trait CollectionSpecMaker2 {
    fn spec(bin_count_exp: u32) -> Box<dyn CollectionSpec2>;
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
