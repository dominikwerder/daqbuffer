use crate::binned::WithLen;
use crate::streamlog::LogItem;
use err::Error;
use netpod::EventDataReadStats;
use serde::{Deserialize, Serialize};

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

pub trait Bins {
    fn bin_count(&self) -> u32;
}

// TODO this is meant for bins, count them, collect range-complete, and deliver these
// information also to the client.
pub trait Collected {
    fn new(bin_count_exp: u32) -> Self;
    fn timed_out(&mut self, k: bool);
}

pub trait Collectable {
    type Collected: Collected;
    fn append_to(&self, collected: &mut Self::Collected);
}

pub trait ToJsonResult {
    type Output;
    fn to_json_result(&self) -> Result<Self::Output, Error>;
}

pub trait Appendable: WithLen {
    fn empty() -> Self;
    fn append(&mut self, src: &Self);
}
