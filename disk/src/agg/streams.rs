use crate::streamlog::LogItem;
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

pub trait Batchable {
    fn append(&mut self, k: &mut Self);
}
