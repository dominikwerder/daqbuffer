use crate::{RangeCompletableItem, Sitemty, StreamItem, WithLen};
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

impl ToJsonBytes for serde_json::Value {
    fn to_json_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(self)?)
    }
}

impl ToJsonResult for Sitemty<serde_json::Value> {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        match self {
            Ok(item) => match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::Data(item) => Ok(Box::new(item.clone())),
                    RangeCompletableItem::RangeComplete => Err(Error::with_msg("RangeComplete")),
                },
                StreamItem::Log(item) => Err(Error::with_msg(format!("Log {:?}", item))),
                StreamItem::Stats(item) => Err(Error::with_msg(format!("Stats {:?}", item))),
            },
            Err(e) => Err(Error::with_msg(format!("Error {:?}", e))),
        }
    }
}
