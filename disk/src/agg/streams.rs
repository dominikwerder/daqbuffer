use crate::agg::binnedt::{AggregatableTdim, AggregatorTdim};
use crate::agg::AggregatableXdim1Bin;
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

impl<T> AggregatableXdim1Bin for StreamItem<T>
where
    T: AggregatableTdim + AggregatableXdim1Bin,
{
    type Output = StreamItem<<T as AggregatableXdim1Bin>::Output>;

    fn into_agg(self) -> Self::Output {
        match self {
            Self::Log(item) => Self::Output::Log(item),
            Self::Stats(item) => Self::Output::Stats(item),
            Self::DataItem(item) => Self::Output::DataItem(item.into_agg()),
        }
    }
}

pub struct StreamItemAggregator<T>
where
    T: AggregatableTdim,
{
    inner_agg: <T as AggregatableTdim>::Aggregator,
}

impl<T> StreamItemAggregator<T>
where
    T: AggregatableTdim,
{
    pub fn new(ts1: u64, ts2: u64) -> Self {
        Self {
            inner_agg: <T as AggregatableTdim>::aggregator_new_static(ts1, ts2),
        }
    }
}

impl<T> AggregatorTdim for StreamItemAggregator<T>
where
    T: AggregatableTdim,
{
    type InputValue = StreamItem<T>;
    type OutputValue = StreamItem<<<T as AggregatableTdim>::Aggregator as AggregatorTdim>::OutputValue>;

    fn ends_before(&self, inp: &Self::InputValue) -> bool {
        match inp {
            StreamItem::Log(_) => false,
            StreamItem::Stats(_) => false,
            StreamItem::DataItem(item) => self.inner_agg.ends_before(item),
        }
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        match inp {
            StreamItem::Log(_) => false,
            StreamItem::Stats(_) => false,
            StreamItem::DataItem(item) => self.inner_agg.ends_after(item),
        }
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        match inp {
            StreamItem::Log(_) => false,
            StreamItem::Stats(_) => false,
            StreamItem::DataItem(item) => self.inner_agg.starts_after(item),
        }
    }

    fn ingest(&mut self, inp: &mut Self::InputValue) {
        match inp {
            StreamItem::Log(_) => {}
            StreamItem::Stats(_) => {}
            StreamItem::DataItem(item) => {
                self.inner_agg.ingest(item);
            }
        }
    }

    fn result(self) -> Vec<Self::OutputValue> {
        self.inner_agg
            .result()
            .into_iter()
            .map(|k| StreamItem::DataItem(k))
            .collect()
    }
}

impl<T> AggregatableTdim for StreamItem<T>
where
    T: AggregatableTdim,
{
    type Output = StreamItem<<StreamItemAggregator<T> as AggregatorTdim>::OutputValue>;
    type Aggregator = StreamItemAggregator<T>;

    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator {
        Self::Aggregator::new(ts1, ts2)
    }
}
