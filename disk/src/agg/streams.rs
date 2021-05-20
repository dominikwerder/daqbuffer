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
    fn append_to(&mut self, collected: &mut Self::Collected);
}

pub trait ToJsonResult {
    type Output;
    fn to_json_result(&self) -> Result<Self::Output, Error>;
}

impl<T> AggregatableXdim1Bin for StreamItem<T>
where
    // TODO bound on the Output ???
    //T: AggregatableTdim + AggregatableXdim1Bin<Output = T>,
    T: AggregatableTdim + AggregatableXdim1Bin,
{
    type Output = StreamItem<<T as AggregatableXdim1Bin>::Output>;

    fn into_agg(self) -> Self::Output {
        // TODO how to handle the type mismatch?
        /*match self {
            Self::Log(item) => Self::Log(item),
            Self::Stats(item) => Self::Stats(item),
            Self::DataItem(item) => Self::DataItem(item.into_agg()),
        }*/
        err::todoval()
    }
}

pub struct StreamItemAggregator<T>
where
    T: AggregatableTdim,
{
    inner_agg: <T as AggregatableTdim>::Aggregator,
    _mark: std::marker::PhantomData<T>,
}

impl<T> StreamItemAggregator<T>
where
    T: AggregatableTdim,
{
    pub fn new(ts1: u64, ts2: u64) -> Self {
        Self {
            inner_agg: <T as AggregatableTdim>::aggregator_new_static(ts1, ts2),
            _mark: std::marker::PhantomData::default(),
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
        todo!()
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn ingest(&mut self, inp: &mut Self::InputValue) {
        todo!()
    }

    fn result(self) -> Vec<Self::OutputValue> {
        todo!()
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

    fn is_range_complete(&self) -> bool {
        match self {
            Self::DataItem(item) => item.is_range_complete(),
            Self::Log(_) => false,
            Self::Stats(_) => false,
        }
    }

    // TODO refactor: is this necessary to have on the trait?
    fn make_range_complete_item() -> Option<Self> {
        match <T as AggregatableTdim>::make_range_complete_item() {
            Some(k) => Some(Self::DataItem(k)),
            None => None,
        }
    }

    // TODO refactor: the point of having the StreamItem is that this function is no longer necessary:
    fn is_log_item(&self) -> bool {
        if let Self::Log(_) = self {
            true
        } else {
            false
        }
    }

    // TODO should be able to remove this from trait:
    fn log_item(self) -> Option<LogItem> {
        if let Self::Log(item) = self {
            Some(item)
        } else {
            None
        }
    }

    // TODO should be able to remove this from trait:
    fn make_log_item(item: LogItem) -> Option<Self> {
        Some(Self::Log(item))
    }

    // TODO should be able to remove this from trait:
    fn is_stats_item(&self) -> bool {
        if let Self::Stats(_) = self {
            true
        } else {
            false
        }
    }

    // TODO should be able to remove this from trait:
    fn stats_item(self) -> Option<EventDataReadStats> {
        if let Self::Stats(_item) = self {
            // TODO this whole function should no longer be needed.
            Some(err::todoval())
        } else {
            None
        }
    }

    // TODO should be able to remove this from trait:
    fn make_stats_item(item: EventDataReadStats) -> Option<Self> {
        Some(Self::Stats(StatsItem::EventDataReadStats(item)))
    }
}
