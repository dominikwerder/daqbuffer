use crate::agg::binnedt::{AggregatableTdim, AggregatorTdim};
use crate::agg::AggregatableXdim1Bin;
use crate::binned::BinnedStreamKind;
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

impl<T, SK> AggregatableXdim1Bin<SK> for StreamItem<T>
where
    SK: BinnedStreamKind,
    T: AggregatableTdim<SK> + AggregatableXdim1Bin<SK>,
{
    type Output = StreamItem<<T as AggregatableXdim1Bin<SK>>::Output>;

    fn into_agg(self) -> Self::Output {
        match self {
            Self::Log(item) => Self::Output::Log(item),
            Self::Stats(item) => Self::Output::Stats(item),
            Self::DataItem(item) => Self::Output::DataItem(item.into_agg()),
        }
    }
}

pub struct StreamItemAggregator<T, SK>
where
    T: AggregatableTdim<SK>,
    SK: BinnedStreamKind,
{
    inner_agg: <T as AggregatableTdim<SK>>::Aggregator,
}

impl<T, SK> StreamItemAggregator<T, SK>
where
    T: AggregatableTdim<SK>,
    SK: BinnedStreamKind,
{
    pub fn new(ts1: u64, ts2: u64) -> Self {
        Self {
            inner_agg: <T as AggregatableTdim<SK>>::aggregator_new_static(ts1, ts2),
        }
    }
}

impl<T, SK> AggregatorTdim<SK> for StreamItemAggregator<T, SK>
where
    T: AggregatableTdim<SK>,
    SK: BinnedStreamKind,
{
    type InputValue = StreamItem<T>;
    type OutputValue = StreamItem<<<T as AggregatableTdim<SK>>::Aggregator as AggregatorTdim<SK>>::OutputValue>;

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

impl<T, SK> AggregatableTdim<SK> for StreamItem<T>
where
    T: AggregatableTdim<SK>,
    SK: BinnedStreamKind,
{
    //type Output = StreamItem<<StreamItemAggregator<T> as AggregatorTdim>::OutputValue>;
    type Aggregator = StreamItemAggregator<T, SK>;

    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator {
        Self::Aggregator::new(ts1, ts2)
    }
}
