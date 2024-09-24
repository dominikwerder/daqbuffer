use super::aggregator::AggregatorNumeric;
use super::aggregator::AggregatorTimeWeight;
use super::container_events::EventValueType;
use super::___;
use crate::vecpreview::PreviewRange;
use crate::vecpreview::VecPreview;
use core::fmt;
use err::thiserror;
use err::ThisError;
use netpod::TsNano;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::collections::VecDeque;

#[allow(unused)]
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[derive(Debug, ThisError)]
#[cstm(name = "ContainerBins")]
pub enum ContainerBinsError {
    Unordered,
}

pub trait BinValueType: fmt::Debug + Clone + PartialOrd {
    // type Container: Container<Self>;
    // type AggregatorTimeWeight: AggregatorTimeWeight<Self>;
    // type AggTimeWeightOutputAvg;

    // fn identity_sum() -> Self;
    // fn add_weighted(&self, add: &Self, f: f32) -> Self;
}

#[derive(Debug, Clone)]
pub struct BinSingle<EVT> {
    pub ts1: TsNano,
    pub ts2: TsNano,
    pub cnt: u64,
    pub min: EVT,
    pub max: EVT,
    pub avg: f32,
    pub lst: EVT,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ContainerBins<EVT>
where
    EVT: EventValueType,
{
    ts1s: VecDeque<TsNano>,
    ts2s: VecDeque<TsNano>,
    cnts: VecDeque<u64>,
    mins: VecDeque<EVT>,
    maxs: VecDeque<EVT>,
    avgs: VecDeque<EVT::AggTimeWeightOutputAvg>,
    lsts: VecDeque<EVT>,
}

impl<EVT> ContainerBins<EVT>
where
    EVT: EventValueType,
{
    pub fn type_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            ts1s: VecDeque::new(),
            ts2s: VecDeque::new(),
            cnts: VecDeque::new(),
            mins: VecDeque::new(),
            maxs: VecDeque::new(),
            avgs: VecDeque::new(),
            lsts: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.ts1s.len()
    }

    pub fn verify(&self) -> Result<(), ContainerBinsError> {
        if self.ts1s.iter().zip(self.ts1s.iter().skip(1)).any(|(&a, &b)| a > b) {
            return Err(ContainerBinsError::Unordered);
        }
        if self.ts2s.iter().zip(self.ts2s.iter().skip(1)).any(|(&a, &b)| a > b) {
            return Err(ContainerBinsError::Unordered);
        }
        Ok(())
    }

    pub fn ts1_first(&self) -> Option<TsNano> {
        self.ts1s.front().map(|&x| x)
    }

    pub fn ts2_last(&self) -> Option<TsNano> {
        self.ts2s.back().map(|&x| x)
    }

    pub fn len_before(&self, end: TsNano) -> usize {
        let pp = self.ts2s.partition_point(|&x| x <= end);
        assert!(pp <= self.len(), "len_before  pp {}  len {}", pp, self.len());
        pp
    }

    pub fn pop_front(&mut self) -> Option<BinSingle<EVT>> {
        let ts1 = if let Some(x) = self.ts1s.pop_front() {
            x
        } else {
            return None;
        };
        let ts2 = if let Some(x) = self.ts2s.pop_front() {
            x
        } else {
            return None;
        };
        todo!()
    }

    pub fn push_back(
        &mut self,
        ts1: TsNano,
        ts2: TsNano,
        cnt: u64,
        min: EVT,
        max: EVT,
        avg: EVT::AggTimeWeightOutputAvg,
        lst: EVT,
    ) {
        self.ts1s.push_back(ts1);
        self.ts2s.push_back(ts2);
        self.cnts.push_back(cnt);
        self.mins.push_back(min);
        self.maxs.push_back(max);
        self.avgs.push_back(avg);
        self.lsts.push_back(lst);
    }
}

impl<EVT> fmt::Debug for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let self_name = any::type_name::<Self>();
        write!(
            fmt,
            "{self_name}  {{  len: {:?},  ts1s: {:?},  ts2s: {:?}, cnts: {:?},  avgs {:?}  }}",
            self.len(),
            VecPreview::new(&self.ts1s),
            VecPreview::new(&self.ts2s),
            VecPreview::new(&self.cnts),
            VecPreview::new(&self.avgs),
        )
    }
}

pub struct ContainerBinsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    evs: &'a mut ContainerBins<EVT>,
    len: usize,
}

impl<'a, EVT> ContainerBinsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    pub fn new(evs: &'a mut ContainerBins<EVT>, len: usize) -> Self {
        let len = len.min(evs.len());
        Self { evs, len }
    }
}

impl<'a, EVT> ContainerBinsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    pub fn ts1_first(&self) -> Option<TsNano> {
        self.evs.ts1_first()
    }

    pub fn ts2_last(&self) -> Option<TsNano> {
        self.evs.ts2_last()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn pop_front(&mut self) -> Option<BinSingle<EVT>> {
        if self.len != 0 {
            if let Some(ev) = self.evs.pop_front() {
                self.len -= 1;
                Some(ev)
            } else {
                None
            }
        } else {
            None
        }
    }
}
