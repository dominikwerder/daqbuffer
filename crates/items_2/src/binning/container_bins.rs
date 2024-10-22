use super::aggregator::AggregatorNumeric;
use super::aggregator::AggregatorTimeWeight;
use super::container_events::EventValueType;
use super::___;
use core::fmt;
use err::thiserror;
use err::ThisError;
use items_0::vecpreview::VecPreview;
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
    pub fnl: bool,
}

#[derive(Debug, Clone)]
pub struct BinRef<'a, EVT>
where
    EVT: EventValueType,
{
    pub ts1: TsNano,
    pub ts2: TsNano,
    pub cnt: u64,
    pub min: &'a EVT,
    pub max: &'a EVT,
    pub avg: &'a EVT::AggTimeWeightOutputAvg,
    pub lst: &'a EVT,
    pub fnl: bool,
}

pub struct IterDebug<'a, EVT>
where
    EVT: EventValueType,
{
    bins: &'a ContainerBins<EVT>,
    ix: usize,
    len: usize,
}

impl<'a, EVT> Iterator for IterDebug<'a, EVT>
where
    EVT: EventValueType,
{
    type Item = BinRef<'a, EVT>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ix < self.bins.len() && self.ix < self.len {
            let b = &self.bins;
            let i = self.ix;
            self.ix += 1;
            let ret = BinRef {
                ts1: b.ts1s[i],
                ts2: b.ts2s[i],
                cnt: b.cnts[i],
                min: &b.mins[i],
                max: &b.maxs[i],
                avg: &b.avgs[i],
                lst: &b.lsts[i],
                fnl: b.fnls[i],
            };
            Some(ret)
        } else {
            None
        }
    }
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
    fnls: VecDeque<bool>,
}

impl<EVT> ContainerBins<EVT>
where
    EVT: EventValueType,
{
    pub fn from_constituents(
        ts1s: VecDeque<TsNano>,
        ts2s: VecDeque<TsNano>,
        cnts: VecDeque<u64>,
        mins: VecDeque<EVT>,
        maxs: VecDeque<EVT>,
        avgs: VecDeque<EVT::AggTimeWeightOutputAvg>,
        lsts: VecDeque<EVT>,
        fnls: VecDeque<bool>,
    ) -> Self {
        Self {
            ts1s,
            ts2s,
            cnts,
            mins,
            maxs,
            avgs,
            lsts,
            fnls,
        }
    }

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
            fnls: VecDeque::new(),
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

    pub fn ts1s_iter(&self) -> std::collections::vec_deque::Iter<TsNano> {
        self.ts1s.iter()
    }

    pub fn ts2s_iter(&self) -> std::collections::vec_deque::Iter<TsNano> {
        self.ts2s.iter()
    }

    pub fn cnts_iter(&self) -> std::collections::vec_deque::Iter<u64> {
        self.cnts.iter()
    }

    pub fn mins_iter(&self) -> std::collections::vec_deque::Iter<EVT> {
        self.mins.iter()
    }

    pub fn maxs_iter(&self) -> std::collections::vec_deque::Iter<EVT> {
        self.maxs.iter()
    }

    pub fn avgs_iter(&self) -> std::collections::vec_deque::Iter<EVT::AggTimeWeightOutputAvg> {
        self.avgs.iter()
    }

    pub fn fnls_iter(&self) -> std::collections::vec_deque::Iter<bool> {
        self.fnls.iter()
    }

    pub fn zip_iter(
        &self,
    ) -> std::iter::Zip<
        std::iter::Zip<
            std::iter::Zip<
                std::iter::Zip<
                    std::iter::Zip<
                        std::iter::Zip<
                            std::collections::vec_deque::Iter<TsNano>,
                            std::collections::vec_deque::Iter<TsNano>,
                        >,
                        std::collections::vec_deque::Iter<u64>,
                    >,
                    std::collections::vec_deque::Iter<EVT>,
                >,
                std::collections::vec_deque::Iter<EVT>,
            >,
            std::collections::vec_deque::Iter<EVT::AggTimeWeightOutputAvg>,
        >,
        std::collections::vec_deque::Iter<bool>,
    > {
        self.ts1s_iter()
            .zip(self.ts2s_iter())
            .zip(self.cnts_iter())
            .zip(self.mins_iter())
            .zip(self.maxs_iter())
            .zip(self.avgs_iter())
            .zip(self.fnls_iter())
    }

    pub fn edges_iter(
        &self,
    ) -> std::iter::Zip<std::collections::vec_deque::Iter<TsNano>, std::collections::vec_deque::Iter<TsNano>> {
        self.ts1s.iter().zip(self.ts2s.iter())
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
        fnl: bool,
    ) {
        self.ts1s.push_back(ts1);
        self.ts2s.push_back(ts2);
        self.cnts.push_back(cnt);
        self.mins.push_back(min);
        self.maxs.push_back(max);
        self.avgs.push_back(avg);
        self.lsts.push_back(lst);
        self.fnls.push_back(fnl);
    }

    pub fn iter_debug(&self) -> IterDebug<EVT> {
        IterDebug {
            bins: self,
            ix: 0,
            len: self.len(),
        }
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
            "{self_name}  {{  len: {:?},  ts1s: {:?},  ts2s: {:?}, cnts: {:?},  avgs {:?},  fnls {:?}  }}",
            self.len(),
            VecPreview::new(&self.ts1s),
            VecPreview::new(&self.ts2s),
            VecPreview::new(&self.cnts),
            VecPreview::new(&self.avgs),
            VecPreview::new(&self.fnls),
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
