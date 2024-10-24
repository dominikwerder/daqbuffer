use super::aggregator::AggregatorNumeric;
use super::aggregator::AggregatorTimeWeight;
use super::container_events::EventValueType;
use super::___;
use core::fmt;
use err::thiserror;
use err::ThisError;
use items_0::collect_s::Collectable;
use items_0::timebin::BinningggContainerBinsDyn;
use items_0::timebin::BinsBoxed;
use items_0::vecpreview::VecPreview;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::TypeName;
use items_0::WithLen;
use netpod::EnumVariant;
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
        todo!("pop_front");
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

impl<EVT> fmt::Display for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

impl<EVT> AsAnyMut for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn as_any_mut(&mut self) -> &mut dyn any::Any {
        self
    }
}

impl<EVT> WithLen for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn len(&self) -> usize {
        Self::len(self)
    }
}

impl<EVT> TypeName for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn type_name(&self) -> String {
        BinningggContainerBinsDyn::type_name(self).into()
    }
}

impl<EVT> AsAnyRef for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn as_any_ref(&self) -> &dyn any::Any {
        self
    }
}

#[derive(Debug)]
pub struct ContainerBinsCollector<EVT>
where
    EVT: EventValueType,
{
    bins: ContainerBins<EVT>,
}

impl<EVT> ContainerBinsCollector<EVT> where EVT: EventValueType {}

impl<EVT> WithLen for ContainerBinsCollector<EVT>
where
    EVT: EventValueType,
{
    fn len(&self) -> usize {
        self.bins.len()
    }
}

impl<EVT> items_0::container::ByteEstimate for ContainerBinsCollector<EVT>
where
    EVT: EventValueType,
{
    fn byte_estimate(&self) -> u64 {
        // TODO need better estimate
        self.bins.len() as u64 * 200
    }
}

impl<EVT> items_0::collect_s::Collector for ContainerBinsCollector<EVT>
where
    EVT: EventValueType,
{
    fn ingest(&mut self, src: &mut dyn Collectable) {
        todo!()
    }

    fn set_range_complete(&mut self) {
        todo!()
    }

    fn set_timed_out(&mut self) {
        todo!()
    }

    fn set_continue_at_here(&mut self) {
        todo!()
    }

    fn result(
        &mut self,
        range: Option<netpod::range::evrange::SeriesRange>,
        binrange: Option<netpod::BinnedRangeEnum>,
    ) -> Result<Box<dyn items_0::collect_s::Collected>, err::Error> {
        todo!()
    }
}

impl<EVT> Collectable for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn new_collector(&self) -> Box<dyn items_0::collect_s::Collector> {
        todo!()
    }
}

macro_rules! try_to_old_time_binned {
    ($sty:ty, $this:expr, $lst:expr) => {
        let this = $this;
        let a = this as &dyn any::Any;
        if let Some(src) = a.downcast_ref::<ContainerBins<$sty>>() {
            use items_0::Empty;
            let mut ret = crate::binsdim0::BinsDim0::<$sty>::empty();
            for ((((((&ts1, &ts2), &cnt), min), max), avg), fnl) in src.zip_iter() {
                ret.push(ts1.ns(), ts2.ns(), cnt, *min, *max, *avg as f32, $lst);
            }
            return Box::new(ret);
        }
    };
}

impl<EVT> BinningggContainerBinsDyn for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn type_name(&self) -> &'static str {
        any::type_name::<Self>()
    }

    fn empty(&self) -> BinsBoxed {
        Box::new(Self::new())
    }

    fn clone(&self) -> BinsBoxed {
        Box::new(<Self as Clone>::clone(self))
    }

    fn edges_iter(
        &self,
    ) -> std::iter::Zip<std::collections::vec_deque::Iter<TsNano>, std::collections::vec_deque::Iter<TsNano>> {
        self.ts1s.iter().zip(self.ts2s.iter())
    }

    fn drain_into(&mut self, dst: &mut dyn BinningggContainerBinsDyn, range: std::ops::Range<usize>) {
        let obj = dst.as_any_mut();
        if let Some(dst) = obj.downcast_mut::<Self>() {
            dst.ts1s.extend(self.ts1s.drain(range.clone()));
        } else {
            let styn = any::type_name::<EVT>();
            panic!("unexpected drain  EVT {}  dst {}", styn, Self::type_name());
        }
    }

    fn to_old_time_binned(&self) -> Box<dyn items_0::timebin::TimeBinned> {
        try_to_old_time_binned!(u8, self, 0);
        try_to_old_time_binned!(u16, self, 0);
        try_to_old_time_binned!(u32, self, 0);
        try_to_old_time_binned!(u64, self, 0);
        try_to_old_time_binned!(i8, self, 0);
        try_to_old_time_binned!(i16, self, 0);
        try_to_old_time_binned!(i32, self, 0);
        try_to_old_time_binned!(i64, self, 0);
        try_to_old_time_binned!(f32, self, 0.);
        try_to_old_time_binned!(f64, self, 0.);
        try_to_old_time_binned!(bool, self, false);
        // try_to_old_time_binned!(String, self, String::new());
        let a = self as &dyn any::Any;
        if let Some(src) = a.downcast_ref::<ContainerBins<EnumVariant>>() {
            use items_0::Empty;
            let mut ret = crate::binsdim0::BinsDim0::<EnumVariant>::empty();
            for ((((((&ts1, &ts2), &cnt), min), max), avg), _fnl) in src.zip_iter() {
                ret.push(
                    ts1.ns(),
                    ts2.ns(),
                    cnt,
                    min.clone(),
                    max.clone(),
                    *avg as f32,
                    EnumVariant::new(0, ""),
                );
            }
            return Box::new(ret);
        }
        let styn = any::type_name::<EVT>();
        todo!("TODO impl for {styn}");
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
