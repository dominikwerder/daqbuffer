use super::aggregator::AggregatorNumeric;
use super::aggregator::AggregatorTimeWeight;
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
#[cstm(name = "ValueContainerError")]
pub enum ValueContainerError {}

pub trait Container<EVT>: fmt::Debug + Clone + PreviewRange + Serialize + for<'a> Deserialize<'a> {
    fn new() -> Self;
    // fn verify(&self) -> Result<(), ValueContainerError>;
    fn push_back(&mut self, val: EVT);
    fn pop_front(&mut self) -> Option<EVT>;
}

pub trait EventValueType: fmt::Debug + Clone + PartialOrd {
    type Container: Container<Self>;
    type AggregatorTimeWeight: AggregatorTimeWeight<Self>;
    type AggTimeWeightOutputAvg;

    fn identity_sum() -> Self;
    fn add_weighted(&self, add: &Self, f: f32) -> Self;
}

impl<EVT> Container<EVT> for VecDeque<EVT>
where
    EVT: EventValueType + Serialize + for<'a> Deserialize<'a>,
{
    fn new() -> Self {
        VecDeque::new()
    }

    fn push_back(&mut self, val: EVT) {
        self.push_back(val);
    }

    fn pop_front(&mut self) -> Option<EVT> {
        self.pop_front()
    }
}

impl EventValueType for f32 {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric<Self>;
    type AggTimeWeightOutputAvg = <Self::AggregatorTimeWeight as AggregatorTimeWeight<Self>>::OutputAvg;

    fn identity_sum() -> Self {
        0.
    }

    fn add_weighted(&self, add: &Self, f: f32) -> Self {
        todo!()
    }
}

impl EventValueType for f64 {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric<Self>;
    type AggTimeWeightOutputAvg = <Self::AggregatorTimeWeight as AggregatorTimeWeight<Self>>::OutputAvg;

    fn identity_sum() -> Self {
        0.
    }

    fn add_weighted(&self, add: &Self, f: f32) -> Self {
        todo!()
    }
}

impl EventValueType for u64 {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric<Self>;
    type AggTimeWeightOutputAvg = <Self::AggregatorTimeWeight as AggregatorTimeWeight<Self>>::OutputAvg;

    fn identity_sum() -> Self {
        0
    }

    fn add_weighted(&self, add: &Self, f: f32) -> Self {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct EventSingle<EVT> {
    pub ts: TsNano,
    pub val: EVT,
}

#[derive(Debug, ThisError)]
#[cstm(name = "EventsContainerError")]
pub enum EventsContainerError {
    Unordered,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    tss: VecDeque<TsNano>,
    vals: <EVT as EventValueType>::Container,
}

impl<EVT> ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    pub fn type_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            tss: VecDeque::new(),
            vals: Container::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.tss.len()
    }

    pub fn verify(&self) -> Result<(), EventsContainerError> {
        if self.tss.iter().zip(self.tss.iter().skip(1)).any(|(&a, &b)| a > b) {
            return Err(EventsContainerError::Unordered);
        }
        Ok(())
    }

    pub fn ts_first(&self) -> Option<TsNano> {
        self.tss.front().map(|&x| x)
    }

    pub fn ts_last(&self) -> Option<TsNano> {
        self.tss.back().map(|&x| x)
    }

    pub fn len_before(&self, end: TsNano) -> usize {
        let pp = self.tss.partition_point(|&x| x < end);
        assert!(pp <= self.len(), "len_before  pp {}  len {}", pp, self.len());
        pp
    }

    pub fn pop_front(&mut self) -> Option<EventSingle<EVT>> {
        if let (Some(ts), Some(val)) = (self.tss.pop_front(), self.vals.pop_front()) {
            Some(EventSingle { ts, val })
        } else {
            None
        }
    }

    pub fn push_back(&mut self, ts: TsNano, val: EVT) {
        self.tss.push_back(ts);
        self.vals.push_back(val);
    }
}

impl<EVT> fmt::Debug for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let self_name = any::type_name::<Self>();
        write!(
            fmt,
            "{self_name}  {{  len: {:?},  tss: {:?},  vals {:?}  }}",
            self.len(),
            VecPreview::new(&self.tss),
            VecPreview::new(&self.vals),
        )
    }
}

pub struct ContainerEventsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    evs: &'a mut ContainerEvents<EVT>,
    len: usize,
}

impl<'a, EVT> ContainerEventsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    pub fn new(evs: &'a mut ContainerEvents<EVT>, len: usize) -> Self {
        let len = len.min(evs.len());
        Self { evs, len }
    }
}

impl<'a, EVT> ContainerEventsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    pub fn ts_first(&self) -> Option<TsNano> {
        self.evs.ts_first()
    }

    pub fn ts_last(&self) -> Option<TsNano> {
        self.evs.ts_last()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn pop_front(&mut self) -> Option<EventSingle<EVT>> {
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
