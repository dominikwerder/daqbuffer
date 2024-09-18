use super::___;
use netpod::TsNano;
use serde::Deserialize;
use serde::Serialize;
use std::collections::VecDeque;

#[allow(unused)]
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

pub trait Container: Clone {}

impl<T> Container for VecDeque<T> where T: EventValueType {}

pub trait EventValueType: Clone {
    type Container: Container;
}

impl EventValueType for f32 {
    type Container = VecDeque<Self>;
}

#[derive(Clone)]
pub struct ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    tss: VecDeque<TsNano>,
    // vals: VecDeque<EVT>,
    vals: VecDeque<<EVT as EventValueType>::Container>,
}

// TODO why does this already impl Serialize even though there is no bound for EVT?
// TODO try to actually instantiate and serialize in a test.

#[derive(Clone, Serialize, Deserialize)]
pub struct ContainerEvents2<EVT>
where
    EVT: EventValueType,
{
    tss: VecDeque<TsNano>,
    vals: VecDeque<EVT>,
}
