use super::aggregator::AggregatorNumeric;
use super::aggregator::AggregatorTimeWeight;
use super::___;
use crate::vecpreview::PreviewRange;
use crate::vecpreview::VecPreview;
use core::fmt;
use netpod::TsNano;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::collections::VecDeque;

#[allow(unused)]
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

pub trait Container: fmt::Debug + Clone + PreviewRange + Serialize + for<'a> Deserialize<'a> {
    fn new() -> Self;
}

pub trait EventValueType: fmt::Debug + Clone {
    type Container: Container;
    type AggregatorTimeWeight: AggregatorTimeWeight;
}

impl<T> Container for VecDeque<T>
where
    T: EventValueType + Serialize + for<'a> Deserialize<'a>,
{
    fn new() -> Self {
        VecDeque::new()
    }
}

impl EventValueType for f32 {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric<Self>;
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
