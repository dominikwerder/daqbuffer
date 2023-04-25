pub mod collect_s;
pub mod container;
pub mod framable;
pub mod isodate;
pub mod overlap;
pub mod scalar_ops;
pub mod streamitem;
pub mod subfr;
pub mod timebin;
pub mod transform;

pub mod bincode {
    pub use bincode::*;
}

pub use futures_util;

use collect_s::Collectable;
use container::ByteEstimate;
use netpod::range::evrange::SeriesRange;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use timebin::TimeBinnable;

pub trait WithLen {
    fn len(&self) -> usize;
}

pub trait Empty {
    fn empty() -> Self;
}

pub trait Appendable<STY>: Empty + WithLen {
    fn push(&mut self, ts: u64, pulse: u64, value: STY);
}

pub trait TypeName {
    fn type_name(&self) -> String;
}

pub trait AppendEmptyBin {
    fn append_empty_bin(&mut self, ts1: u64, ts2: u64);
}

pub trait AsAnyRef {
    fn as_any_ref(&self) -> &dyn Any;
}

pub trait AsAnyMut {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T> AsAnyRef for Box<T>
where
    T: AsAnyRef + ?Sized,
{
    fn as_any_ref(&self) -> &dyn Any {
        self.as_ref().as_any_ref()
    }
}

impl<T> AsAnyMut for Box<T>
where
    T: AsAnyMut + ?Sized,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self.as_mut().as_any_mut()
    }
}

#[derive(Debug)]
pub enum MergeError {
    NotCompatible,
    Full,
}

impl From<MergeError> for err::Error {
    fn from(e: MergeError) -> Self {
        format!("{e:?}").into()
    }
}

// TODO can I remove the Any bound?

/// Container of some form of events, for use as trait object.
pub trait Events:
    fmt::Debug
    + TypeName
    + Any
    + Collectable
    + TimeBinnable
    + WithLen
    + ByteEstimate
    + Send
    + erased_serde::Serialize
    + EventsNonObj
{
    fn as_time_binnable_mut(&mut self) -> &mut dyn TimeBinnable;
    fn verify(&self) -> bool;
    fn output_info(&self);
    fn as_collectable_mut(&mut self) -> &mut dyn Collectable;
    fn as_collectable_with_default_ref(&self) -> &dyn Collectable;
    fn as_collectable_with_default_mut(&mut self) -> &mut dyn Collectable;
    fn ts_min(&self) -> Option<u64>;
    fn ts_max(&self) -> Option<u64>;
    // TODO is this used?
    fn take_new_events_until_ts(&mut self, ts_end: u64) -> Box<dyn Events>;
    fn new_empty_evs(&self) -> Box<dyn Events>;
    fn drain_into_evs(&mut self, dst: &mut Box<dyn Events>, range: (usize, usize)) -> Result<(), MergeError>;
    fn find_lowest_index_gt_evs(&self, ts: u64) -> Option<usize>;
    fn find_lowest_index_ge_evs(&self, ts: u64) -> Option<usize>;
    fn find_highest_index_lt_evs(&self, ts: u64) -> Option<usize>;
    fn clone_dyn(&self) -> Box<dyn Events>;
    fn partial_eq_dyn(&self, other: &dyn Events) -> bool;
    fn serde_id(&self) -> &'static str;
    fn nty_id(&self) -> u32;
    fn tss(&self) -> &VecDeque<u64>;
    fn pulses(&self) -> &VecDeque<u64>;
    fn frame_type_id(&self) -> u32;
    fn to_min_max_avg(&mut self) -> Box<dyn Events>;
}

impl WithLen for Box<dyn Events> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

pub trait EventsNonObj {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>);
}

erased_serde::serialize_trait_object!(Events);

impl PartialEq for Box<dyn Events> {
    fn eq(&self, other: &Self) -> bool {
        Events::partial_eq_dyn(self.as_ref(), other.as_ref())
    }
}

impl EventsNonObj for Box<dyn Events> {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>) {
        todo!()
    }
}

impl Events for Box<dyn Events> {
    fn as_time_binnable_mut(&mut self) -> &mut dyn TimeBinnable {
        Events::as_time_binnable_mut(self.as_mut())
    }

    fn verify(&self) -> bool {
        Events::verify(self.as_ref())
    }

    fn output_info(&self) {
        Events::output_info(self.as_ref())
    }

    fn as_collectable_mut(&mut self) -> &mut dyn Collectable {
        Events::as_collectable_mut(self.as_mut())
    }

    fn as_collectable_with_default_ref(&self) -> &dyn Collectable {
        Events::as_collectable_with_default_ref(self.as_ref())
    }

    fn as_collectable_with_default_mut(&mut self) -> &mut dyn Collectable {
        Events::as_collectable_with_default_mut(self.as_mut())
    }

    fn ts_min(&self) -> Option<u64> {
        Events::ts_min(self.as_ref())
    }

    fn ts_max(&self) -> Option<u64> {
        Events::ts_max(self.as_ref())
    }

    fn take_new_events_until_ts(&mut self, ts_end: u64) -> Box<dyn Events> {
        Events::take_new_events_until_ts(self.as_mut(), ts_end)
    }

    fn new_empty_evs(&self) -> Box<dyn Events> {
        Events::new_empty_evs(self.as_ref())
    }

    fn drain_into_evs(&mut self, dst: &mut Box<dyn Events>, range: (usize, usize)) -> Result<(), MergeError> {
        Events::drain_into_evs(self.as_mut(), dst, range)
    }

    fn find_lowest_index_gt_evs(&self, ts: u64) -> Option<usize> {
        Events::find_lowest_index_gt_evs(self.as_ref(), ts)
    }

    fn find_lowest_index_ge_evs(&self, ts: u64) -> Option<usize> {
        Events::find_lowest_index_ge_evs(self.as_ref(), ts)
    }

    fn find_highest_index_lt_evs(&self, ts: u64) -> Option<usize> {
        Events::find_highest_index_lt_evs(self.as_ref(), ts)
    }

    fn clone_dyn(&self) -> Box<dyn Events> {
        Events::clone_dyn(self.as_ref())
    }

    fn partial_eq_dyn(&self, other: &dyn Events) -> bool {
        Events::partial_eq_dyn(self.as_ref(), other)
    }

    fn serde_id(&self) -> &'static str {
        Events::serde_id(self.as_ref())
    }

    fn nty_id(&self) -> u32 {
        Events::nty_id(self.as_ref())
    }

    fn tss(&self) -> &VecDeque<u64> {
        Events::tss(self.as_ref())
    }

    fn pulses(&self) -> &VecDeque<u64> {
        Events::pulses(self.as_ref())
    }

    fn frame_type_id(&self) -> u32 {
        Events::frame_type_id(self.as_ref())
    }

    fn to_min_max_avg(&mut self) -> Box<dyn Events> {
        Events::to_min_max_avg(self.as_mut())
    }
}
