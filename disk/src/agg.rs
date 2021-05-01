/*!
Aggregation and binning support.
*/

use super::eventchunker::EventFull;
use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::{EventDataReadStats, NanoRange};
use netpod::{Node, ScalarType};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{debug, error, info, span, trace, warn, Level};

pub mod binnedt;
pub mod binnedx;
pub mod eventbatch;
pub mod scalarbinbatch;

pub trait AggregatorTdim {
    type InputValue;
    type OutputValue: AggregatableXdim1Bin + AggregatableTdim;
    fn ends_before(&self, inp: &Self::InputValue) -> bool;
    fn ends_after(&self, inp: &Self::InputValue) -> bool;
    fn starts_after(&self, inp: &Self::InputValue) -> bool;
    fn ingest(&mut self, inp: &Self::InputValue);
    fn result(self) -> Self::OutputValue;
}

pub trait AggregatableXdim1Bin {
    type Output: AggregatableXdim1Bin + AggregatableTdim;
    fn into_agg(self) -> Self::Output;
}

pub trait AggregatableTdim {
    type Output: AggregatableXdim1Bin + AggregatableTdim;
    type Aggregator: AggregatorTdim<InputValue = Self>;
    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator;
}

/// Batch of events with a scalar (zero dimensions) numeric value.
pub struct ValuesDim0 {
    tss: Vec<u64>,
    values: Vec<Vec<f32>>,
}

impl std::fmt::Debug for ValuesDim0 {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "count {}  tsA {:?}  tsB {:?}",
            self.tss.len(),
            self.tss.first(),
            self.tss.last()
        )
    }
}

impl AggregatableXdim1Bin for ValuesDim1 {
    type Output = MinMaxAvgScalarEventBatch;

    fn into_agg(self) -> Self::Output {
        let mut ret = MinMaxAvgScalarEventBatch {
            tss: Vec::with_capacity(self.tss.len()),
            mins: Vec::with_capacity(self.tss.len()),
            maxs: Vec::with_capacity(self.tss.len()),
            avgs: Vec::with_capacity(self.tss.len()),
        };
        for i1 in 0..self.tss.len() {
            let ts = self.tss[i1];
            let mut min = f32::MAX;
            let mut max = f32::MIN;
            let mut sum = 0f32;
            let vals = &self.values[i1];
            assert!(vals.len() > 0);
            for i2 in 0..vals.len() {
                let v = vals[i2];
                //info!("value  {}  {}  {}", i1, i2, v);
                min = min.min(v);
                max = max.max(v);
                sum += v;
            }
            if min == f32::MAX {
                min = f32::NAN;
            }
            if max == f32::MIN {
                max = f32::NAN;
            }
            ret.tss.push(ts);
            ret.mins.push(min);
            ret.maxs.push(max);
            ret.avgs.push(sum / vals.len() as f32);
        }
        ret
    }
}

pub struct ValuesDim1ExtractStats {
    pub dur: Duration,
}

impl ValuesDim1ExtractStats {
    pub fn new() -> Self {
        Self {
            dur: Duration::default(),
        }
    }
    pub fn trans(self: &mut Self, k: &mut Self) {
        self.dur += k.dur;
        k.dur = Duration::default();
    }
}

/// Batch of events with a numeric one-dimensional (i.e. array) value.
pub struct ValuesDim1 {
    pub tss: Vec<u64>,
    pub values: Vec<Vec<f32>>,
    pub event_data_read_stats: EventDataReadStats,
    pub values_dim_1_extract_stats: ValuesDim1ExtractStats,
}

impl ValuesDim1 {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            values: vec![],
            event_data_read_stats: EventDataReadStats::new(),
            values_dim_1_extract_stats: ValuesDim1ExtractStats::new(),
        }
    }
}

impl std::fmt::Debug for ValuesDim1 {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "count {}  tsA {:?}  tsB {:?}",
            self.tss.len(),
            self.tss.first(),
            self.tss.last()
        )
    }
}

impl AggregatableXdim1Bin for ValuesDim0 {
    type Output = MinMaxAvgScalarEventBatch;

    fn into_agg(self) -> Self::Output {
        let mut ret = MinMaxAvgScalarEventBatch {
            tss: Vec::with_capacity(self.tss.len()),
            mins: Vec::with_capacity(self.tss.len()),
            maxs: Vec::with_capacity(self.tss.len()),
            avgs: Vec::with_capacity(self.tss.len()),
        };
        for i1 in 0..self.tss.len() {
            let ts = self.tss[i1];
            let mut min = f32::MAX;
            let mut max = f32::MIN;
            let mut sum = 0f32;
            let vals = &self.values[i1];
            assert!(vals.len() > 0);
            for i2 in 0..vals.len() {
                let v = vals[i2];
                //info!("value  {}  {}  {}", i1, i2, v);
                min = min.min(v);
                max = max.max(v);
                sum += v;
            }
            if min == f32::MAX {
                min = f32::NAN;
            }
            if max == f32::MIN {
                max = f32::NAN;
            }
            ret.tss.push(ts);
            ret.mins.push(min);
            ret.maxs.push(max);
            ret.avgs.push(sum / vals.len() as f32);
        }
        ret
    }
}

pub enum Fits {
    Empty,
    Lower,
    Greater,
    Inside,
    PartlyLower,
    PartlyGreater,
    PartlyLowerAndGreater,
}

pub trait FitsInside {
    fn fits_inside(&self, range: NanoRange) -> Fits;
}

pub struct Dim0F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>>,
{
    inp: S,
    errored: bool,
    completed: bool,
}

impl<S> Dim0F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>>,
{
    pub fn new(inp: S) -> Self {
        Self {
            inp,
            errored: false,
            completed: false,
        }
    }
}

impl<S> Stream for Dim0F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>> + Unpin,
{
    type Item = Result<ValuesDim0, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("Dim0F32Stream  poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => {
                // TODO implement here for dim-0
                let mut ret = ValuesDim0 {
                    tss: vec![],
                    values: vec![],
                };
                use ScalarType::*;
                for i1 in 0..k.tss.len() {
                    // TODO iterate sibling arrays after single bounds check
                    let ty = &k.scalar_types[i1];
                    let decomp = k.decomps[i1].as_ref().unwrap();
                    match ty {
                        F64 => {
                            const BY: usize = 8;
                            // do the conversion

                            // TODO  only a scalar!
                            err::todoval::<u32>();

                            let n1 = decomp.len();
                            assert!(n1 % ty.bytes() as usize == 0);
                            let ele_count = n1 / ty.bytes() as usize;
                            let mut j = Vec::with_capacity(ele_count);
                            // this is safe for ints and floats
                            unsafe {
                                j.set_len(ele_count);
                            }
                            let mut p1 = 0;
                            for i1 in 0..ele_count {
                                let u = unsafe {
                                    let mut r = [0u8; BY];
                                    std::ptr::copy_nonoverlapping(&decomp[p1], r.as_mut_ptr(), BY);
                                    f64::from_be_bytes(r)
                                    //f64::from_be_bytes(std::mem::transmute::<_, [u8; 8]>(&decomp[p1]))
                                };
                                j[i1] = u as f32;
                                p1 += BY;
                            }
                            ret.tss.push(k.tss[i1]);
                            ret.values.push(j);
                        }
                        _ => err::todoval(),
                    }
                }
                self.errored = true;
                Ready(Some(Err(Error::with_msg(format!("TODO not yet implemented")))))
            }
            Ready(Some(Err(e))) => {
                self.errored = true;
                Ready(Some(Err(e)))
            }
            Ready(None) => {
                self.completed = true;
                Ready(None)
            }
            Pending => Pending,
        }
    }
}

pub trait IntoDim0F32Stream {
    fn into_dim_0_f32_stream(self) -> Dim0F32Stream<Self>
    where
        Self: Stream<Item = Result<EventFull, Error>> + Sized;
}

impl<T> IntoDim0F32Stream for T
where
    T: Stream<Item = Result<EventFull, Error>>,
{
    fn into_dim_0_f32_stream(self) -> Dim0F32Stream<T> {
        Dim0F32Stream::new(self)
    }
}

pub struct Dim1F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>>,
{
    inp: S,
    errored: bool,
    completed: bool,
}

impl<S> Dim1F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>>,
{
    pub fn new(inp: S) -> Self {
        Self {
            inp,
            errored: false,
            completed: false,
        }
    }
}

impl<S> Stream for Dim1F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>> + Unpin,
{
    type Item = Result<ValuesDim1, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("Dim1F32Stream  poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => {
                let inst1 = Instant::now();
                let mut ret = ValuesDim1::empty();
                use ScalarType::*;
                for i1 in 0..k.tss.len() {
                    // TODO iterate sibling arrays after single bounds check
                    let ty = &k.scalar_types[i1];
                    let decomp = k.decomps[i1].as_ref().unwrap();
                    match ty {
                        U16 => {
                            const BY: usize = 2;
                            // do the conversion
                            let n1 = decomp.len();
                            assert!(n1 % ty.bytes() as usize == 0);
                            let ele_count = n1 / ty.bytes() as usize;
                            let mut j = Vec::with_capacity(ele_count);
                            let mut p1 = 0;
                            for _ in 0..ele_count {
                                let u = unsafe {
                                    let mut r = [0u8; BY];
                                    std::ptr::copy_nonoverlapping(&decomp[p1], r.as_mut_ptr(), BY);
                                    u16::from_be_bytes(r)
                                };
                                j.push(u as f32);
                                p1 += BY;
                            }
                            ret.tss.push(k.tss[i1]);
                            ret.values.push(j);
                        }
                        F64 => {
                            const BY: usize = 8;
                            // do the conversion
                            let n1 = decomp.len();
                            assert!(n1 % ty.bytes() as usize == 0);
                            let ele_count = n1 / ty.bytes() as usize;
                            let mut j = Vec::with_capacity(ele_count);
                            unsafe {
                                j.set_len(ele_count);
                            }
                            let mut p1 = 0;
                            for i1 in 0..ele_count {
                                let u = unsafe {
                                    let mut r = [0u8; BY];
                                    std::ptr::copy_nonoverlapping(&decomp[p1], r.as_mut_ptr(), BY);
                                    f64::from_be_bytes(r)
                                    //f64::from_be_bytes(std::mem::transmute::<_, [u8; 8]>(&decomp[p1]))
                                };
                                j[i1] = u as f32;
                                p1 += BY;
                            }
                            ret.tss.push(k.tss[i1]);
                            ret.values.push(j);
                        }
                        _ => {
                            let e = Error::with_msg(format!("Dim1F32Stream  unhandled scalar type: {:?}", ty));
                            self.errored = true;
                            return Ready(Some(Err(e)));
                        }
                    }
                }
                let inst2 = Instant::now();
                let mut k = k;
                ret.event_data_read_stats.trans(&mut k.event_data_read_stats);
                ret.values_dim_1_extract_stats.dur += inst2.duration_since(inst1);
                Ready(Some(Ok(ret)))
            }
            Ready(Some(Err(e))) => {
                self.errored = true;
                Ready(Some(Err(e)))
            }
            Ready(None) => {
                self.completed = true;
                Ready(None)
            }
            Pending => Pending,
        }
    }
}

pub trait IntoDim1F32Stream {
    fn into_dim_1_f32_stream(self) -> Dim1F32Stream<Self>
    where
        Self: Stream<Item = Result<EventFull, Error>> + Sized;
}

impl<T> IntoDim1F32Stream for T
where
    T: Stream<Item = Result<EventFull, Error>>,
{
    fn into_dim_1_f32_stream(self) -> Dim1F32Stream<T> {
        Dim1F32Stream::new(self)
    }
}

pub fn make_test_node(id: u32) -> Node {
    Node {
        host: "localhost".into(),
        listen: "0.0.0.0".into(),
        port: 8800 + id as u16,
        port_raw: 8800 + id as u16 + 100,
        data_base_path: format!("../tmpdata/node{:02}", id).into(),
        split: id,
        ksprefix: "ks".into(),
    }
}
