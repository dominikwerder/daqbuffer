/*!
Aggregation and binning support.
*/

use super::eventchunker::EventFull;
use crate::agg::binnedt::AggregatableTdim;
use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::streams::StreamItem;
use crate::binned::{BinnedStreamKind, RangeCompletableItem};
use bytes::BytesMut;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::NanoRange;
use netpod::ScalarType;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub mod binnedt;
pub mod binnedt2;
pub mod binnedt3;
pub mod binnedx;
pub mod eventbatch;
pub mod scalarbinbatch;
pub mod streams;

pub trait AggregatableXdim1Bin<SK>
where
    SK: BinnedStreamKind,
{
    type Output: AggregatableXdim1Bin<SK> + AggregatableTdim<SK>;
    fn into_agg(self) -> Self::Output;
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

impl<SK> AggregatableXdim1Bin<SK> for ValuesDim1
where
    SK: BinnedStreamKind,
{
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
            let mut sum = f32::NAN;
            let mut count = 0;
            let vals = &self.values[i1];
            for i2 in 0..vals.len() {
                let v = vals[i2];
                min = min.min(v);
                max = max.max(v);
                if v.is_nan() {
                } else {
                    if sum.is_nan() {
                        sum = v;
                    } else {
                        sum += v;
                    }
                    count += 1;
                }
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
            if sum.is_nan() {
                ret.avgs.push(sum);
            } else {
                ret.avgs.push(sum / count as f32);
            }
        }
        ret
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValuesExtractStats {
    pub dur: Duration,
}

impl ValuesExtractStats {
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
}

impl ValuesDim1 {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            values: vec![],
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

impl<SK> AggregatableXdim1Bin<SK> for ValuesDim0
where
    SK: BinnedStreamKind,
{
    type Output = MinMaxAvgScalarEventBatch;

    fn into_agg(self) -> Self::Output {
        let mut ret = MinMaxAvgScalarEventBatch {
            tss: Vec::with_capacity(self.tss.len()),
            mins: Vec::with_capacity(self.tss.len()),
            maxs: Vec::with_capacity(self.tss.len()),
            avgs: Vec::with_capacity(self.tss.len()),
        };
        // TODO stats are not yet in ValuesDim0
        err::todoval::<u32>();
        //if self.range_complete_observed {
        //    ret.range_complete_observed = true;
        //}
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

pub struct Dim1F32Stream<S> {
    inp: S,
    errored: bool,
    completed: bool,
}

trait NumEx {
    const BY: usize;
}

struct NumF32;
impl NumEx for NumF32 {
    const BY: usize = 4;
}

macro_rules! make_get_values {
    ($n:ident, $TY:ident, $FROM_BYTES:ident, $BY:expr) => {
        fn $n(decomp: &BytesMut, ty: &ScalarType) -> Result<Vec<f32>, Error> {
            let n1 = decomp.len();
            if ty.bytes() as usize != $BY {
                Err(Error::with_msg(format!(
                    "ty.bytes() != BY  {} vs {}",
                    ty.bytes(),
                    $BY
                )))?;
            }
            if n1 % ty.bytes() as usize != 0 {
                Err(Error::with_msg(format!(
                    "n1 % ty.bytes() as usize != 0  {} vs {}",
                    n1,
                    ty.bytes()
                )))?;
            }
            let ele_count = n1 / ty.bytes() as usize;
            let mut j = Vec::with_capacity(ele_count);
            let mut p2 = j.as_mut_ptr();
            let mut p1 = 0;
            for _ in 0..ele_count {
                unsafe {
                    let mut r = [0u8; $BY];
                    std::ptr::copy_nonoverlapping(&decomp[p1], r.as_mut_ptr(), $BY);
                    *p2 = $TY::$FROM_BYTES(r) as f32;
                    p1 += $BY;
                    p2 = p2.add(1);
                };
            }
            unsafe {
                j.set_len(ele_count);
            }
            Ok(j)
        }
    };
}

make_get_values!(get_values_u8_le, u8, from_le_bytes, 1);
make_get_values!(get_values_u16_le, u16, from_le_bytes, 2);
make_get_values!(get_values_u32_le, u32, from_le_bytes, 4);
make_get_values!(get_values_u64_le, u64, from_le_bytes, 8);
make_get_values!(get_values_i8_le, i8, from_le_bytes, 1);
make_get_values!(get_values_i16_le, i16, from_le_bytes, 2);
make_get_values!(get_values_i32_le, i32, from_le_bytes, 4);
make_get_values!(get_values_i64_le, i64, from_le_bytes, 8);
make_get_values!(get_values_f32_le, f32, from_le_bytes, 4);
make_get_values!(get_values_f64_le, f64, from_le_bytes, 8);

make_get_values!(get_values_u8_be, u8, from_be_bytes, 1);
make_get_values!(get_values_u16_be, u16, from_be_bytes, 2);
make_get_values!(get_values_u32_be, u32, from_be_bytes, 4);
make_get_values!(get_values_u64_be, u64, from_be_bytes, 8);
make_get_values!(get_values_i8_be, i8, from_be_bytes, 1);
make_get_values!(get_values_i16_be, i16, from_be_bytes, 2);
make_get_values!(get_values_i32_be, i32, from_be_bytes, 4);
make_get_values!(get_values_i64_be, i64, from_be_bytes, 8);
make_get_values!(get_values_f32_be, f32, from_be_bytes, 4);
make_get_values!(get_values_f64_be, f64, from_be_bytes, 8);

impl<S> Dim1F32Stream<S> {
    pub fn new(inp: S) -> Self {
        Self {
            inp,
            errored: false,
            completed: false,
        }
    }

    fn process_event_data(&mut self, k: &EventFull) -> Result<ValuesDim1, Error> {
        let mut ret = ValuesDim1::empty();
        use ScalarType::*;
        for i1 in 0..k.tss.len() {
            // TODO iterate sibling arrays after single bounds check
            let ty = &k.scalar_types[i1];
            let be = k.be[i1];
            let decomp = k.decomps[i1].as_ref().unwrap();
            match ty {
                U8 => {
                    let value = if be {
                        get_values_u8_be(decomp, ty)?
                    } else {
                        get_values_u8_le(decomp, ty)?
                    };
                    ret.tss.push(k.tss[i1]);
                    ret.values.push(value);
                }
                U16 => {
                    let value = if be {
                        get_values_u16_be(decomp, ty)?
                    } else {
                        get_values_u16_le(decomp, ty)?
                    };
                    ret.tss.push(k.tss[i1]);
                    ret.values.push(value);
                }
                U32 => {
                    let value = if be {
                        get_values_u32_be(decomp, ty)?
                    } else {
                        get_values_u32_le(decomp, ty)?
                    };
                    ret.tss.push(k.tss[i1]);
                    ret.values.push(value);
                }
                U64 => {
                    let value = if be {
                        get_values_u64_be(decomp, ty)?
                    } else {
                        get_values_u64_le(decomp, ty)?
                    };
                    ret.tss.push(k.tss[i1]);
                    ret.values.push(value);
                }
                I8 => {
                    let value = if be {
                        get_values_i8_be(decomp, ty)?
                    } else {
                        get_values_i8_le(decomp, ty)?
                    };
                    ret.tss.push(k.tss[i1]);
                    ret.values.push(value);
                }
                I16 => {
                    let value = if be {
                        get_values_i16_be(decomp, ty)?
                    } else {
                        get_values_i16_le(decomp, ty)?
                    };
                    ret.tss.push(k.tss[i1]);
                    ret.values.push(value);
                }
                I32 => {
                    let value = if be {
                        get_values_i32_be(decomp, ty)?
                    } else {
                        get_values_i32_le(decomp, ty)?
                    };
                    ret.tss.push(k.tss[i1]);
                    ret.values.push(value);
                }
                I64 => {
                    let value = if be {
                        get_values_i64_be(decomp, ty)?
                    } else {
                        get_values_i64_le(decomp, ty)?
                    };
                    ret.tss.push(k.tss[i1]);
                    ret.values.push(value);
                }
                F32 => {
                    let value = if be {
                        get_values_f32_be(decomp, ty)?
                    } else {
                        get_values_f32_le(decomp, ty)?
                    };
                    ret.tss.push(k.tss[i1]);
                    ret.values.push(value);
                }
                F64 => {
                    let value = if be {
                        get_values_f64_be(decomp, ty)?
                    } else {
                        get_values_f64_le(decomp, ty)?
                    };
                    ret.tss.push(k.tss[i1]);
                    ret.values.push(value);
                }
            }
        }
        Ok(ret)
    }
}

impl<S> Stream for Dim1F32Stream<S>
where
    S: Stream<Item = Result<StreamItem<RangeCompletableItem<EventFull>>, Error>> + Unpin,
{
    type Item = Result<StreamItem<RangeCompletableItem<ValuesDim1>>, Error>;

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
                let u = match k {
                    StreamItem::DataItem(item) => match item {
                        RangeCompletableItem::RangeComplete => {
                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                        }
                        RangeCompletableItem::Data(item) => match self.process_event_data(&item) {
                            Ok(item) => {
                                let ret = RangeCompletableItem::Data(item);
                                let ret = StreamItem::DataItem(ret);
                                Ready(Some(Ok(ret)))
                            }
                            Err(e) => {
                                self.errored = true;
                                Ready(Some(Err(e)))
                            }
                        },
                    },
                    StreamItem::Log(item) => Ready(Some(Ok(StreamItem::Log(item)))),
                    StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                };
                let inst2 = Instant::now();
                // TODO  do something with the measured time.
                let _ = inst2.duration_since(inst1);
                u
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
        Self: Stream<Item = Result<StreamItem<RangeCompletableItem<EventFull>>, Error>> + Sized;
}

impl<T> IntoDim1F32Stream for T
where
    T: Stream<Item = Result<StreamItem<RangeCompletableItem<EventFull>>, Error>>,
{
    fn into_dim_1_f32_stream(self) -> Dim1F32Stream<T> {
        Dim1F32Stream::new(self)
    }
}
