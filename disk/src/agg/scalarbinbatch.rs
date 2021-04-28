use crate::agg::{AggregatableTdim, AggregatableXdim1Bin, AggregatorTdim, Fits, FitsInside, MinMaxAvgScalarBinSingle};
use bytes::{BufMut, Bytes, BytesMut};
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::NanoRange;
use serde::{Deserialize, Serialize};
use std::mem::size_of;

#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
pub struct MinMaxAvgScalarBinBatch {
    ts1s: Vec<u64>,
    ts2s: Vec<u64>,
    counts: Vec<u64>,
    mins: Vec<f32>,
    maxs: Vec<f32>,
    avgs: Vec<f32>,
}

impl MinMaxAvgScalarBinBatch {
    pub fn empty() -> Self {
        Self {
            ts1s: vec![],
            ts2s: vec![],
            counts: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
        }
    }
    pub fn len(&self) -> usize {
        self.ts1s.len()
    }
    pub fn push_single(&mut self, g: &MinMaxAvgScalarBinSingle) {
        self.ts1s.push(g.ts1);
        self.ts2s.push(g.ts2);
        self.counts.push(g.count);
        self.mins.push(g.min);
        self.maxs.push(g.max);
        self.avgs.push(g.avg);
    }
    pub fn from_full_frame(buf: &Bytes) -> Self {
        info!("MinMaxAvgScalarBinBatch  construct from full frame  len {}", buf.len());
        assert!(buf.len() >= 4);
        let mut g = MinMaxAvgScalarBinBatch::empty();
        let n1;
        unsafe {
            let ptr = (&buf[0] as *const u8) as *const [u8; 4];
            n1 = u32::from_le_bytes(*ptr);
            trace!(
                "MinMaxAvgScalarBinBatch  construct  --- +++ --- +++ --- +++  n1: {}",
                n1
            );
        }
        if n1 == 0 {
            g
        } else {
            let n2 = n1 as usize;
            g.ts1s.reserve(n2);
            g.ts2s.reserve(n2);
            g.counts.reserve(n2);
            g.mins.reserve(n2);
            g.maxs.reserve(n2);
            g.avgs.reserve(n2);
            unsafe {
                // TODO Can I unsafely create ptrs and just assign them?
                // TODO What are cases where I really need transmute?
                g.ts1s.set_len(n2);
                g.ts2s.set_len(n2);
                g.counts.set_len(n2);
                g.mins.set_len(n2);
                g.maxs.set_len(n2);
                g.avgs.set_len(n2);
                let ptr0 = &buf[4] as *const u8;
                {
                    let ptr1 = ptr0.add(0) as *const u64;
                    for i1 in 0..n2 {
                        g.ts1s[i1] = *ptr1.add(i1);
                    }
                }
                {
                    let ptr1 = ptr0.add((8) * n2) as *const u64;
                    for i1 in 0..n2 {
                        g.ts2s[i1] = *ptr1.add(i1);
                    }
                }
                {
                    let ptr1 = ptr0.add((8 + 8) * n2) as *const u64;
                    for i1 in 0..n2 {
                        g.counts[i1] = *ptr1.add(i1);
                    }
                }
                {
                    let ptr1 = ptr0.add((8 + 8 + 8) * n2) as *const f32;
                    for i1 in 0..n2 {
                        g.mins[i1] = *ptr1.add(i1);
                    }
                }
                {
                    let ptr1 = ptr0.add((8 + 8 + 8 + 4) * n2) as *const f32;
                    for i1 in 0..n2 {
                        g.maxs[i1] = *ptr1;
                    }
                }
                {
                    let ptr1 = ptr0.add((8 + 8 + 8 + 4 + 4) * n2) as *const f32;
                    for i1 in 0..n2 {
                        g.avgs[i1] = *ptr1;
                    }
                }
            }
            info!("CONTENT  {:?}", g);
            g
        }
    }
}

impl FitsInside for MinMaxAvgScalarBinBatch {
    fn fits_inside(&self, range: NanoRange) -> Fits {
        if self.ts1s.is_empty() {
            Fits::Empty
        } else {
            let t1 = *self.ts1s.first().unwrap();
            let t2 = *self.ts2s.last().unwrap();
            if t2 <= range.beg {
                Fits::Lower
            } else if t1 >= range.end {
                Fits::Greater
            } else if t1 < range.beg && t2 > range.end {
                Fits::PartlyLowerAndGreater
            } else if t1 < range.beg {
                Fits::PartlyLower
            } else if t2 > range.end {
                Fits::PartlyGreater
            } else {
                Fits::Inside
            }
        }
    }
}

impl MinMaxAvgScalarBinBatch {
    #[allow(dead_code)]
    fn old_serialized(&self) -> Bytes {
        let n1 = self.ts1s.len();
        let mut g = BytesMut::with_capacity(4 + n1 * (3 * 8 + 3 * 4));
        g.put_u32_le(n1 as u32);
        if n1 > 0 {
            let ptr = &self.ts1s[0] as *const u64 as *const u8;
            let a = unsafe { std::slice::from_raw_parts(ptr, size_of::<u64>() * n1) };
            g.put(a);
            let ptr = &self.ts2s[0] as *const u64 as *const u8;
            let a = unsafe { std::slice::from_raw_parts(ptr, size_of::<u64>() * n1) };
            g.put(a);
            let ptr = &self.counts[0] as *const u64 as *const u8;
            let a = unsafe { std::slice::from_raw_parts(ptr, size_of::<u64>() * n1) };
            g.put(a);
            let ptr = &self.mins[0] as *const f32 as *const u8;
            let a = unsafe { std::slice::from_raw_parts(ptr, size_of::<f32>() * n1) };
            g.put(a);
            let ptr = &self.maxs[0] as *const f32 as *const u8;
            let a = unsafe { std::slice::from_raw_parts(ptr, size_of::<f32>() * n1) };
            g.put(a);
            let ptr = &self.avgs[0] as *const f32 as *const u8;
            let a = unsafe { std::slice::from_raw_parts(ptr, size_of::<f32>() * n1) };
            g.put(a);
        }
        g.freeze()
    }
}

impl std::fmt::Debug for MinMaxAvgScalarBinBatch {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "MinMaxAvgScalarBinBatch  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  avgs {:?}",
            self.ts1s.len(),
            self.ts1s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.ts2s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.counts,
            self.avgs
        )
    }
}

impl AggregatableXdim1Bin for MinMaxAvgScalarBinBatch {
    type Output = MinMaxAvgScalarBinBatch;
    fn into_agg(self) -> Self::Output {
        todo!()
    }
}

impl AggregatableTdim for MinMaxAvgScalarBinBatch {
    type Output = MinMaxAvgScalarBinSingle;
    type Aggregator = MinMaxAvgScalarBinBatchAggregator;
    fn aggregator_new_static(_ts1: u64, _ts2: u64) -> Self::Aggregator {
        todo!()
    }
}

pub struct MinMaxAvgScalarBinBatchAggregator {}

impl AggregatorTdim for MinMaxAvgScalarBinBatchAggregator {
    type InputValue = MinMaxAvgScalarBinBatch;
    type OutputValue = MinMaxAvgScalarBinSingle;

    fn ends_before(&self, _inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn ends_after(&self, _inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn starts_after(&self, _inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn ingest(&mut self, _v: &Self::InputValue) {
        todo!()
    }

    fn result(self) -> Self::OutputValue {
        todo!()
    }
}
