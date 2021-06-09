use crate::agg::streams::{Appendable, StreamItem, ToJsonBytes};
use crate::agg::{Fits, FitsInside};
use crate::binned::{MakeBytesFrame, RangeCompletableItem, StreamKind};
use crate::frame::makeframe::make_frame;
use bytes::{BufMut, Bytes, BytesMut};
use err::Error;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::NanoRange;
use serde::{Deserialize, Serialize};
use std::mem::size_of;

#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
pub struct MinMaxAvgScalarBinBatch {
    pub ts1s: Vec<u64>,
    pub ts2s: Vec<u64>,
    pub counts: Vec<u64>,
    pub mins: Vec<f32>,
    pub maxs: Vec<f32>,
    pub avgs: Vec<f32>,
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

    #[allow(dead_code)]
    fn old_from_full_frame(buf: &Bytes) -> Self {
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

impl std::fmt::Debug for MinMaxAvgScalarBinBatch {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "MinMaxAvgScalarBinBatch  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  mins {:?}  maxs {:?}  avgs {:?}",
            self.ts1s.len(),
            self.ts1s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.ts2s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.counts,
            self.mins,
            self.maxs,
            self.avgs,
        )
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

pub struct MinMaxAvgScalarBinBatchAggregator {
    ts1: u64,
    ts2: u64,
    count: u64,
    min: f32,
    max: f32,
    sum: f32,
    sumc: u64,
}

impl MinMaxAvgScalarBinBatchAggregator {
    pub fn new(ts1: u64, ts2: u64) -> Self {
        Self {
            ts1,
            ts2,
            count: 0,
            min: f32::MAX,
            max: f32::MIN,
            sum: 0f32,
            sumc: 0,
        }
    }
}

impl MakeBytesFrame for Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarBinBatch>>, Error> {
    fn make_bytes_frame(&self) -> Result<Bytes, Error> {
        Ok(make_frame(self)?.freeze())
    }
}

impl Appendable for MinMaxAvgScalarBinBatch {
    fn empty() -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.ts1s.extend_from_slice(&src.ts1s);
        self.ts2s.extend_from_slice(&src.ts2s);
        self.counts.extend_from_slice(&src.counts);
        self.mins.extend_from_slice(&src.mins);
        self.maxs.extend_from_slice(&src.maxs);
        self.avgs.extend_from_slice(&src.avgs);
    }
}

impl ToJsonBytes for MinMaxAvgScalarBinBatch {
    fn to_json_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(self)?)
    }
}
