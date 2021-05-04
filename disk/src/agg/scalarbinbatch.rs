use crate::agg::{AggregatableTdim, AggregatableXdim1Bin, AggregatorTdim, Fits, FitsInside, ValuesExtractStats};
use bytes::{BufMut, Bytes, BytesMut};
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{EventDataReadStats, NanoRange};
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
    pub event_data_read_stats: EventDataReadStats,
    pub values_extract_stats: ValuesExtractStats,
    pub range_complete_observed: bool,
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
            event_data_read_stats: EventDataReadStats::new(),
            values_extract_stats: ValuesExtractStats::new(),
            range_complete_observed: false,
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
            "MinMaxAvgScalarBinBatch  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  avgs {:?}  EDS {:?}  VXS {:?}  COMP {}",
            self.ts1s.len(),
            self.ts1s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.ts2s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.counts,
            self.avgs,
            self.event_data_read_stats,
            self.values_extract_stats,
            self.range_complete_observed,
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

impl AggregatableXdim1Bin for MinMaxAvgScalarBinBatch {
    type Output = MinMaxAvgScalarBinBatch;
    fn into_agg(self) -> Self::Output {
        todo!()
    }
}

impl AggregatableTdim for MinMaxAvgScalarBinBatch {
    type Output = MinMaxAvgScalarBinBatch;
    type Aggregator = MinMaxAvgScalarBinBatchAggregator;
    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator {
        MinMaxAvgScalarBinBatchAggregator::new(ts1, ts2)
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
    event_data_read_stats: EventDataReadStats,
    values_extract_stats: ValuesExtractStats,
    range_complete_observed: bool,
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
            event_data_read_stats: EventDataReadStats::new(),
            values_extract_stats: ValuesExtractStats::new(),
            range_complete_observed: false,
        }
    }
}

impl AggregatorTdim for MinMaxAvgScalarBinBatchAggregator {
    type InputValue = MinMaxAvgScalarBinBatch;
    type OutputValue = MinMaxAvgScalarBinBatch;

    fn ends_before(&self, inp: &Self::InputValue) -> bool {
        match inp.ts2s.last() {
            Some(&ts) => ts <= self.ts1,
            None => true,
        }
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        match inp.ts2s.last() {
            Some(&ts) => ts >= self.ts2,
            _ => panic!(),
        }
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        match inp.ts1s.first() {
            Some(&ts) => ts >= self.ts2,
            _ => panic!(),
        }
    }

    fn ingest(&mut self, v: &mut Self::InputValue) {
        self.event_data_read_stats.trans(&mut v.event_data_read_stats);
        self.values_extract_stats.trans(&mut v.values_extract_stats);
        if v.range_complete_observed {
            self.range_complete_observed = true;
        }
        for i1 in 0..v.ts1s.len() {
            let ts1 = v.ts1s[i1];
            let ts2 = v.ts2s[i1];
            if ts2 <= self.ts1 {
                continue;
            } else if ts1 >= self.ts2 {
                continue;
            } else {
                self.count += v.counts[i1];
                self.min = self.min.min(v.mins[i1]);
                self.max = self.max.max(v.maxs[i1]);
                self.sum += v.avgs[i1];
                self.sumc += 1;
            }
        }
    }

    fn result(mut self) -> Vec<Self::OutputValue> {
        let min = if self.min == f32::MAX { f32::NAN } else { self.min };
        let max = if self.max == f32::MIN { f32::NAN } else { self.max };
        let avg = if self.sumc == 0 {
            f32::NAN
        } else {
            self.sum / self.sumc as f32
        };
        let v = MinMaxAvgScalarBinBatch {
            ts1s: vec![self.ts1],
            ts2s: vec![self.ts2],
            counts: vec![self.count],
            mins: vec![min],
            maxs: vec![max],
            avgs: vec![avg],
            event_data_read_stats: std::mem::replace(&mut self.event_data_read_stats, EventDataReadStats::new()),
            values_extract_stats: std::mem::replace(&mut self.values_extract_stats, ValuesExtractStats::new()),
            range_complete_observed: self.range_complete_observed,
        };
        vec![v]
    }
}
