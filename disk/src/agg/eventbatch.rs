use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::{AggregatableTdim, AggregatableXdim1Bin, AggregatorTdim, ValuesExtractStats};
use bytes::{BufMut, Bytes, BytesMut};
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::EventDataReadStats;
use serde::{Deserialize, Serialize};
use std::mem::size_of;

#[derive(Serialize, Deserialize)]
pub struct MinMaxAvgScalarEventBatch {
    pub tss: Vec<u64>,
    pub mins: Vec<f32>,
    pub maxs: Vec<f32>,
    pub avgs: Vec<f32>,
    pub event_data_read_stats: EventDataReadStats,
    pub values_extract_stats: ValuesExtractStats,
    pub range_complete_observed: bool,
}

impl MinMaxAvgScalarEventBatch {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
            event_data_read_stats: EventDataReadStats::new(),
            values_extract_stats: ValuesExtractStats::new(),
            range_complete_observed: false,
        }
    }

    #[allow(dead_code)]
    pub fn old_from_full_frame(buf: &Bytes) -> Self {
        info!("construct MinMaxAvgScalarEventBatch from full frame  len {}", buf.len());
        assert!(buf.len() >= 4);
        let mut g = MinMaxAvgScalarEventBatch::empty();
        let n1;
        unsafe {
            let ptr = (&buf[0] as *const u8) as *const [u8; 4];
            n1 = u32::from_le_bytes(*ptr);
            trace!("--- +++ --- +++ --- +++  n1: {}", n1);
        }
        if n1 == 0 {
            g
        } else {
            let n2 = n1 as usize;
            g.tss.reserve(n2);
            g.mins.reserve(n2);
            g.maxs.reserve(n2);
            g.avgs.reserve(n2);
            unsafe {
                // TODO Can I unsafely create ptrs and just assign them?
                // TODO What are cases where I really need transmute?
                g.tss.set_len(n2);
                g.mins.set_len(n2);
                g.maxs.set_len(n2);
                g.avgs.set_len(n2);
                let ptr0 = &buf[4] as *const u8;
                {
                    let ptr1 = ptr0 as *const u64;
                    for i1 in 0..n2 {
                        g.tss[i1] = *ptr1.add(i1);
                    }
                }
                {
                    let ptr1 = ptr0.add((8) * n2) as *const f32;
                    for i1 in 0..n2 {
                        g.mins[i1] = *ptr1.add(i1);
                    }
                }
                {
                    let ptr1 = ptr0.add((8 + 4) * n2) as *const f32;
                    for i1 in 0..n2 {
                        g.maxs[i1] = *ptr1;
                    }
                }
                {
                    let ptr1 = ptr0.add((8 + 4 + 4) * n2) as *const f32;
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

impl std::fmt::Debug for MinMaxAvgScalarEventBatch {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "MinMaxAvgScalarEventBatch  count {}  tss {:?}  mins {:?}  maxs {:?}  avgs {:?}  EDS {:?}  VXS {:?}",
            self.tss.len(),
            self.tss,
            self.mins,
            self.maxs,
            self.avgs,
            self.event_data_read_stats,
            self.values_extract_stats,
        )
    }
}

impl AggregatableXdim1Bin for MinMaxAvgScalarEventBatch {
    type Output = MinMaxAvgScalarEventBatch;
    fn into_agg(self) -> Self::Output {
        self
    }
}

impl AggregatableTdim for MinMaxAvgScalarEventBatch {
    type Output = MinMaxAvgScalarBinBatch;
    type Aggregator = MinMaxAvgScalarEventBatchAggregator;
    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator {
        MinMaxAvgScalarEventBatchAggregator::new(ts1, ts2)
    }
}

impl MinMaxAvgScalarEventBatch {
    #[allow(dead_code)]
    fn old_serialized(&self) -> Bytes {
        let n1 = self.tss.len();
        let mut g = BytesMut::with_capacity(4 + n1 * (8 + 3 * 4));
        g.put_u32_le(n1 as u32);
        if n1 > 0 {
            let ptr = &self.tss[0] as *const u64 as *const u8;
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
        info!("impl Frameable for MinMaxAvgScalarEventBatch  g.len() {}", g.len());
        g.freeze()
    }
}

pub struct MinMaxAvgScalarEventBatchAggregator {
    ts1: u64,
    ts2: u64,
    count: u64,
    min: f32,
    max: f32,
    sum: f32,
    event_data_read_stats: EventDataReadStats,
    values_extract_stats: ValuesExtractStats,
    range_complete_observed: bool,
}

impl MinMaxAvgScalarEventBatchAggregator {
    pub fn new(ts1: u64, ts2: u64) -> Self {
        Self {
            ts1,
            ts2,
            min: f32::MAX,
            max: f32::MIN,
            sum: 0f32,
            count: 0,
            event_data_read_stats: EventDataReadStats::new(),
            values_extract_stats: ValuesExtractStats::new(),
            range_complete_observed: false,
        }
    }
}

impl AggregatorTdim for MinMaxAvgScalarEventBatchAggregator {
    type InputValue = MinMaxAvgScalarEventBatch;
    type OutputValue = MinMaxAvgScalarBinBatch;

    fn ends_before(&self, inp: &Self::InputValue) -> bool {
        match inp.tss.last() {
            Some(&ts) => ts < self.ts1,
            None => true,
        }
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        match inp.tss.last() {
            Some(&ts) => ts >= self.ts2,
            _ => panic!(),
        }
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        match inp.tss.first() {
            Some(&ts) => ts >= self.ts2,
            _ => panic!(),
        }
    }

    fn ingest(&mut self, v: &mut Self::InputValue) {
        if false {
            trace!(
                "ingest  {}  {}  {}   {:?}  {:?}",
                self.ends_before(v),
                self.ends_after(v),
                self.starts_after(v),
                v.tss.first().map(|k| k / SEC),
                v.tss.last().map(|k| k / SEC),
            );
        }
        self.event_data_read_stats.trans(&mut v.event_data_read_stats);
        self.values_extract_stats.trans(&mut v.values_extract_stats);
        if v.range_complete_observed {
            self.range_complete_observed = true;
        }
        for i1 in 0..v.tss.len() {
            let ts = v.tss[i1];
            if ts < self.ts1 {
                trace!(
                    "EventBatchAgg  {}  {}  {}  {}   IS BEFORE",
                    v.tss[i1],
                    v.mins[i1],
                    v.maxs[i1],
                    v.avgs[i1]
                );
                continue;
            } else if ts >= self.ts2 {
                trace!(
                    "EventBatchAgg  {}  {}  {}  {}   IS AFTER",
                    v.tss[i1],
                    v.mins[i1],
                    v.maxs[i1],
                    v.avgs[i1]
                );
                continue;
            } else {
                trace!(
                    "EventBatchAgg  {}  {}  {}  {}",
                    v.tss[i1],
                    v.mins[i1],
                    v.maxs[i1],
                    v.avgs[i1]
                );
                self.min = self.min.min(v.mins[i1]);
                self.max = self.max.max(v.maxs[i1]);
                self.sum += v.avgs[i1];
                self.count += 1;
            }
        }
    }

    fn result(mut self) -> Vec<Self::OutputValue> {
        let min = if self.min == f32::MAX { f32::NAN } else { self.min };
        let max = if self.max == f32::MIN { f32::NAN } else { self.max };
        let avg = if self.count == 0 {
            f32::NAN
        } else {
            self.sum / self.count as f32
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
