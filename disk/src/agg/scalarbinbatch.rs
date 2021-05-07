use crate::agg::binnedt::{AggregatableTdim, AggregatorTdim};
use crate::agg::{AggregatableXdim1Bin, Fits, FitsInside};
use crate::streamlog::LogItem;
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
            "MinMaxAvgScalarBinBatch  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  avgs {:?}",
            self.ts1s.len(),
            self.ts1s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.ts2s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.counts,
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

    fn is_range_complete(&self) -> bool {
        false
    }

    fn make_range_complete_item() -> Option<Self> {
        None
    }

    fn is_log_item(&self) -> bool {
        false
    }

    fn log_item(self) -> Option<LogItem> {
        None
    }

    fn make_log_item(_item: LogItem) -> Option<Self> {
        None
    }

    fn is_stats_item(&self) -> bool {
        false
    }

    fn stats_item(self) -> Option<EventDataReadStats> {
        None
    }

    fn make_stats_item(_item: EventDataReadStats) -> Option<Self> {
        None
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

    fn result(self) -> Vec<Self::OutputValue> {
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
        };
        vec![v]
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MinMaxAvgScalarBinBatchStreamItem {
    Values(MinMaxAvgScalarBinBatch),
    RangeComplete,
    EventDataReadStats(EventDataReadStats),
    Log(LogItem),
}

impl AggregatableTdim for MinMaxAvgScalarBinBatchStreamItem {
    type Output = MinMaxAvgScalarBinBatchStreamItem;
    type Aggregator = MinMaxAvgScalarBinBatchStreamItemAggregator;

    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator {
        Self::Aggregator::new(ts1, ts2)
    }

    fn is_range_complete(&self) -> bool {
        if let MinMaxAvgScalarBinBatchStreamItem::RangeComplete = self {
            true
        } else {
            false
        }
    }

    fn make_range_complete_item() -> Option<Self> {
        Some(MinMaxAvgScalarBinBatchStreamItem::RangeComplete)
    }

    fn is_log_item(&self) -> bool {
        if let MinMaxAvgScalarBinBatchStreamItem::Log(_) = self {
            true
        } else {
            false
        }
    }

    fn log_item(self) -> Option<LogItem> {
        if let MinMaxAvgScalarBinBatchStreamItem::Log(item) = self {
            Some(item)
        } else {
            None
        }
    }

    fn make_log_item(item: LogItem) -> Option<Self> {
        Some(MinMaxAvgScalarBinBatchStreamItem::Log(item))
    }

    fn is_stats_item(&self) -> bool {
        if let MinMaxAvgScalarBinBatchStreamItem::EventDataReadStats(_) = self {
            true
        } else {
            false
        }
    }

    fn stats_item(self) -> Option<EventDataReadStats> {
        if let MinMaxAvgScalarBinBatchStreamItem::EventDataReadStats(item) = self {
            Some(item)
        } else {
            None
        }
    }

    fn make_stats_item(item: EventDataReadStats) -> Option<Self> {
        Some(MinMaxAvgScalarBinBatchStreamItem::EventDataReadStats(item))
    }
}

impl AggregatableXdim1Bin for MinMaxAvgScalarBinBatchStreamItem {
    type Output = MinMaxAvgScalarBinBatchStreamItem;

    fn into_agg(self) -> Self::Output {
        self
    }
}

pub struct MinMaxAvgScalarBinBatchStreamItemAggregator {
    agg: MinMaxAvgScalarBinBatchAggregator,
}

impl MinMaxAvgScalarBinBatchStreamItemAggregator {
    pub fn new(ts1: u64, ts2: u64) -> Self {
        let agg = <MinMaxAvgScalarBinBatch as AggregatableTdim>::aggregator_new_static(ts1, ts2);
        Self { agg }
    }
}

impl AggregatorTdim for MinMaxAvgScalarBinBatchStreamItemAggregator {
    type InputValue = MinMaxAvgScalarBinBatchStreamItem;
    type OutputValue = MinMaxAvgScalarBinBatchStreamItem;

    fn ends_before(&self, inp: &Self::InputValue) -> bool {
        match inp {
            MinMaxAvgScalarBinBatchStreamItem::Values(vals) => self.agg.ends_before(vals),
            _ => false,
        }
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        match inp {
            MinMaxAvgScalarBinBatchStreamItem::Values(vals) => self.agg.ends_after(vals),
            _ => false,
        }
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        match inp {
            MinMaxAvgScalarBinBatchStreamItem::Values(vals) => self.agg.starts_after(vals),
            _ => false,
        }
    }

    fn ingest(&mut self, inp: &mut Self::InputValue) {
        match inp {
            MinMaxAvgScalarBinBatchStreamItem::Values(vals) => self.agg.ingest(vals),
            MinMaxAvgScalarBinBatchStreamItem::EventDataReadStats(_) => panic!(),
            MinMaxAvgScalarBinBatchStreamItem::RangeComplete => panic!(),
            MinMaxAvgScalarBinBatchStreamItem::Log(_) => panic!(),
        }
    }

    fn result(self) -> Vec<Self::OutputValue> {
        let ret: Vec<_> = self
            .agg
            .result()
            .into_iter()
            .map(MinMaxAvgScalarBinBatchStreamItem::Values)
            .collect();
        ret
    }
}
