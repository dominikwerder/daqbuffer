/*!
Aggregation and binning support.
*/

use super::eventchunker::EventFull;
use crate::agg::eventbatch::{MinMaxAvgScalarEventBatch, MinMaxAvgScalarEventBatchAggregator};
use crate::agg::scalarbinbatch::{MinMaxAvgScalarBinBatch, MinMaxAvgScalarBinBatchAggregator};
use crate::eventchunker::EventChunkerItem;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::ScalarType;
use netpod::{EventDataReadStats, NanoRange};
use serde::{Deserialize, Serialize};
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
    type OutputValue: AggregatableXdim1Bin + AggregatableTdim + Unpin;
    fn ends_before(&self, inp: &Self::InputValue) -> bool;
    fn ends_after(&self, inp: &Self::InputValue) -> bool;
    fn starts_after(&self, inp: &Self::InputValue) -> bool;
    fn ingest(&mut self, inp: &mut Self::InputValue);
    fn result(self) -> Vec<Self::OutputValue>;
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
    // TODO add the stats and flags
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

    fn into_agg(mut self) -> Self::Output {
        let mut ret = MinMaxAvgScalarEventBatch {
            tss: Vec::with_capacity(self.tss.len()),
            mins: Vec::with_capacity(self.tss.len()),
            maxs: Vec::with_capacity(self.tss.len()),
            avgs: Vec::with_capacity(self.tss.len()),
            event_data_read_stats: EventDataReadStats::new(),
            values_extract_stats: ValuesExtractStats::new(),
            range_complete_observed: false,
        };
        ret.event_data_read_stats.trans(&mut self.event_data_read_stats);
        ret.values_extract_stats.trans(&mut self.values_extract_stats);
        if self.range_complete_observed {
            ret.range_complete_observed = true;
        }
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
    pub event_data_read_stats: EventDataReadStats,
    pub values_extract_stats: ValuesExtractStats,
    pub range_complete_observed: bool,
}

impl ValuesDim1 {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            values: vec![],
            event_data_read_stats: EventDataReadStats::new(),
            values_extract_stats: ValuesExtractStats::new(),
            range_complete_observed: false,
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
            event_data_read_stats: EventDataReadStats::new(),
            values_extract_stats: ValuesExtractStats::new(),
            range_complete_observed: false,
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
                    return Err(e);
                }
            }
        }
        Ok(ret)
    }
}

#[derive(Debug)]
pub enum Dim1F32StreamItem {
    Values(ValuesDim1),
    RangeComplete,
    EventDataReadStats(EventDataReadStats),
}

impl<S> Stream for Dim1F32Stream<S>
where
    S: Stream<Item = Result<EventChunkerItem, Error>> + Unpin,
{
    type Item = Result<Dim1F32StreamItem, Error>;

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
                let u = match &k {
                    EventChunkerItem::Events(events) => match self.process_event_data(events) {
                        Ok(k) => {
                            let ret = Dim1F32StreamItem::Values(k);
                            Ready(Some(Ok(ret)))
                        }
                        Err(e) => {
                            self.errored = true;
                            Ready(Some(Err(e)))
                        }
                    },
                    EventChunkerItem::RangeComplete => Ready(Some(Ok(Dim1F32StreamItem::RangeComplete))),
                    EventChunkerItem::EventDataReadStats(_stats) => {
                        // TODO  ret.event_data_read_stats.trans(&mut k.event_data_read_stats);
                        // TODO  ret.values_extract_stats.dur += inst2.duration_since(inst1);
                        err::todoval()
                    }
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
        Self: Stream<Item = Result<EventChunkerItem, Error>> + Sized;
}

impl<T> IntoDim1F32Stream for T
where
    T: Stream<Item = Result<EventChunkerItem, Error>>,
{
    fn into_dim_1_f32_stream(self) -> Dim1F32Stream<T> {
        Dim1F32Stream::new(self)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MinMaxAvgScalarEventBatchStreamItem {
    Values(MinMaxAvgScalarEventBatch),
    RangeComplete,
    EventDataReadStats(EventDataReadStats),
}

impl AggregatableXdim1Bin for Dim1F32StreamItem {
    type Output = MinMaxAvgScalarEventBatchStreamItem;

    fn into_agg(self) -> Self::Output {
        match self {
            Dim1F32StreamItem::Values(vals) => MinMaxAvgScalarEventBatchStreamItem::Values(vals.into_agg()),
            _ => panic!(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MinMaxAvgScalarBinBatchStreamItem {
    Values(MinMaxAvgScalarBinBatch),
    RangeComplete,
    EventDataReadStats(EventDataReadStats),
}

pub struct MinMaxAvgScalarEventBatchStreamItemAggregator {
    agg: MinMaxAvgScalarEventBatchAggregator,
    event_data_read_stats: EventDataReadStats,
}

impl MinMaxAvgScalarEventBatchStreamItemAggregator {
    pub fn new(ts1: u64, ts2: u64) -> Self {
        let agg = <MinMaxAvgScalarEventBatch as AggregatableTdim>::aggregator_new_static(ts1, ts2);
        Self {
            agg,
            event_data_read_stats: EventDataReadStats::new(),
        }
    }
}

impl AggregatorTdim for MinMaxAvgScalarEventBatchStreamItemAggregator {
    type InputValue = MinMaxAvgScalarEventBatchStreamItem;
    type OutputValue = MinMaxAvgScalarBinBatchStreamItem;

    fn ends_before(&self, inp: &Self::InputValue) -> bool {
        match inp {
            MinMaxAvgScalarEventBatchStreamItem::Values(vals) => self.agg.ends_before(vals),
            _ => todo!(),
        }
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        match inp {
            MinMaxAvgScalarEventBatchStreamItem::Values(vals) => self.agg.ends_after(vals),
            _ => todo!(),
        }
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        match inp {
            MinMaxAvgScalarEventBatchStreamItem::Values(vals) => self.agg.starts_after(vals),
            _ => todo!(),
        }
    }

    fn ingest(&mut self, inp: &mut Self::InputValue) {
        match inp {
            MinMaxAvgScalarEventBatchStreamItem::Values(vals) => self.agg.ingest(vals),
            MinMaxAvgScalarEventBatchStreamItem::EventDataReadStats(stats) => self.event_data_read_stats.trans(stats),
            MinMaxAvgScalarEventBatchStreamItem::RangeComplete => panic!(),
        }
    }

    fn result(self) -> Vec<Self::OutputValue> {
        let mut ret: Vec<Self::OutputValue> = self
            .agg
            .result()
            .into_iter()
            .map(MinMaxAvgScalarBinBatchStreamItem::Values)
            .collect();
        ret.push(MinMaxAvgScalarBinBatchStreamItem::EventDataReadStats(
            self.event_data_read_stats,
        ));
        ret
    }
}

impl AggregatableTdim for MinMaxAvgScalarEventBatchStreamItem {
    type Output = MinMaxAvgScalarBinBatchStreamItem;
    type Aggregator = MinMaxAvgScalarEventBatchStreamItemAggregator;

    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator {
        //<Self as AggregatableTdim>::Aggregator::new(ts1, ts2)
        Self::Aggregator::new(ts1, ts2)
    }
}

impl AggregatableXdim1Bin for MinMaxAvgScalarEventBatchStreamItem {
    type Output = MinMaxAvgScalarEventBatchStreamItem;

    fn into_agg(self) -> Self::Output {
        self
    }
}

pub struct MinMaxAvgScalarBinBatchStreamItemAggregator {
    agg: MinMaxAvgScalarBinBatchAggregator,
    event_data_read_stats: EventDataReadStats,
}

impl MinMaxAvgScalarBinBatchStreamItemAggregator {
    pub fn new(ts1: u64, ts2: u64) -> Self {
        let agg = <MinMaxAvgScalarBinBatch as AggregatableTdim>::aggregator_new_static(ts1, ts2);
        Self {
            agg,
            event_data_read_stats: EventDataReadStats::new(),
        }
    }
}

impl AggregatorTdim for MinMaxAvgScalarBinBatchStreamItemAggregator {
    type InputValue = MinMaxAvgScalarBinBatchStreamItem;
    type OutputValue = MinMaxAvgScalarBinBatchStreamItem;

    fn ends_before(&self, inp: &Self::InputValue) -> bool {
        match inp {
            MinMaxAvgScalarBinBatchStreamItem::Values(vals) => self.agg.ends_before(vals),
            _ => todo!(),
        }
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        match inp {
            MinMaxAvgScalarBinBatchStreamItem::Values(vals) => self.agg.ends_after(vals),
            _ => todo!(),
        }
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        match inp {
            MinMaxAvgScalarBinBatchStreamItem::Values(vals) => self.agg.starts_after(vals),
            _ => todo!(),
        }
    }

    fn ingest(&mut self, inp: &mut Self::InputValue) {
        match inp {
            MinMaxAvgScalarBinBatchStreamItem::Values(vals) => self.agg.ingest(vals),
            MinMaxAvgScalarBinBatchStreamItem::EventDataReadStats(stats) => self.event_data_read_stats.trans(stats),
            MinMaxAvgScalarBinBatchStreamItem::RangeComplete => panic!(),
        }
    }

    fn result(self) -> Vec<Self::OutputValue> {
        let mut ret: Vec<Self::OutputValue> = self
            .agg
            .result()
            .into_iter()
            .map(MinMaxAvgScalarBinBatchStreamItem::Values)
            .collect();
        ret.push(MinMaxAvgScalarBinBatchStreamItem::EventDataReadStats(
            self.event_data_read_stats,
        ));
        ret
    }
}

impl AggregatableTdim for MinMaxAvgScalarBinBatchStreamItem {
    type Output = MinMaxAvgScalarBinBatchStreamItem;
    type Aggregator = MinMaxAvgScalarBinBatchStreamItemAggregator;

    fn aggregator_new_static(ts1: u64, ts2: u64) -> Self::Aggregator {
        Self::Aggregator::new(ts1, ts2)
    }
}

impl AggregatableXdim1Bin for MinMaxAvgScalarBinBatchStreamItem {
    type Output = MinMaxAvgScalarBinBatchStreamItem;

    fn into_agg(self) -> Self::Output {
        self
    }
}
