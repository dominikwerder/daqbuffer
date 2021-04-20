/*!
Aggregation and binning support.
*/

use crate::EventFull;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::BinSpecDimT;
use netpod::{Node, ScalarType};
use std::pin::Pin;
use std::task::{Context, Poll};
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

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
    fn aggregator_new(&self, ts1: u64, ts2: u64) -> Self::Aggregator;
}

/// DO NOT USE. This is just a dummy for some testing.
impl AggregatableXdim1Bin for () {
    type Output = ();
    fn into_agg(self) -> Self::Output {
        todo!()
    }
}
/// DO NOT USE. This is just a dummy for some testing.
impl AggregatableTdim for () {
    type Output = ();
    type Aggregator = ();
    fn aggregator_new(&self, _ts1: u64, _ts2: u64) -> Self::Aggregator {
        todo!()
    }
}
/// DO NOT USE. This is just a dummy for some testing.
impl AggregatorTdim for () {
    type InputValue = ();
    type OutputValue = ();

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

pub struct MinMaxAvgScalarEventBatch {
    pub tss: Vec<u64>,
    pub mins: Vec<f32>,
    pub maxs: Vec<f32>,
    pub avgs: Vec<f32>,
}

impl MinMaxAvgScalarEventBatch {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
        }
    }
}

impl std::fmt::Debug for MinMaxAvgScalarEventBatch {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "MinMaxAvgScalarEventBatch  count {}", self.tss.len())
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

    fn aggregator_new(&self, ts1: u64, ts2: u64) -> Self::Aggregator {
        MinMaxAvgScalarEventBatchAggregator::new(ts1, ts2)
    }
}

pub struct MinMaxAvgScalarEventBatchAggregator {
    ts1: u64,
    ts2: u64,
    count: u64,
    min: f32,
    max: f32,
    sum: f32,
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
        }
    }
}

impl AggregatorTdim for MinMaxAvgScalarEventBatchAggregator {
    type InputValue = MinMaxAvgScalarEventBatch;
    type OutputValue = MinMaxAvgScalarBinSingle;

    fn ends_before(&self, inp: &Self::InputValue) -> bool {
        match inp.tss.last() {
            Some(ts) => *ts < self.ts1,
            None => true,
        }
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        match inp.tss.last() {
            Some(ts) => *ts >= self.ts2,
            _ => panic!(),
        }
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        match inp.tss.first() {
            Some(ts) => *ts >= self.ts2,
            _ => panic!(),
        }
    }

    fn ingest(&mut self, v: &Self::InputValue) {
        for i1 in 0..v.tss.len() {
            let ts = v.tss[i1];
            if ts < self.ts1 {
                //info!("EventBatchAgg  {}  {}  {}  {}   IS BEFORE", v.tss[i1], v.mins[i1], v.maxs[i1], v.avgs[i1]);
                continue;
            } else if ts >= self.ts2 {
                //info!("EventBatchAgg  {}  {}  {}  {}   IS AFTER", v.tss[i1], v.mins[i1], v.maxs[i1], v.avgs[i1]);
                continue;
            } else {
                //info!("EventBatchAgg  {}  {}  {}  {}", v.tss[i1], v.mins[i1], v.maxs[i1], v.avgs[i1]);
                self.min = self.min.min(v.mins[i1]);
                self.max = self.max.max(v.maxs[i1]);
                self.sum += v.avgs[i1];
                self.count += 1;
            }
        }
    }

    fn result(self) -> Self::OutputValue {
        let min = if self.min == f32::MAX { f32::NAN } else { self.min };
        let max = if self.max == f32::MIN { f32::NAN } else { self.max };
        let avg = if self.count == 0 {
            f32::NAN
        } else {
            self.sum / self.count as f32
        };
        MinMaxAvgScalarBinSingle {
            ts1: self.ts1,
            ts2: self.ts2,
            count: self.count,
            min,
            max,
            avg,
        }
    }
}

#[allow(dead_code)]
pub struct MinMaxAvgScalarBinBatch {
    ts1s: Vec<u64>,
    ts2s: Vec<u64>,
    counts: Vec<u64>,
    mins: Vec<f32>,
    maxs: Vec<f32>,
    avgs: Vec<f32>,
}

impl std::fmt::Debug for MinMaxAvgScalarBinBatch {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "MinMaxAvgScalarBinBatch  count {}", self.ts1s.len())
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
    fn aggregator_new(&self, _ts1: u64, _ts2: u64) -> Self::Aggregator {
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

pub struct MinMaxAvgScalarBinSingle {
    ts1: u64,
    ts2: u64,
    count: u64,
    min: f32,
    max: f32,
    avg: f32,
}

impl std::fmt::Debug for MinMaxAvgScalarBinSingle {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "MinMaxAvgScalarBinSingle  ts1 {}  ts2 {}  count {}  min {:7.2e}  max {:7.2e}  avg {:7.2e}",
            self.ts1, self.ts2, self.count, self.min, self.max, self.avg
        )
    }
}

impl AggregatableTdim for MinMaxAvgScalarBinSingle {
    type Output = MinMaxAvgScalarBinSingle;
    type Aggregator = MinMaxAvgScalarBinSingleAggregator;
    fn aggregator_new(&self, _ts1: u64, _ts2: u64) -> Self::Aggregator {
        todo!()
    }
}

impl AggregatableXdim1Bin for MinMaxAvgScalarBinSingle {
    type Output = MinMaxAvgScalarBinSingle;
    fn into_agg(self) -> Self::Output {
        self
    }
}

pub struct MinMaxAvgScalarBinSingleAggregator {}

impl AggregatorTdim for MinMaxAvgScalarBinSingleAggregator {
    type InputValue = MinMaxAvgScalarBinSingle;
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

pub struct Dim0F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>>,
{
    inp: S,
}

impl<S> Stream for Dim0F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>> + Unpin,
{
    type Item = Result<ValuesDim0, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => {
                let mut ret = ValuesDim1 {
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
                            if true {
                                todo!();
                            }

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
                Ready(Some(Ok(err::todoval())))
            }
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
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
        Dim0F32Stream { inp: self }
    }
}

pub struct Dim1F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>>,
{
    inp: S,
}

impl<S> Stream for Dim1F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>> + Unpin,
{
    type Item = Result<ValuesDim1, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => {
                let mut ret = ValuesDim1 {
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
                        _ => todo!(),
                    }
                }
                Ready(Some(Ok(ret)))
            }
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
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
        Dim1F32Stream { inp: self }
    }
}

pub trait IntoBinnedXBins1<I: AggregatableXdim1Bin> {
    type StreamOut;
    fn into_binned_x_bins_1(self) -> Self::StreamOut
    where
        Self: Stream<Item = Result<I, Error>>;
}

impl<T, I: AggregatableXdim1Bin> IntoBinnedXBins1<I> for T
where
    T: Stream<Item = Result<I, Error>> + Unpin,
{
    type StreamOut = IntoBinnedXBins1DefaultStream<T, I>;

    fn into_binned_x_bins_1(self) -> Self::StreamOut {
        IntoBinnedXBins1DefaultStream { inp: self }
    }
}

pub struct IntoBinnedXBins1DefaultStream<S, I>
where
    S: Stream<Item = Result<I, Error>> + Unpin,
    I: AggregatableXdim1Bin,
{
    inp: S,
}

impl<S, I> Stream for IntoBinnedXBins1DefaultStream<S, I>
where
    S: Stream<Item = Result<I, Error>> + Unpin,
    I: AggregatableXdim1Bin,
{
    type Item = Result<I::Output, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => Ready(Some(Ok(k.into_agg()))),
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub trait IntoBinnedT {
    type StreamOut: Stream;
    fn into_binned_t(self, spec: BinSpecDimT) -> Self::StreamOut;
}

impl<T, I> IntoBinnedT for T
where
    I: AggregatableTdim + Unpin,
    T: Stream<Item = Result<I, Error>> + Unpin,
    I::Aggregator: Unpin,
{
    type StreamOut = IntoBinnedTDefaultStream<T, I>;

    fn into_binned_t(self, spec: BinSpecDimT) -> Self::StreamOut {
        IntoBinnedTDefaultStream::new(self, spec)
    }
}

pub struct IntoBinnedTDefaultStream<S, I>
where
    I: AggregatableTdim,
    S: Stream<Item = Result<I, Error>>,
{
    inp: S,
    aggtor: Option<I::Aggregator>,
    spec: BinSpecDimT,
    curbin: u32,
    left: Option<Poll<Option<Result<I, Error>>>>,
}

impl<S, I> IntoBinnedTDefaultStream<S, I>
where
    I: AggregatableTdim,
    S: Stream<Item = Result<I, Error>>,
{
    pub fn new(inp: S, spec: BinSpecDimT) -> Self {
        //info!("spec ts  {}   {}", spec.ts1, spec.ts2);
        Self {
            inp,
            aggtor: None,
            spec,
            curbin: 0,
            left: None,
        }
    }
}

impl<T, I> Stream for IntoBinnedTDefaultStream<T, I>
where
    I: AggregatableTdim + Unpin,
    T: Stream<Item = Result<I, Error>> + Unpin,
    I::Aggregator: Unpin,
{
    type Item = Result<<I::Aggregator as AggregatorTdim>::OutputValue, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            let cur = if self.curbin as u64 >= self.spec.count {
                Ready(None)
            } else if let Some(k) = self.left.take() {
                k
            } else {
                self.inp.poll_next_unpin(cx)
            };
            break match cur {
                Ready(Some(Ok(k))) => {
                    if self.aggtor.is_none() {
                        let range = self.spec.get_range(self.curbin);
                        //info!("range:   {}   {}", range.ts1, range.ts2);
                        self.aggtor = Some(k.aggregator_new(range.beg, range.end));
                    }
                    let ag = self.aggtor.as_mut().unwrap();
                    if ag.ends_before(&k) {
                        //info!("ENDS BEFORE");
                        continue 'outer;
                    } else if ag.starts_after(&k) {
                        //info!("STARTS AFTER");
                        self.left = Some(Ready(Some(Ok(k))));
                        self.curbin += 1;
                        Ready(Some(Ok(self.aggtor.take().unwrap().result())))
                    } else {
                        //info!("INGEST");
                        ag.ingest(&k);
                        // if this input contains also data after the current bin, then I need to keep
                        // it for the next round.
                        if ag.ends_after(&k) {
                            //info!("ENDS AFTER");
                            self.left = Some(Ready(Some(Ok(k))));
                            self.curbin += 1;
                            Ready(Some(Ok(self.aggtor.take().unwrap().result())))
                        } else {
                            //info!("ENDS WITHIN");
                            continue 'outer;
                        }
                    }
                }
                Ready(Some(Err(e))) => Ready(Some(Err(e))),
                Ready(None) => match self.aggtor.take() {
                    Some(ag) => Ready(Some(Ok(ag.result()))),
                    None => {
                        warn!("TODO add trailing bins");
                        Ready(None)
                    }
                },
                Pending => Pending,
            };
        }
    }
}
pub fn make_test_node(id: u32) -> Node {
    Node {
        id,
        host: "localhost".into(),
        port: 8800 + id as u16,
        port_raw: 8800 + id as u16 + 100,
        data_base_path: format!("../tmpdata/node{:02}", id).into(),
        split: id,
        ksprefix: "ks".into(),
    }
}
