#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use std::task::{Context, Poll};
use std::pin::Pin;
use crate::EventFull;
use futures_core::Stream;
use futures_util::{pin_mut, StreamExt, future::ready};
use netpod::ScalarType;

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


// dummy
impl AggregatableXdim1Bin for () {
    type Output = ();
    fn into_agg(self) -> Self::Output { todo!() }
}
impl AggregatableTdim for () {
    type Output = ();
    type Aggregator = ();
    fn aggregator_new(&self, ts1: u64, ts2: u64) -> Self::Aggregator {
        todo!()
    }
}
impl AggregatorTdim for () {
    type InputValue = ();
    type OutputValue = ();

    fn ends_before(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn ingest(&mut self, v: &Self::InputValue) { todo!() }
    fn result(self) -> Self::OutputValue { todo!() }
}


pub struct ValuesDim0 {
    tss: Vec<u64>,
    values: Vec<Vec<f32>>,
}

impl std::fmt::Debug for ValuesDim0 {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "count {}  tsA {:?}  tsB {:?}", self.tss.len(), self.tss.first(), self.tss.last())
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
            if min == f32::MAX { min = f32::NAN; }
            if max == f32::MIN { max = f32::NAN; }
            ret.tss.push(ts);
            ret.mins.push(min);
            ret.maxs.push(max);
            ret.avgs.push(sum / vals.len() as f32);
        }
        ret
    }

}


pub struct ValuesDim1 {
    tss: Vec<u64>,
    values: Vec<Vec<f32>>,
}

impl std::fmt::Debug for ValuesDim1 {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "count {}  tsA {:?}  tsB {:?}", self.tss.len(), self.tss.first(), self.tss.last())
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
            if min == f32::MAX { min = f32::NAN; }
            if max == f32::MIN { max = f32::NAN; }
            ret.tss.push(ts);
            ret.mins.push(min);
            ret.maxs.push(max);
            ret.avgs.push(sum / vals.len() as f32);
        }
        ret
    }

}


pub struct MinMaxAvgScalarEventBatch {
    tss: Vec<u64>,
    mins: Vec<f32>,
    maxs: Vec<f32>,
    avgs: Vec<f32>,
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
    min: f32,
    max: f32,
    sum: f32,
    count: u64,
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
            Some(ts) => {
                *ts < self.ts1
            }
            _ => panic!()
        }
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        match inp.tss.last() {
            Some(ts) => {
                *ts >= self.ts2
            }
            _ => panic!()
        }
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        match inp.tss.first() {
            Some(ts) => {
                *ts >= self.ts2
            }
            _ => panic!()
        }
    }

    fn ingest(&mut self, v: &Self::InputValue) {
        for i1 in 0..v.tss.len() {
            let ts = v.tss[i1];
            if ts < self.ts1 {
                //info!("EventBatchAgg  {}  {}  {}  {}   IS BEFORE", v.tss[i1], v.mins[i1], v.maxs[i1], v.avgs[i1]);
                continue;
            }
            else if ts >= self.ts2 {
                //info!("EventBatchAgg  {}  {}  {}  {}   IS AFTER", v.tss[i1], v.mins[i1], v.maxs[i1], v.avgs[i1]);
                continue;
            }
            else {
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
        let avg = if self.count == 0 { f32::NAN } else { self.sum / self.count as f32 };
        MinMaxAvgScalarBinSingle {
            ts1: self.ts1,
            ts2: self.ts2,
            min,
            max,
            avg,
        }
    }

}


pub struct MinMaxAvgScalarBinBatch {
    ts1s: Vec<u64>,
    ts2s: Vec<u64>,
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
    fn aggregator_new(&self, ts1: u64, ts2: u64) -> Self::Aggregator {
        todo!()
    }
}

pub struct MinMaxAvgScalarBinBatchAggregator {}

impl AggregatorTdim for MinMaxAvgScalarBinBatchAggregator {
    type InputValue = MinMaxAvgScalarBinBatch;
    type OutputValue = MinMaxAvgScalarBinSingle;

    fn ends_before(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }


    fn ingest(&mut self, v: &Self::InputValue) {
        todo!()
    }

    fn result(self) -> Self::OutputValue { todo!() }

}


pub struct MinMaxAvgScalarBinSingle {
    ts1: u64,
    ts2: u64,
    min: f32,
    max: f32,
    avg: f32,
}

impl std::fmt::Debug for MinMaxAvgScalarBinSingle {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "MinMaxAvgScalarBinSingle  ts1 {}  ts2 {}  min {:7.2e}  max {:7.2e}  avg {:7.2e}", self.ts1, self.ts2, self.min, self.max, self.avg)
    }
}

impl AggregatableTdim for MinMaxAvgScalarBinSingle {
    type Output = MinMaxAvgScalarBinSingle;
    type Aggregator = MinMaxAvgScalarBinSingleAggregator;
    fn aggregator_new(&self, ts1: u64, ts2: u64) -> Self::Aggregator {
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

    fn ends_before(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn ends_after(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }

    fn starts_after(&self, inp: &Self::InputValue) -> bool {
        todo!()
    }


    fn ingest(&mut self, v: &Self::InputValue) {
        todo!()
    }

    fn result(self) -> Self::OutputValue { todo!() }

}




pub struct Dim0F32Stream<S> where S: Stream<Item=Result<EventFull, Error>> {
    inp: S,
}

impl<S> Stream for Dim0F32Stream<S> where S: Stream<Item=Result<EventFull, Error>> + Unpin {
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
                            todo!();

                            let n1 = decomp.len();
                            assert!(n1 % ty.bytes() as usize == 0);
                            let ele_count = n1 / ty.bytes() as usize;
                            let mut j = Vec::with_capacity(ele_count);
                            // this is safe for ints and floats
                            unsafe { j.set_len(ele_count); }
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
                        },
                        _ => todo!()
                    }
                }
                Ready(Some(Ok(todo!())))
            }
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }

}

pub trait IntoDim0F32Stream {
    fn into_dim_0_f32_stream(self) -> Dim0F32Stream<Self> where Self: Stream<Item=Result<EventFull, Error>> + Sized;
}

impl<T> IntoDim0F32Stream for T where T: Stream<Item=Result<EventFull, Error>> {

    fn into_dim_0_f32_stream(self) -> Dim0F32Stream<T> {
        Dim0F32Stream {
            inp: self,
        }
    }

}




pub struct Dim1F32Stream<S> where S: Stream<Item=Result<EventFull, Error>> {
    inp: S,
}

impl<S> Stream for Dim1F32Stream<S> where S: Stream<Item=Result<EventFull, Error>> + Unpin {
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
                            unsafe { j.set_len(ele_count); }
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
                        },
                        _ => todo!()
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
    fn into_dim_1_f32_stream(self) -> Dim1F32Stream<Self> where Self: Stream<Item=Result<EventFull, Error>> + Sized;
}

impl<T> IntoDim1F32Stream for T where T: Stream<Item=Result<EventFull, Error>> {

    fn into_dim_1_f32_stream(self) -> Dim1F32Stream<T> {
        Dim1F32Stream {
            inp: self,
        }
    }

}


pub trait IntoBinnedXBins1<I: AggregatableXdim1Bin> {
    type StreamOut;
    fn into_binned_x_bins_1(self) -> Self::StreamOut where Self: Stream<Item=Result<I, Error>>;
}

impl<T, I: AggregatableXdim1Bin> IntoBinnedXBins1<I> for T where T: Stream<Item=Result<I, Error>> + Unpin {
    type StreamOut = IntoBinnedXBins1DefaultStream<T, I>;

    fn into_binned_x_bins_1(self) -> Self::StreamOut {
        IntoBinnedXBins1DefaultStream {
            inp: self,
        }
    }

}

pub struct IntoBinnedXBins1DefaultStream<S, I> where S: Stream<Item=Result<I, Error>> + Unpin, I: AggregatableXdim1Bin {
    inp: S,
}

impl<S, I> Stream for IntoBinnedXBins1DefaultStream<S, I> where S: Stream<Item=Result<I, Error>> + Unpin, I: AggregatableXdim1Bin {
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

impl<T, I> IntoBinnedT for T where I: AggregatableTdim + Unpin, T: Stream<Item=Result<I, Error>> + Unpin, I::Aggregator: Unpin {
    type StreamOut = IntoBinnedTDefaultStream<T, I>;

    fn into_binned_t(self, spec: BinSpecDimT) -> Self::StreamOut {
        IntoBinnedTDefaultStream::new(self, spec)
    }

}

pub struct IntoBinnedTDefaultStream<S, I> where I: AggregatableTdim, S: Stream<Item=Result<I, Error>> {
    inp: S,
    aggtor: Option<I::Aggregator>,
    spec: BinSpecDimT,
    curbin: u32,
    left: Option<Poll<Option<Result<I, Error>>>>,
}

impl<S, I> IntoBinnedTDefaultStream<S, I> where I: AggregatableTdim, S: Stream<Item=Result<I, Error>> {

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
where I: AggregatableTdim + Unpin, T: Stream<Item=Result<I, Error>> + Unpin, I::Aggregator: Unpin
{
    type Item = Result<<I::Aggregator as AggregatorTdim>::OutputValue, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            let cur = if self.curbin as u64 >= self.spec.count {
                Ready(None)
            }
            else if let Some(k) = self.left.take() {
                k
            }
            else {
                self.inp.poll_next_unpin(cx)
            };
            break match cur {
                Ready(Some(Ok(k))) => {
                    if self.aggtor.is_none() {
                        let range = self.spec.get_range(self.curbin);
                        //info!("range:   {}   {}", range.ts1, range.ts2);
                        self.aggtor = Some(k.aggregator_new(range.ts1, range.ts2));
                    }
                    let ag = self.aggtor.as_mut().unwrap();
                    if ag.ends_before(&k) {
                        //info!("ENDS BEFORE");
                        continue 'outer;
                    }
                    else if ag.starts_after(&k) {
                        //info!("STARTS AFTER");
                        self.left = Some(Ready(Some(Ok(k))));
                        self.curbin += 1;
                        Ready(Some(Ok(self.aggtor.take().unwrap().result())))
                    }
                    else {
                        //info!("INGEST");
                        ag.ingest(&k);
                        // if this input contains also data after the current bin, then I need to keep
                        // it for the next round.
                        if ag.ends_after(&k) {
                            //info!("ENDS AFTER");
                            self.left = Some(Ready(Some(Ok(k))));
                            self.curbin += 1;
                            Ready(Some(Ok(self.aggtor.take().unwrap().result())))
                        }
                        else {
                            //info!("ENDS WITHIN");
                            continue 'outer;
                        }
                    }
                }
                Ready(Some(Err(e))) => Ready(Some(Err(e))),
                Ready(None) => {
                    match self.aggtor.take() {
                        Some(ag) => {
                            Ready(Some(Ok(ag.result())))
                        }
                        None => {
                            warn!("TODO add trailing bins");
                            Ready(None)
                        }
                    }
                },
                Pending => Pending,
            };
        }
    }

}

pub struct BinSpecDimT {
    count: u64,
    ts1: u64,
    ts2: u64,
    bs: u64,
}

impl BinSpecDimT {

    pub fn over_range(count: u64, ts1: u64, ts2: u64) -> Self {
        assert!(count >= 1);
        assert!(count <= 2000);
        assert!(ts2 > ts1);
        let dt = ts2 - ts1;
        assert!(dt <= DAY * 14);
        let bs = dt / count;
        let thresholds = [
            2, 10, 100,
            1000, 10_000, 100_000,
            MU, MU * 10, MU * 100,
            MS, MS * 10, MS * 100,
            SEC, SEC * 5, SEC * 10, SEC * 20,
            MIN, MIN * 5, MIN * 10, MIN * 20,
            HOUR, HOUR * 2, HOUR * 4, HOUR * 12,
            DAY, DAY * 2, DAY * 4, DAY * 8, DAY * 16,
            WEEK, WEEK * 2, WEEK * 10, WEEK * 60,
        ];
        let mut i1 = 0;
        let bs = loop {
            if i1 >= thresholds.len() { break *thresholds.last().unwrap(); }
            let t = thresholds[i1];
            if bs < t { break t; }
            i1 += 1;
        };
        //info!("INPUT TS  {}  {}", ts1, ts2);
        //info!("chosen binsize: {}  {}", i1, bs);
        let ts1 = ts1 / bs * bs;
        let ts2 = (ts2 + bs - 1) / bs * bs;
        //info!("ADJUSTED TS  {}  {}", ts1, ts2);
        BinSpecDimT {
            count,
            ts1,
            ts2,
            bs,
        }
    }

    pub fn get_range(&self, ix: u32) -> TimeRange {
        TimeRange {
            ts1: self.ts1 + ix as u64 * self.bs,
            ts2: self.ts1 + (ix as u64 + 1) * self.bs,
        }
    }

}

pub struct TimeRange {
    ts1: u64,
    ts2: u64,
}

const MU: u64 = 1000;
const MS: u64 = MU * 1000;
const SEC: u64 = MS * 1000;
const MIN: u64 = SEC * 60;
const HOUR: u64 = MIN * 60;
const DAY: u64 = HOUR * 24;
const WEEK: u64 = DAY * 7;




#[test]
fn agg_x_dim_0() {
    taskrun::run(async { agg_x_dim_0_inner().await; Ok(()) }).unwrap();
}

async fn agg_x_dim_0_inner() {
    let query = netpod::AggQuerySingleChannel {
        ksprefix: "daq_swissfel".into(),
        keyspace: 2,
        channel: netpod::Channel {
            name: "S10BC01-DBAM070:EOM1_T1".into(),
            backend: "sf-databuffer".into(),
        },
        timebin: 18723,
        tb_file_count: 1,
        split: 12,
        tbsize: 1000 * 60 * 60 * 24,
        buffer_size: 1024 * 4,
    };
    let bin_count = 20;
    let ts1 = query.timebin as u64 * query.tbsize as u64 * MS;
    let ts2 = ts1 + HOUR * 24;
    let fut1 = crate::EventBlobsComplete::new(&query)
    .into_dim_1_f32_stream()
    //.take(1000)
    .map(|q| {
        if let Ok(ref k) = q {
            //info!("vals: {:?}", k);
        }
        q
    })
    .into_binned_x_bins_1()
    .map(|k| {
        //info!("after X binning  {:?}", k.as_ref().unwrap());
        k
    })
    .into_binned_t(BinSpecDimT::over_range(bin_count, ts1, ts2))
    .map(|k| {
        info!("after T binning  {:?}", k.as_ref().unwrap());
        k
    })
    .for_each(|k| ready(()));
    fut1.await;
}


#[test]
fn agg_x_dim_1() {
    taskrun::run(async { agg_x_dim_1_inner().await; Ok(()) }).unwrap();
}

async fn agg_x_dim_1_inner() {
    let vals = ValuesDim1 {
        tss: vec![0, 1, 2, 3],
        values: vec![
            vec![0., 0., 0.],
            vec![1., 1., 1.],
            vec![2., 2., 2.],
            vec![3., 3., 3.],
        ],
    };
    // I want to distinguish already in the outer part between dim-0 and dim-1 and generate
    // separate code for these cases...
    // That means that also the reading chain itself needs to be typed on that.
    // Need to supply some event-payload converter type which has that type as Output type.
    let vals2 = vals.into_agg();
    // Now the T-binning:

    /*
    T-aggregator must be able to produce empty-values of correct type even if we never get
    a single value of input data.
    Therefore, it needs the bin range definition.
    How do I want to drive the system?
    If I write the T-binner as a Stream, then I also need to pass it the input!
    Meaning, I need to pass the Stream which produces the actual numbers from disk.

    readchannel()  -> Stream of timestamped byte blobs
    .to_f32()  -> Stream ?    indirection to branch on the underlying shape
    .agg_x_bins_1()  -> Stream ?    can I keep it at the single indirection on the top level?
    */
    let query = netpod::AggQuerySingleChannel {
        ksprefix: "daq_swissfel".into(),
        keyspace: 3,
        channel: netpod::Channel {
            name: "S10BC01-DBAM070:BAM_CH1_NORM".into(),
            backend: "sf-databuffer".into(),
        },
        timebin: 18722,
        tb_file_count: 1,
        split: 12,
        tbsize: 1000 * 60 * 60 * 24,
        buffer_size: 1024 * 4,
    };
    let bin_count = 100;
    let ts1 = query.timebin as u64 * query.tbsize as u64 * MS;
    let ts2 = ts1 + HOUR * 24;
    let fut1 = crate::EventBlobsComplete::new(&query)
    .into_dim_1_f32_stream()
    //.take(1000)
    .map(|q| {
        if let Ok(ref k) = q {
            //info!("vals: {:?}", k);
        }
        q
    })
    .into_binned_x_bins_1()
    .map(|k| {
        //info!("after X binning  {:?}", k.as_ref().unwrap());
        k
    })
    .into_binned_t(BinSpecDimT::over_range(bin_count, ts1, ts2))
    .map(|k| {
        info!("after T binning  {:?}", k.as_ref().unwrap());
        k
    })
    .for_each(|k| ready(()));
    fut1.await;
}
