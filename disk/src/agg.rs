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
    type OutputValue: AggregatableXdim1Bin + AggregatableTdim;
}

pub trait AggregatableXdim1Bin {
    type Output: AggregatableXdim1Bin + AggregatableTdim;
    fn into_agg(self) -> Self::Output;
}

pub trait AggregatableTdim {
    type Output: AggregatableXdim1Bin + AggregatableTdim;
    type Aggregator: AggregatorTdim;
    fn aggregator_new(&self) -> Self::Aggregator;
}


// dummy
impl AggregatableXdim1Bin for () {
    type Output = ();
    fn into_agg(self) -> Self::Output { todo!() }
}
impl AggregatableTdim for () {
    type Output = ();
    type Aggregator = ();
    fn aggregator_new(&self) -> Self::Aggregator {
        todo!()
    }
}
impl AggregatorTdim for () {
    type OutputValue = ();
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
            for i2 in 0..vals.len() {
                let v = vals[i2];
                min = min.min(v);
                max = max.max(v);
                sum += v;
            }
            ret.tss.push(ts);
            ret.mins.push(min);
            ret.maxs.push(max);
            ret.avgs.push(sum / ret.tss.len() as f32);
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

    fn aggregator_new(&self) -> Self::Aggregator {
        MinMaxAvgScalarEventBatchAggregator::new()
    }

}

pub struct MinMaxAvgScalarEventBatchAggregator {
}

impl MinMaxAvgScalarEventBatchAggregator {

    pub fn new() -> Self {
        Self {
        }
    }

}

impl AggregatorTdim for MinMaxAvgScalarEventBatchAggregator {
    type OutputValue = MinMaxAvgScalarBinBatch;
}


pub struct MinMaxAvgScalarBinBatch {
    ts1s: Vec<u64>,
    ts2s: Vec<u64>,
    mins: Vec<f32>,
    maxs: Vec<f32>,
    avgs: Vec<f32>,
}

impl AggregatableXdim1Bin for MinMaxAvgScalarBinBatch {
    type Output = MinMaxAvgScalarBinBatch;
    fn into_agg(self) -> Self::Output {
        todo!()
    }
}

impl AggregatableTdim for MinMaxAvgScalarBinBatch {
    type Output = MinMaxAvgScalarBinSingle;
    type Aggregator = MinMaxAvgScalarBinSingle;
    fn aggregator_new(&self) -> Self::Aggregator {
        todo!()
    }
}

pub struct MinMaxAvgScalarBinSingle {
    ts1: u64,
    ts2: u64,
    min: f32,
    max: f32,
    avg: f32,
}

impl AggregatableTdim for MinMaxAvgScalarBinSingle {
    type Output = MinMaxAvgScalarBinSingle;
    type Aggregator = MinMaxAvgScalarBinSingle;
    fn aggregator_new(&self) -> Self::Aggregator {
        todo!()
    }
}

impl AggregatorTdim for MinMaxAvgScalarBinSingle {
    type OutputValue = ();
}

impl AggregatableXdim1Bin for MinMaxAvgScalarBinSingle {
    type Output = MinMaxAvgScalarBinSingle;
    fn into_agg(self) -> Self::Output {
        self
    }
}




pub struct Dim1F32Stream<S>
where S: Stream<Item=Result<EventFull, Error>>
{
    inp: S,
}

impl<S> Stream for Dim1F32Stream<S>
where S: Stream<Item=Result<EventFull, Error>> + Unpin
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
                            // do the conversion
                            let n1 = decomp.len();
                            assert!(n1 % ty.bytes() as usize == 0);
                            let ele_count = n1 / ty.bytes() as usize;
                            let mut j = Vec::with_capacity(ele_count);
                            // this is safe for ints and floats
                            unsafe { j.set_len(ele_count); }
                            let mut p1 = 0;
                            for i1 in 0..ele_count {
                                unsafe {
                                    j[i1] = std::mem::transmute_copy::<_, f64>(&decomp[p1]) as f32;
                                    p1 += 8;
                                }
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
    fn into_dim_1_f32_stream(self) -> Dim1F32Stream<Self>
        where Self: Sized,
              Self: Stream<Item=Result<EventFull, Error>>;
}

impl<T> IntoDim1F32Stream for T
    where T: Stream<Item=Result<EventFull, Error>>
{

    fn into_dim_1_f32_stream(self) -> Dim1F32Stream<T>
    {
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
    fn into_binned_t(self) -> Self::StreamOut;
}

impl<T, I> IntoBinnedT for T where I: AggregatableTdim, T: Stream<Item=Result<I, Error>> + Unpin {
    //type Bla = <<I as AggregatableTdim>::Aggregator as AggregatorTdim>::OutputValue;
    type StreamOut = IntoBinnedTDefaultStream<T, I>;

    fn into_binned_t(self) -> Self::StreamOut {
        IntoBinnedTDefaultStream::new(self)
    }

}

pub struct IntoBinnedTDefaultStream<S, I> where I: AggregatableTdim, S: Stream<Item=Result<I, Error>> {
    inp: S,
}

impl<S, I> IntoBinnedTDefaultStream<S, I> where I: AggregatableTdim, S: Stream<Item=Result<I, Error>> {

    pub fn new(inp: S) -> Self {
        Self {
            inp: inp,
        }
    }

}

impl<T, I> Stream for IntoBinnedTDefaultStream<T, I> where I: AggregatableTdim, T: Stream<Item=Result<I, Error>> + Unpin {
    type Item = Result<MinMaxAvgScalarBinSingle, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => {
                todo!()
            }
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }

}






#[test]
fn agg_x_dim_1() {
    crate::run(async { agg_x_dim_1_inner().await; Ok(()) }).unwrap();
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
        timebin: 18721,
        tb_file_count: 1,
        split: 12,
        tbsize: 1000 * 60 * 60 * 24,
        buffer_size: 1024 * 4,
    };
    let fut1 = crate::EventBlobsComplete::new(&query)
    .into_dim_1_f32_stream()
    .take(10)
    .map(|q| {
        if let Ok(ref k) = q {
            info!("vals: {:?}", k);
        }
        q
    })
    .into_binned_x_bins_1()
    .map(|k| {
        info!("after X binning  {:?}", k.as_ref().unwrap());
        k
    })
    .into_binned_t()
    .for_each(|k| ready(()));
    fut1.await;
}
