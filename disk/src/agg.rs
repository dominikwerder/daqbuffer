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

impl AggregatableXdim1Bin for ValuesDim1 {
    type Output = MinMaxAvgScalarEventBatch;
    fn into_agg(self) -> Self::Output {
        todo!()
    }
}


pub struct MinMaxAvgScalarEventBatch {
    ts1s: Vec<u64>,
    ts2s: Vec<u64>,
    mins: Vec<f32>,
    maxs: Vec<f32>,
    avgs: Vec<f32>,
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
        todo!()
    }
}

pub struct MinMaxAvgScalarEventBatchAggregator {}

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

#[test]
fn agg_x_dim_1() {
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
    */
}
