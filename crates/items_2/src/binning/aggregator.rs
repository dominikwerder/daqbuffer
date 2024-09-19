use std::marker::PhantomData;

pub trait AggregatorTimeWeight {}

pub struct AggregatorNumeric<T> {
    _t0: PhantomData<T>,
}

trait AggWithSame {}

impl AggWithSame for f64 {}

impl<T> AggregatorTimeWeight for AggregatorNumeric<T> where T: AggWithSame {}

impl AggregatorTimeWeight for AggregatorNumeric<f32> {}

impl AggregatorTimeWeight for AggregatorNumeric<u64> {}

// TODO do enum right from begin, using a SOA enum container.
