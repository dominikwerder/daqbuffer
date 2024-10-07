use super::container_events::EventValueType;
use core::fmt;
use netpod::log::*;
use netpod::DtNano;
use serde::Deserialize;
use serde::Serialize;

pub trait AggTimeWeightOutputAvg: fmt::Debug + Clone + Send + Serialize + for<'a> Deserialize<'a> {}

impl AggTimeWeightOutputAvg for u64 {}

impl AggTimeWeightOutputAvg for f32 {}

impl AggTimeWeightOutputAvg for f64 {}

pub trait AggregatorTimeWeight<EVT>: fmt::Debug + Send
where
    EVT: EventValueType,
{
    fn new() -> Self;
    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: EVT);
    fn reset_for_new_bin(&mut self);
    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> EVT::AggTimeWeightOutputAvg;
}

#[derive(Debug)]
pub struct AggregatorNumeric {
    sum: f64,
}

trait AggWithF64: EventValueType<AggTimeWeightOutputAvg = f64> {
    fn as_f64(&self) -> f64;
}

impl AggWithF64 for f64 {
    fn as_f64(&self) -> f64 {
        *self
    }
}

impl<EVT> AggregatorTimeWeight<EVT> for AggregatorNumeric
where
    EVT: AggWithF64,
{
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: EVT) {
        let f = dt.ns() as f64 / bl.ns() as f64;
        trace!("INGEST  {}  {:?}", f, val);
        self.sum += f * val.as_f64();
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> EVT::AggTimeWeightOutputAvg {
        let sum = self.sum.clone();
        trace!("result_and_reset_for_new_bin  sum {}  {}", sum, filled_width_fraction);
        self.sum = 0.;
        sum / filled_width_fraction as f64
    }
}

impl AggregatorTimeWeight<f32> for AggregatorNumeric {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: f32) {
        let f = dt.ns() as f64 / bl.ns() as f64;
        trace!("INGEST  {}  {}", f, val);
        self.sum += f * val as f64;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> f32 {
        let sum = self.sum.clone() as f32;
        trace!("result_and_reset_for_new_bin  sum {}  {}", sum, filled_width_fraction);
        self.sum = 0.;
        sum / filled_width_fraction
    }
}

impl AggregatorTimeWeight<u64> for AggregatorNumeric {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: u64) {
        let f = dt.ns() as f64 / bl.ns() as f64;
        trace!("INGEST  {}  {}", f, val);
        self.sum += f * val as f64;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> f64 {
        let sum = self.sum.clone();
        trace!("result_and_reset_for_new_bin  sum {}  {}", sum, filled_width_fraction);
        self.sum = 0.;
        sum / filled_width_fraction as f64
    }
}

// TODO do enum right from begin, using a SOA enum container.
