use super::binnedvaluetype::BinnedNumericValue;
use super::binnedvaluetype::BinnedValueType;
use super::container_events::EventValueType;
use netpod::DtNano;

pub trait AggregatorTimeWeight<EVT>
where
    EVT: EventValueType,
{
    type OutputAvg;

    fn new() -> Self;
    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: EVT);
    fn reset_for_new_bin(&mut self);
    fn result_and_reset_for_new_bin(&mut self) -> Self::OutputAvg;
}

pub struct AggregatorNumeric<EVT> {
    sum: EVT,
}

trait AggWithSame: EventValueType {}

impl AggWithSame for f64 {}

impl<EVT> AggregatorTimeWeight<EVT> for AggregatorNumeric<EVT>
where
    EVT: AggWithSame,
{
    type OutputAvg = EVT;

    fn new() -> Self {
        Self {
            sum: EVT::identity_sum(),
        }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: EVT) {
        let f = dt.ns() as f32 / bl.ns() as f32;
        eprintln!("INGEST  {}  {:?}", f, val);
        self.sum.add_weighted(&val, f);
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = EVT::identity_sum();
    }

    fn result_and_reset_for_new_bin(&mut self) -> Self::OutputAvg {
        let ret = self.sum.clone();
        self.sum = EVT::identity_sum();
        ret
    }
}

impl AggregatorTimeWeight<f32> for AggregatorNumeric<f32> {
    type OutputAvg = f32;

    fn new() -> Self {
        Self {
            sum: f32::identity_sum(),
        }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: f32) {
        let f = dt.ns() as f32 / bl.ns() as f32;
        eprintln!("INGEST  {}  {}", f, val);
        self.sum += f * val;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = f32::identity_sum();
    }

    fn result_and_reset_for_new_bin(&mut self) -> Self::OutputAvg {
        let ret = self.sum.clone();
        self.sum = f32::identity_sum();
        ret
    }
}

impl AggregatorTimeWeight<u64> for AggregatorNumeric<u64> {
    type OutputAvg = u64;

    fn new() -> Self {
        todo!()
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: u64) {
        todo!()
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = u64::identity_sum();
    }

    fn result_and_reset_for_new_bin(&mut self) -> Self::OutputAvg {
        let ret = self.sum.clone();
        self.sum = u64::identity_sum();
        ret
    }
}

// TODO do enum right from begin, using a SOA enum container.
