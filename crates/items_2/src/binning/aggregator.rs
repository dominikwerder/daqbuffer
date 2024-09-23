use super::container_events::EventValueType;
use netpod::DtNano;

pub trait AggregatorTimeWeight<EVT>
where
    EVT: EventValueType,
{
    fn new() -> Self;
    fn reset_for_new_bin(&mut self);
    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: EVT);
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
    fn new() -> Self {
        todo!()
    }

    fn reset_for_new_bin(&mut self) {
        todo!()
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: EVT) {
        todo!()
    }
}

impl AggregatorTimeWeight<f32> for AggregatorNumeric<f32> {
    fn new() -> Self {
        Self {
            sum: f32::sum_identity(),
        }
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = f32::sum_identity();
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: f32) {
        let f = dt.ns() as f32 / bl.ns() as f32;
        eprintln!("INGEST  {}  {}", f, val);
        self.sum += f * val;
    }
}

impl AggregatorTimeWeight<u64> for AggregatorNumeric<u64> {
    fn new() -> Self {
        todo!()
    }

    fn reset_for_new_bin(&mut self) {
        todo!()
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: u64) {
        todo!()
    }
}

// TODO do enum right from begin, using a SOA enum container.
