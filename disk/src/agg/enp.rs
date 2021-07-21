use items::eventvalues::EventValues;
use items::numops::NumOps;
use items::EventsNodeProcessor;
use netpod::{AggKind, Shape};
use std::marker::PhantomData;

pub struct Identity<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for Identity<NTY>
where
    NTY: NumOps,
{
    type Input = EventValues<NTY>;
    type Output = EventValues<NTY>;

    fn create(_shape: Shape, _agg_kind: AggKind) -> Self {
        Self { _m1: PhantomData }
    }

    fn process(&self, inp: Self::Input) -> Self::Output {
        inp
    }
}
