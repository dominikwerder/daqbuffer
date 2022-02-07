use items::scalarevents::ScalarEvents;
use items::waveevents::WaveEvents;
use items::EventsNodeProcessor;
use items::{numops::NumOps, statsevents::StatsEvents};
use netpod::{AggKind, Shape};
use std::marker::PhantomData;

pub struct Identity<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for Identity<NTY>
where
    NTY: NumOps,
{
    type Input = ScalarEvents<NTY>;
    type Output = ScalarEvents<NTY>;

    fn create(_shape: Shape, _agg_kind: AggKind) -> Self {
        Self { _m1: PhantomData }
    }

    fn process(&self, inp: Self::Input) -> Self::Output {
        inp
    }
}

pub struct Stats1Scalar {}

impl EventsNodeProcessor for Stats1Scalar {
    type Input = StatsEvents;
    type Output = StatsEvents;

    fn create(_shape: Shape, _agg_kind: AggKind) -> Self {
        Self {}
    }

    fn process(&self, inp: Self::Input) -> Self::Output {
        inp
    }
}

pub struct Stats1Wave<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for Stats1Wave<NTY>
where
    NTY: NumOps,
{
    type Input = WaveEvents<NTY>;
    type Output = StatsEvents;

    fn create(_shape: Shape, _agg_kind: AggKind) -> Self {
        Self { _m1: PhantomData }
    }

    fn process(&self, _inp: Self::Input) -> Self::Output {
        err::todoval()
    }
}
