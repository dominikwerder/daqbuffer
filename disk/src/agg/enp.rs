use crate::binned::{EventsNodeProcessor, NumOps};
use crate::decode::EventValues;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

pub struct Identity<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for Identity<NTY>
where
    NTY: NumOps,
{
    type Input = NTY;
    type Output = EventValues<NTY>;

    fn process(inp: EventValues<Self::Input>) -> Self::Output {
        todo!()
    }
}

#[derive(Serialize, Deserialize)]
pub struct XBinnedScalarEvents<NTY> {
    tss: Vec<u64>,
    mins: Vec<NTY>,
    maxs: Vec<NTY>,
    avgs: Vec<f32>,
    xbincount: Vec<u32>,
}

pub struct WaveXBinner<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for WaveXBinner<NTY>
where
    NTY: NumOps,
{
    type Input = Vec<NTY>;
    type Output = XBinnedScalarEvents<NTY>;

    fn process(_inp: EventValues<Self::Input>) -> Self::Output {
        todo!()
    }
}
