use crate::agg::streams::Appendable;
use crate::binned::{EventsNodeProcessor, NumOps, PushableIndex, RangeOverlapInfo, WithLen, WithTimestamps};
use crate::decode::EventValues;
use netpod::NanoRange;
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

impl<NTY> XBinnedScalarEvents<NTY> {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
            xbincount: vec![],
        }
    }
}

impl<NTY> WithLen for XBinnedScalarEvents<NTY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<NTY> WithTimestamps for XBinnedScalarEvents<NTY> {
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

impl<NTY> RangeOverlapInfo for XBinnedScalarEvents<NTY> {
    fn ends_before(&self, range: NanoRange) -> bool {
        match self.tss.last() {
            Some(&ts) => ts < range.beg,
            None => true,
        }
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        match self.tss.last() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        match self.tss.first() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }
}

impl<NTY> PushableIndex for XBinnedScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push(src.tss[ix]);
        self.xbincount.push(src.xbincount[ix]);
        self.mins.push(src.mins[ix]);
        self.maxs.push(src.maxs[ix]);
        self.avgs.push(src.avgs[ix]);
    }
}

impl<NTY> Appendable for XBinnedScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn empty() -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.tss.extend_from_slice(&src.tss);
        self.xbincount.extend_from_slice(&src.xbincount);
        self.mins.extend_from_slice(&src.mins);
        self.maxs.extend_from_slice(&src.maxs);
        self.avgs.extend_from_slice(&src.avgs);
    }
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

    fn process(inp: EventValues<Self::Input>) -> Self::Output {
        let nev = inp.tss.len();
        let mut ret = XBinnedScalarEvents {
            tss: inp.tss,
            xbincount: Vec::with_capacity(nev),
            mins: Vec::with_capacity(nev),
            maxs: Vec::with_capacity(nev),
            avgs: Vec::with_capacity(nev),
        };
        for i1 in 0..nev {
            let mut min = None;
            let mut max = None;
            let mut sum = 0f32;
            let mut count = 0;
            let vals = &inp.values[i1];
            for i2 in 0..vals.len() {
                let v = vals[i2];
                min = match min {
                    None => Some(v),
                    Some(min) => {
                        if v < min {
                            Some(v)
                        } else {
                            Some(min)
                        }
                    }
                };
                max = match max {
                    None => Some(v),
                    Some(max) => {
                        if v > max {
                            Some(v)
                        } else {
                            Some(max)
                        }
                    }
                };
                let vf = v.as_();
                if vf.is_nan() {
                } else {
                    sum += vf;
                    count += 1;
                }
            }
            // TODO while X-binning I expect values, otherwise it is illegal input.
            ret.xbincount.push(nev as u32);
            ret.mins.push(min.unwrap());
            ret.maxs.push(max.unwrap());
            if count == 0 {
                ret.avgs.push(f32::NAN);
            } else {
                ret.avgs.push(sum / count as f32);
            }
        }
        ret
    }
}
