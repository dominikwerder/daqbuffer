use crate::agg::streams::{Appendable, StreamItem};
use crate::binned::{MakeBytesFrame, RangeCompletableItem, RangeOverlapInfo};
use crate::frame::makeframe::make_frame;
use bytes::{BufMut, Bytes, BytesMut};
use err::Error;
use netpod::log::*;
use netpod::NanoRange;
use serde::{Deserialize, Serialize};
use std::mem::size_of;

#[derive(Serialize, Deserialize)]
pub struct MinMaxAvgScalarEventBatch {
    pub tss: Vec<u64>,
    pub mins: Vec<f32>,
    pub maxs: Vec<f32>,
    pub avgs: Vec<f32>,
}

impl MinMaxAvgScalarEventBatch {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
        }
    }

    #[allow(dead_code)]
    pub fn old_from_full_frame(buf: &Bytes) -> Self {
        info!("construct MinMaxAvgScalarEventBatch from full frame  len {}", buf.len());
        assert!(buf.len() >= 4);
        let mut g = MinMaxAvgScalarEventBatch::empty();
        let n1;
        unsafe {
            let ptr = (&buf[0] as *const u8) as *const [u8; 4];
            n1 = u32::from_le_bytes(*ptr);
            trace!("--- +++ --- +++ --- +++  n1: {}", n1);
        }
        if n1 == 0 {
            g
        } else {
            let n2 = n1 as usize;
            g.tss.reserve(n2);
            g.mins.reserve(n2);
            g.maxs.reserve(n2);
            g.avgs.reserve(n2);
            unsafe {
                // TODO Can I unsafely create ptrs and just assign them?
                // TODO What are cases where I really need transmute?
                g.tss.set_len(n2);
                g.mins.set_len(n2);
                g.maxs.set_len(n2);
                g.avgs.set_len(n2);
                let ptr0 = &buf[4] as *const u8;
                {
                    let ptr1 = ptr0 as *const u64;
                    for i1 in 0..n2 {
                        g.tss[i1] = *ptr1.add(i1);
                    }
                }
                {
                    let ptr1 = ptr0.add((8) * n2) as *const f32;
                    for i1 in 0..n2 {
                        g.mins[i1] = *ptr1.add(i1);
                    }
                }
                {
                    let ptr1 = ptr0.add((8 + 4) * n2) as *const f32;
                    for i1 in 0..n2 {
                        g.maxs[i1] = *ptr1;
                    }
                }
                {
                    let ptr1 = ptr0.add((8 + 4 + 4) * n2) as *const f32;
                    for i1 in 0..n2 {
                        g.avgs[i1] = *ptr1;
                    }
                }
            }
            info!("CONTENT  {:?}", g);
            g
        }
    }
}

impl std::fmt::Debug for MinMaxAvgScalarEventBatch {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "MinMaxAvgScalarEventBatch  count {}  tss {:?}  mins {:?}  maxs {:?}  avgs {:?}",
            self.tss.len(),
            self.tss,
            self.mins,
            self.maxs,
            self.avgs,
        )
    }
}

impl MinMaxAvgScalarEventBatch {
    #[allow(dead_code)]
    fn old_serialized(&self) -> Bytes {
        let n1 = self.tss.len();
        let mut g = BytesMut::with_capacity(4 + n1 * (8 + 3 * 4));
        g.put_u32_le(n1 as u32);
        if n1 > 0 {
            let ptr = &self.tss[0] as *const u64 as *const u8;
            let a = unsafe { std::slice::from_raw_parts(ptr, size_of::<u64>() * n1) };
            g.put(a);
            let ptr = &self.mins[0] as *const f32 as *const u8;
            let a = unsafe { std::slice::from_raw_parts(ptr, size_of::<f32>() * n1) };
            g.put(a);
            let ptr = &self.maxs[0] as *const f32 as *const u8;
            let a = unsafe { std::slice::from_raw_parts(ptr, size_of::<f32>() * n1) };
            g.put(a);
            let ptr = &self.avgs[0] as *const f32 as *const u8;
            let a = unsafe { std::slice::from_raw_parts(ptr, size_of::<f32>() * n1) };
            g.put(a);
        }
        info!("impl Frameable for MinMaxAvgScalarEventBatch  g.len() {}", g.len());
        g.freeze()
    }
}

impl MakeBytesFrame for Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarEventBatch>>, Error> {
    fn make_bytes_frame(&self) -> Result<Bytes, Error> {
        Ok(make_frame(self)?.freeze())
    }
}

impl RangeOverlapInfo for MinMaxAvgScalarEventBatch {
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

impl Appendable for MinMaxAvgScalarEventBatch {
    fn empty() -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.tss.extend_from_slice(&src.tss);
        self.mins.extend_from_slice(&src.mins);
        self.maxs.extend_from_slice(&src.maxs);
        self.avgs.extend_from_slice(&src.avgs);
    }
}
