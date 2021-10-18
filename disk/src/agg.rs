//! Aggregation and binning support.

use bytes::BytesMut;
use err::Error;
use netpod::ScalarType;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub mod binnedt;
pub mod enp;
pub mod scalarbinbatch;
pub mod streams;

#[derive(Debug, Serialize, Deserialize)]
pub struct ValuesExtractStats {
    pub dur: Duration,
}

impl ValuesExtractStats {
    pub fn new() -> Self {
        Self {
            dur: Duration::default(),
        }
    }
    pub fn trans(self: &mut Self, k: &mut Self) {
        self.dur += k.dur;
        k.dur = Duration::default();
    }
}

/// Batch of events with a numeric one-dimensional (i.e. array) value.
pub struct ValuesDim1 {
    pub tss: Vec<u64>,
    pub values: Vec<Vec<f32>>,
}

impl ValuesDim1 {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            values: vec![],
        }
    }
}

impl std::fmt::Debug for ValuesDim1 {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "count {}  tsA {:?}  tsB {:?}",
            self.tss.len(),
            self.tss.first(),
            self.tss.last()
        )
    }
}

trait NumEx {
    const BY: usize;
}

struct NumF32;
impl NumEx for NumF32 {
    const BY: usize = 4;
}

macro_rules! make_get_values {
    ($n:ident, $TY:ident, $FROM_BYTES:ident, $BY:expr) => {
        #[allow(unused)]
        fn $n(decomp: &BytesMut, ty: &ScalarType) -> Result<Vec<f32>, Error> {
            let n1 = decomp.len();
            if ty.bytes() as usize != $BY {
                Err(Error::with_msg(format!(
                    "ty.bytes() != BY  {} vs {}",
                    ty.bytes(),
                    $BY
                )))?;
            }
            if n1 % ty.bytes() as usize != 0 {
                Err(Error::with_msg(format!(
                    "n1 % ty.bytes() as usize != 0  {} vs {}",
                    n1,
                    ty.bytes()
                )))?;
            }
            let ele_count = n1 / ty.bytes() as usize;
            let mut j = Vec::with_capacity(ele_count);
            let mut p2 = j.as_mut_ptr();
            let mut p1 = 0;
            for _ in 0..ele_count {
                unsafe {
                    let mut r = [0u8; $BY];
                    std::ptr::copy_nonoverlapping(&decomp[p1], r.as_mut_ptr(), $BY);
                    *p2 = $TY::$FROM_BYTES(r) as f32;
                    p1 += $BY;
                    p2 = p2.add(1);
                };
            }
            unsafe {
                j.set_len(ele_count);
            }
            Ok(j)
        }
    };
}

make_get_values!(get_values_u8_le, u8, from_le_bytes, 1);
make_get_values!(get_values_u16_le, u16, from_le_bytes, 2);
make_get_values!(get_values_u32_le, u32, from_le_bytes, 4);
make_get_values!(get_values_u64_le, u64, from_le_bytes, 8);
make_get_values!(get_values_i8_le, i8, from_le_bytes, 1);
make_get_values!(get_values_i16_le, i16, from_le_bytes, 2);
make_get_values!(get_values_i32_le, i32, from_le_bytes, 4);
make_get_values!(get_values_i64_le, i64, from_le_bytes, 8);
make_get_values!(get_values_f32_le, f32, from_le_bytes, 4);
make_get_values!(get_values_f64_le, f64, from_le_bytes, 8);

make_get_values!(get_values_u8_be, u8, from_be_bytes, 1);
make_get_values!(get_values_u16_be, u16, from_be_bytes, 2);
make_get_values!(get_values_u32_be, u32, from_be_bytes, 4);
make_get_values!(get_values_u64_be, u64, from_be_bytes, 8);
make_get_values!(get_values_i8_be, i8, from_be_bytes, 1);
make_get_values!(get_values_i16_be, i16, from_be_bytes, 2);
make_get_values!(get_values_i32_be, i32, from_be_bytes, 4);
make_get_values!(get_values_i64_be, i64, from_be_bytes, 8);
make_get_values!(get_values_f32_be, f32, from_be_bytes, 4);
make_get_values!(get_values_f64_be, f64, from_be_bytes, 8);
