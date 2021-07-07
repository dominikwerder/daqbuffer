use crate::SubFrId;
use num_traits::{AsPrimitive, Bounded, Float, Zero};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::Add;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct BoolNum(pub u8);

impl BoolNum {
    pub const MIN: Self = Self(0);
    pub const MAX: Self = Self(1);
}

impl Add<BoolNum> for BoolNum {
    type Output = BoolNum;

    fn add(self, rhs: BoolNum) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl num_traits::Zero for BoolNum {
    fn zero() -> Self {
        Self(0)
    }

    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl num_traits::AsPrimitive<f32> for BoolNum {
    fn as_(self) -> f32 {
        self.0 as f32
    }
}

impl num_traits::Bounded for BoolNum {
    fn min_value() -> Self {
        Self(0)
    }

    fn max_value() -> Self {
        Self(1)
    }
}

impl PartialEq for BoolNum {
    fn eq(&self, other: &Self) -> bool {
        PartialEq::eq(&self.0, &other.0)
    }
}

impl PartialOrd for BoolNum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        PartialOrd::partial_cmp(&self.0, &other.0)
    }
}

pub trait NumOps:
    Sized
    + Copy
    + Send
    + Unpin
    + Debug
    + Zero
    + AsPrimitive<f32>
    + Bounded
    + PartialOrd
    + SubFrId
    + Serialize
    + DeserializeOwned
{
    fn min_or_nan() -> Self;
    fn max_or_nan() -> Self;
    fn is_nan(&self) -> bool;
}

macro_rules! impl_num_ops {
    ($ty:ident, $min_or_nan:ident, $max_or_nan:ident, $is_nan:ident) => {
        impl NumOps for $ty {
            fn min_or_nan() -> Self {
                $ty::$min_or_nan
            }
            fn max_or_nan() -> Self {
                $ty::$max_or_nan
            }
            fn is_nan(&self) -> bool {
                $is_nan(self)
            }
        }
    };
}

fn is_nan_int<T>(_x: &T) -> bool {
    false
}

fn is_nan_float<T: Float>(x: &T) -> bool {
    x.is_nan()
}

impl_num_ops!(u8, MIN, MAX, is_nan_int);
impl_num_ops!(u16, MIN, MAX, is_nan_int);
impl_num_ops!(u32, MIN, MAX, is_nan_int);
impl_num_ops!(u64, MIN, MAX, is_nan_int);
impl_num_ops!(i8, MIN, MAX, is_nan_int);
impl_num_ops!(i16, MIN, MAX, is_nan_int);
impl_num_ops!(i32, MIN, MAX, is_nan_int);
impl_num_ops!(i64, MIN, MAX, is_nan_int);
impl_num_ops!(f32, NAN, NAN, is_nan_float);
impl_num_ops!(f64, NAN, NAN, is_nan_float);
impl_num_ops!(BoolNum, MIN, MAX, is_nan_int);
