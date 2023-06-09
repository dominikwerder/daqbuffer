use items_0::subfr::SubFrId;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StringNum(pub String);

impl StringNum {
    pub const MIN: Self = Self(String::new());
    pub const MAX: Self = Self(String::new());
}

impl Add<StringNum> for StringNum {
    type Output = StringNum;

    fn add(self, _rhs: StringNum) -> Self::Output {
        todo!()
    }
}

impl num_traits::Zero for StringNum {
    fn zero() -> Self {
        Self(String::new())
    }

    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

impl num_traits::Bounded for StringNum {
    fn min_value() -> Self {
        Self(String::new())
    }

    fn max_value() -> Self {
        Self(String::new())
    }
}

impl PartialEq for StringNum {
    fn eq(&self, other: &Self) -> bool {
        PartialEq::eq(&self.0, &other.0)
    }
}

impl PartialOrd for StringNum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        PartialOrd::partial_cmp(&self.0, &other.0)
    }
}

pub trait NumOps:
    Sized
    + Clone
    + AsPrimF32
    + Send
    + Sync
    + 'static
    + Unpin
    + Debug
    //+ Zero
    //+ Bounded
    + PartialOrd
    + SubFrId
    + Serialize
    + DeserializeOwned
    + items_0::scalar_ops::ScalarOps
{
    fn min_or_nan() -> Self;
    fn max_or_nan() -> Self;
    fn is_nan(&self) -> bool;
    fn zero() -> Self;
}

macro_rules! impl_num_ops {
    ($ty:ident, $min_or_nan:ident, $max_or_nan:ident, $is_nan:ident, $zero:expr) => {
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
            fn zero() -> Self {
                $zero
            }
        }
    };
}

impl AsPrimF32 for bool {
    fn as_prim_f32(&self) -> f32 {
        if *self {
            1.
        } else {
            0.
        }
    }
}

impl NumOps for bool {
    fn min_or_nan() -> Self {
        todo!()
    }

    fn max_or_nan() -> Self {
        todo!()
    }

    fn is_nan(&self) -> bool {
        todo!()
    }

    fn zero() -> Self {
        false
    }
}

fn is_nan_int<T>(_x: &T) -> bool {
    false
}

fn is_nan_f32(x: &f32) -> bool {
    f32::is_nan(*x)
}

fn is_nan_f64(x: &f64) -> bool {
    f64::is_nan(*x)
}

pub trait AsPrimF32 {
    fn as_prim_f32(&self) -> f32;
}

macro_rules! impl_as_prim_f32 {
    ($ty:ident) => {
        impl AsPrimF32 for $ty {
            fn as_prim_f32(&self) -> f32 {
                *self as f32
            }
        }
    };
}

impl_as_prim_f32!(u8);
impl_as_prim_f32!(u16);
impl_as_prim_f32!(u32);
impl_as_prim_f32!(u64);
impl_as_prim_f32!(i8);
impl_as_prim_f32!(i16);
impl_as_prim_f32!(i32);
impl_as_prim_f32!(i64);
impl_as_prim_f32!(f32);
impl_as_prim_f32!(f64);

impl AsPrimF32 for BoolNum {
    fn as_prim_f32(&self) -> f32 {
        self.0 as f32
    }
}

impl AsPrimF32 for StringNum {
    fn as_prim_f32(&self) -> f32 {
        netpod::log::error!("TODO impl AsPrimF32 for StringNum");
        todo!()
    }
}

impl_num_ops!(u8, MIN, MAX, is_nan_int, 0);
impl_num_ops!(u16, MIN, MAX, is_nan_int, 0);
impl_num_ops!(u32, MIN, MAX, is_nan_int, 0);
impl_num_ops!(u64, MIN, MAX, is_nan_int, 0);
impl_num_ops!(i8, MIN, MAX, is_nan_int, 0);
impl_num_ops!(i16, MIN, MAX, is_nan_int, 0);
impl_num_ops!(i32, MIN, MAX, is_nan_int, 0);
impl_num_ops!(i64, MIN, MAX, is_nan_int, 0);
impl_num_ops!(f32, NAN, NAN, is_nan_f32, 0.);
impl_num_ops!(f64, NAN, NAN, is_nan_f64, 0.);
impl_num_ops!(BoolNum, MIN, MAX, is_nan_int, BoolNum(0));
impl_num_ops!(StringNum, MIN, MAX, is_nan_int, StringNum(String::new()));

impl SubFrId for StringNum {
    const SUB: u32 = 0x0d;
}

impl SubFrId for BoolNum {
    const SUB: u32 = 0x0e;
}

impl items_0::scalar_ops::AsPrimF32 for BoolNum {
    fn as_prim_f32_b(&self) -> f32 {
        todo!()
    }
}

impl items_0::scalar_ops::AsPrimF32 for StringNum {
    fn as_prim_f32_b(&self) -> f32 {
        todo!()
    }
}

impl items_0::scalar_ops::ScalarOps for BoolNum {
    fn zero_b() -> Self {
        todo!()
    }

    fn equal_slack(&self, _rhs: &Self) -> bool {
        todo!()
    }
}

impl items_0::scalar_ops::ScalarOps for StringNum {
    fn zero_b() -> Self {
        todo!()
    }

    fn equal_slack(&self, _rhs: &Self) -> bool {
        todo!()
    }
}
