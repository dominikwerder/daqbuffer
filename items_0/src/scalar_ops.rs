use crate::subfr::SubFrId;
use serde::Serialize;
use std::fmt;

#[allow(unused)]
const fn is_nan_int<T>(_x: &T) -> bool {
    false
}

#[allow(unused)]
fn is_nan_f32(x: f32) -> bool {
    x.is_nan()
}

#[allow(unused)]
fn is_nan_f64(x: f64) -> bool {
    x.is_nan()
}

pub trait AsPrimF32 {
    fn as_prim_f32_b(&self) -> f32;
}

macro_rules! impl_as_prim_f32 {
    ($ty:ident) => {
        impl AsPrimF32 for $ty {
            fn as_prim_f32_b(&self) -> f32 {
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

pub trait ScalarOps:
    fmt::Debug + Clone + PartialOrd + SubFrId + AsPrimF32 + Serialize + Unpin + Send + 'static
{
    fn zero_b() -> Self;
    fn equal_slack(&self, rhs: &Self) -> bool;
}

macro_rules! impl_num_ops {
    ($ty:ident, $zero:expr, $equal_slack:ident) => {
        impl ScalarOps for $ty {
            fn zero_b() -> Self {
                $zero
            }

            fn equal_slack(&self, rhs: &Self) -> bool {
                $equal_slack(*self, *rhs)
            }
        }
    };
}

fn equal_int<T: PartialEq>(a: T, b: T) -> bool {
    a == b
}

fn equal_f32(a: f32, b: f32) -> bool {
    (a - b).abs() < 1e-4 || (a / b > 0.999 && a / b < 1.001)
}

fn equal_f64(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-6 || (a / b > 0.99999 && a / b < 1.00001)
}

impl_num_ops!(u8, 0, equal_int);
impl_num_ops!(u16, 0, equal_int);
impl_num_ops!(u32, 0, equal_int);
impl_num_ops!(u64, 0, equal_int);
impl_num_ops!(i8, 0, equal_int);
impl_num_ops!(i16, 0, equal_int);
impl_num_ops!(i32, 0, equal_int);
impl_num_ops!(i64, 0, equal_int);
impl_num_ops!(f32, 0., equal_f32);
impl_num_ops!(f64, 0., equal_f64);
