use crate::subfr::SubFrId;
use serde::Serialize;
use std::fmt;
use std::ops;

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

impl AsPrimF32 for bool {
    fn as_prim_f32_b(&self) -> f32 {
        if *self {
            1.
        } else {
            0.
        }
    }
}

impl AsPrimF32 for String {
    fn as_prim_f32_b(&self) -> f32 {
        // Well, at least some impl.
        self.len() as f32
    }
}

pub trait ScalarOps:
    fmt::Debug + Clone + PartialOrd + PartialEq + SubFrId + AsPrimF32 + Serialize + Unpin + Send + 'static
{
    fn scalar_type_name() -> &'static str;
    fn zero_b() -> Self;
    fn equal_slack(&self, rhs: &Self) -> bool;
    fn add(&mut self, rhs: &Self);
    fn div(&mut self, n: usize);
    fn find_vec_min(a: &Vec<Self>) -> Option<Self>;
    fn find_vec_max(a: &Vec<Self>) -> Option<Self>;
    fn avg_vec(a: &Vec<Self>) -> Option<Self>;
}

macro_rules! impl_scalar_ops {
    ($ty:ident, $zero:expr, $equal_slack:ident, $mac_add:ident, $mac_div:ident, $sty_name:expr) => {
        impl ScalarOps for $ty {
            fn scalar_type_name() -> &'static str {
                $sty_name
            }

            fn zero_b() -> Self {
                $zero
            }

            fn equal_slack(&self, rhs: &Self) -> bool {
                $equal_slack(self, rhs)
            }

            fn add(&mut self, rhs: &Self) {
                $mac_add!(self, rhs);
            }

            fn div(&mut self, n: usize) {
                $mac_div!(self, n);
            }

            fn find_vec_min(a: &Vec<Self>) -> Option<Self> {
                if a.len() == 0 {
                    None
                } else {
                    let mut k = &a[0];
                    for (i, v) in a.iter().enumerate() {
                        if *v < *k {
                            k = &a[i];
                        }
                    }
                    Some(k.clone())
                }
            }

            fn find_vec_max(a: &Vec<Self>) -> Option<Self> {
                if a.len() == 0 {
                    None
                } else {
                    let mut k = &a[0];
                    for (i, v) in a.iter().enumerate() {
                        if *v > *k {
                            k = &a[i];
                        }
                    }
                    Some(k.clone())
                }
            }

            fn avg_vec(a: &Vec<Self>) -> Option<Self> {
                if a.len() == 0 {
                    None
                } else {
                    let mut sum = Self::zero_b();
                    let mut c = 0;
                    for v in a.iter() {
                        sum.add(v);
                        c += 1;
                    }
                    ScalarOps::div(&mut sum, c);
                    Some(sum)
                }
            }
        }
    };
}

fn equal_int<T: PartialEq>(a: T, b: T) -> bool {
    a == b
}

fn equal_f32(&a: &f32, &b: &f32) -> bool {
    (a - b).abs() < 1e-4 || (a / b > 0.999 && a / b < 1.001)
}

fn equal_f64(&a: &f64, &b: &f64) -> bool {
    (a - b).abs() < 1e-6 || (a / b > 0.99999 && a / b < 1.00001)
}

fn equal_bool(&a: &bool, &b: &bool) -> bool {
    a == b
}

fn equal_string(a: &String, b: &String) -> bool {
    a == b
}

fn add_int<T: ops::AddAssign>(a: &mut T, b: &T) {
    ops::AddAssign::add_assign(a, todo!());
}

macro_rules! add_int {
    ($a:expr, $b:expr) => {
        *$a += $b;
    };
}

macro_rules! add_bool {
    ($a:expr, $b:expr) => {
        *$a |= $b;
    };
}

macro_rules! add_string {
    ($a:expr, $b:expr) => {
        $a.push_str($b);
    };
}

macro_rules! div_int {
    ($a:expr, $b:expr) => {
        // TODO for average calculation, the accumulator must be large enough!
        // Use u64 for all ints, and f32 for all floats.
        // Therefore, the name "add" is too general.
        //*$a /= $b;
    };
}

macro_rules! div_bool {
    ($a:expr, $b:expr) => {
        //
    };
}

macro_rules! div_string {
    ($a:expr, $b:expr) => {
        //
    };
}

impl_scalar_ops!(u8, 0, equal_int, add_int, div_int, "u8");
impl_scalar_ops!(u16, 0, equal_int, add_int, div_int, "u16");
impl_scalar_ops!(u32, 0, equal_int, add_int, div_int, "u32");
impl_scalar_ops!(u64, 0, equal_int, add_int, div_int, "u64");
impl_scalar_ops!(i8, 0, equal_int, add_int, div_int, "i8");
impl_scalar_ops!(i16, 0, equal_int, add_int, div_int, "i16");
impl_scalar_ops!(i32, 0, equal_int, add_int, div_int, "i32");
impl_scalar_ops!(i64, 0, equal_int, add_int, div_int, "i64");
impl_scalar_ops!(f32, 0., equal_f32, add_int, div_int, "f32");
impl_scalar_ops!(f64, 0., equal_f64, add_int, div_int, "f64");
impl_scalar_ops!(bool, false, equal_bool, add_bool, div_bool, "bool");
impl_scalar_ops!(String, String::new(), equal_string, add_string, div_string, "string");
