#[cfg(test)]
mod api1;
#[cfg(test)]
mod api4;
pub mod archapp;
pub mod binnedbinary;
pub mod binnedjson;
#[cfg(test)]
mod events;
#[cfg(test)]
mod timeweightedjson;

use bytes::BytesMut;
use err::Error;
use std::future::Future;

fn f32_cmp_near(x: f32, y: f32) -> bool {
    let x = {
        let mut a = x.to_le_bytes();
        a[0] &= 0xf0;
        f32::from_ne_bytes(a)
    };
    let y = {
        let mut a = y.to_le_bytes();
        a[0] &= 0xf0;
        f32::from_ne_bytes(a)
    };
    x == y
}

fn f64_cmp_near(x: f64, y: f64) -> bool {
    let x = {
        let mut a = x.to_le_bytes();
        a[0] &= 0x00;
        a[1] &= 0x00;
        f64::from_ne_bytes(a)
    };
    let y = {
        let mut a = y.to_le_bytes();
        a[0] &= 0x00;
        a[1] &= 0x00;
        f64::from_ne_bytes(a)
    };
    x == y
}

fn f32_iter_cmp_near<A, B>(a: A, b: B) -> bool
where
    A: IntoIterator<Item = f32>,
    B: IntoIterator<Item = f32>,
{
    let mut a = a.into_iter();
    let mut b = b.into_iter();
    loop {
        let x = a.next();
        let y = b.next();
        if let (Some(x), Some(y)) = (x, y) {
            if !f32_cmp_near(x, y) {
                return false;
            }
        } else if x.is_some() || y.is_some() {
            return false;
        } else {
            return true;
        }
    }
}

fn f64_iter_cmp_near<A, B>(a: A, b: B) -> bool
where
    A: IntoIterator<Item = f64>,
    B: IntoIterator<Item = f64>,
{
    let mut a = a.into_iter();
    let mut b = b.into_iter();
    loop {
        let x = a.next();
        let y = b.next();
        if let (Some(x), Some(y)) = (x, y) {
            if !f64_cmp_near(x, y) {
                return false;
            }
        } else if x.is_some() || y.is_some() {
            return false;
        } else {
            return true;
        }
    }
}

#[test]
fn test_f32_iter_cmp_near() {
    let a = [-127.553e17];
    let b = [-127.554e17];
    assert_eq!(f32_iter_cmp_near(a, b), false);
    let a = [-127.55300e17];
    let b = [-127.55301e17];
    assert_eq!(f32_iter_cmp_near(a, b), true);
}

fn run_test<F>(f: F) -> Result<(), Error>
where
    F: Future<Output = Result<(), Error>> + Send,
{
    let runtime = taskrun::get_runtime();
    let _g = runtime.enter();
    runtime.block_on(f)
    //let jh = tokio::spawn(f);
    //jh.await;
}

#[test]
fn bufs() {
    use bytes::{Buf, BufMut};
    let mut buf = BytesMut::with_capacity(1024);
    assert!(buf.as_mut().len() == 0);
    buf.put_u32_le(123);
    assert!(buf.as_mut().len() == 4);
    let mut b2 = buf.split_to(4);
    assert!(b2.capacity() == 4);
    b2.advance(2);
    assert!(b2.capacity() == 2);
    b2.advance(2);
    assert!(b2.capacity() == 0);
    assert!(buf.capacity() == 1020);
    assert!(buf.remaining() == 0);
    assert!(buf.remaining_mut() >= 1020);
    assert!(buf.capacity() == 1020);
}
