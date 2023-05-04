#[cfg(test)]
mod api1;
#[cfg(test)]
mod api4;
pub mod archapp;
pub mod binnedjson;
#[cfg(test)]
mod timeweightedjson;

use bytes::BytesMut;
use err::Error;
use std::future::Future;

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
