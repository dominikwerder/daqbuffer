pub mod streams;

use err::Error;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::StreamItem;
#[allow(unused)]
use netpod::log::*;
use netpod::NanoRange;
use netpod::Shape;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::fs::File;
use tokio::io::AsyncRead;
use tokio::io::ReadBuf;

pub enum Fits {
    Empty,
    Lower,
    Greater,
    Inside,
    PartlyLower,
    PartlyGreater,
    PartlyLowerAndGreater,
}

pub trait WithLen {
    fn len(&self) -> usize;
}

pub trait WithTimestamps {
    fn ts(&self, ix: usize) -> u64;
}

pub trait ByteEstimate {
    fn byte_estimate(&self) -> u64;
}

pub trait RangeOverlapInfo {
    // TODO do not take by value.
    fn ends_before(&self, range: NanoRange) -> bool;
    fn ends_after(&self, range: NanoRange) -> bool;
    fn starts_after(&self, range: NanoRange) -> bool;
}

pub trait FitsInside {
    fn fits_inside(&self, range: NanoRange) -> Fits;
}

pub trait FilterFittingInside: Sized {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self>;
}

pub trait PushableIndex {
    // TODO get rid of usage, involves copy.
    // TODO check whether it makes sense to allow a move out of src. Or use a deque for src type and pop?
    fn push_index(&mut self, src: &Self, ix: usize);
}

pub trait NewEmpty {
    fn empty(shape: Shape) -> Self;
}

pub trait Appendable: WithLen {
    fn empty_like_self(&self) -> Self;

    // TODO get rid of usage, involves copy.
    fn append(&mut self, src: &Self);

    // TODO the `ts2` makes no sense for non-bin-implementors
    fn append_zero(&mut self, ts1: u64, ts2: u64);
}

pub trait Clearable {
    fn clear(&mut self);
}

pub trait EventAppendable
where
    Self: Sized,
{
    type Value;
    fn append_event(ret: Option<Self>, ts: u64, pulse: u64, value: Self::Value) -> Self;
}

pub trait TimeBins: Send + Unpin + WithLen + Appendable + FilterFittingInside {
    fn ts1s(&self) -> &Vec<u64>;
    fn ts2s(&self) -> &Vec<u64>;
}

// TODO should get I/O and tokio dependence out of this crate
pub trait ReadableFromFile: Sized {
    fn read_from_file(file: File) -> Result<ReadPbv<Self>, Error>;
    // TODO should not need this:
    fn from_buf(buf: &[u8]) -> Result<Self, Error>;
}

// TODO should get I/O and tokio dependence out of this crate
pub struct ReadPbv<T>
where
    T: ReadableFromFile,
{
    buf: Vec<u8>,
    all: Vec<u8>,
    file: Option<File>,
    _m1: PhantomData<T>,
}

impl<T> ReadPbv<T>
where
    T: ReadableFromFile,
{
    pub fn new(file: File) -> Self {
        Self {
            // TODO make buffer size a parameter:
            buf: vec![0; 1024 * 32],
            all: vec![],
            file: Some(file),
            _m1: PhantomData,
        }
    }
}

impl<T> Future for ReadPbv<T>
where
    T: ReadableFromFile + Unpin,
{
    type Output = Result<StreamItem<RangeCompletableItem<T>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        let mut buf = std::mem::replace(&mut self.buf, Vec::new());
        let ret = 'outer: loop {
            let mut dst = ReadBuf::new(&mut buf);
            if dst.remaining() == 0 || dst.capacity() == 0 {
                break Ready(Err(Error::with_msg("bad read buffer")));
            }
            let fp = self.file.as_mut().unwrap();
            let f = Pin::new(fp);
            break match File::poll_read(f, cx, &mut dst) {
                Ready(res) => match res {
                    Ok(_) => {
                        if dst.filled().len() > 0 {
                            self.all.extend_from_slice(dst.filled());
                            continue 'outer;
                        } else {
                            match T::from_buf(&mut self.all) {
                                Ok(item) => Ready(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))),
                                Err(e) => Ready(Err(e)),
                            }
                        }
                    }
                    Err(e) => Ready(Err(e.into())),
                },
                Pending => Pending,
            };
        };
        self.buf = buf;
        ret
    }
}
