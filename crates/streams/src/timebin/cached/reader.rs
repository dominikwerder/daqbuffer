use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use items_2::binsdim0::BinsDim0;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::TsNano;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug, ThisError)]
#[cstm(name = "BinCachedReader")]
pub enum Error {}

pub struct CachedReader {}

impl CachedReader {
    pub fn new(series: u64, bin_len: DtMs, range: BinnedRange<TsNano>) -> Result<Self, Error> {
        let ret = Self {};
        Ok(ret)
    }
}

impl Stream for CachedReader {
    type Item = Result<BinsDim0<f32>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        Ready(None)
    }
}
