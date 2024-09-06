use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use items_0::streamitem::Sitemty;
use items_2::binsdim0::BinsDim0;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::TsNano;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug, ThisError)]
#[cstm(name = "BinCachedGapFill")]
pub enum Error {}

// Try to read from cache for the given bin len.
// For gaps in the stream, construct an alternative input from finer bin len with a binner.
pub struct GapFill {}

impl GapFill {
    pub fn new(series: u64, bin_len: DtMs, range: BinnedRange<TsNano>) -> Result<Self, Error> {
        // TODO assert that the requested bin_len is a cacheable length.
        todo!()
    }
}

impl Stream for GapFill {
    type Item = Sitemty<BinsDim0<f32>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        // When do we detect a gap:
        // - when the current item poses a gap to the last.
        // - when we see EOS before the requested range is filled.
        // Requirements:
        // Must always request fully cache-aligned ranges.
        // Must remember where the last bin ended.

        // When a gap is detected:
        // - buffer the current item, if there is one (can also be EOS).
        // - create a new producer of bin:
        //   - FromFiner(series, bin_len, range)
        //     what does FromFiner bring to the table?
        //     It does not attempt to read the given bin-len from a cache, because we just did attempt that.
        //     It still requires that bin-len is cacheable. (NO! it must work with the layering that I passed!)
        //     Then it finds the next cacheable
        todo!()
    }
}
