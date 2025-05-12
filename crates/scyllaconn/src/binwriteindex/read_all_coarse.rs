use super::BinWriteIndexRtStream;
use crate::worker::ScyllaQueue;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use daqbuf_series::SeriesId;
use futures_util::TryStreamExt;
use netpod::log;
use netpod::range::evrange::NanoRange;
use netpod::ttl::RetentionTime;
use netpod::DtMs;
use std::collections::VecDeque;

macro_rules! info { ($($arg:expr),*) => ( if true { log::info!($($arg),*); } ); }
macro_rules! debug { ($($arg:expr),*) => ( if true { log::debug!($($arg),*); } ); }

autoerr::create_error_v1!(
    name(Error, "BinIndexReadAllCoarse"),
    enum variants {
        Worker(#[from] crate::worker::Error),
        BinWriteIndexRead(#[from] super::Error),
    },
);

pub async fn read_all_coarse(
    series: SeriesId,
    range: NanoRange,
    scyqueue: &ScyllaQueue,
) -> Result<VecDeque<(RetentionTime, MspU32, u32, DtMs)>, Error> {
    let rts = {
        use RetentionTime::*;
        [Long, Medium, Short]
    };
    let mut ret = VecDeque::new();
    for rt in rts {
        let pbp = PrebinnedPartitioning::Day1;
        let mut stream = BinWriteIndexRtStream::new(rt.clone(), series, pbp, range.clone(), scyqueue.clone());
        while let Some(x) = stream.try_next().await? {
            for e in x.entries {
                let binlen = DtMs::from_ms_u64(e.binlen as u64);
                let item = (rt.clone(), x.msp.clone(), e.lsp, binlen);
                ret.push_back(item);
            }
        }
    }
    Ok(ret)
}

pub fn select_potential_binlen(options: VecDeque<(RetentionTime, MspU32, u32, DtMs)>) -> Result<(), Error> {
    // Check first if there are common binlen over all the range.
    // If not, filter out the options which could build content from finer resolution.
    // Then heuristically select the best match.
    // PrebinnedPartitioning::Day1.msp_lsp(val)
    todo!()
}
