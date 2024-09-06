use crate::timebin::grid::find_next_finer_bin_len;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use items_0::on_sitemty_data;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::timebin::TimeBinnableTy;
use items_2::binsdim0::BinsDim0;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::BinnedRangeEnum;
use netpod::DtMs;
use netpod::TsNano;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug, ThisError)]
#[cstm(name = "TimeBinnedFromLayers")]
pub enum Error {
    Logic,
    GapFill(#[from] super::gapfill::Error),
}

type BoxedInput = Pin<Box<dyn Stream<Item = Sitemty<BinsDim0<f32>>> + Send>>;

pub struct TimeBinnedFromLayers {
    inp: BoxedInput,
}

impl TimeBinnedFromLayers {
    pub fn type_name() -> &'static str {
        core::any::type_name::<Self>()
    }

    pub fn new(
        series: u64,
        range: BinnedRange<TsNano>,
        do_time_weight: bool,
        bin_len_layers: Vec<DtMs>,
    ) -> Result<Self, Error> {
        info!(
            "{}::new  {:?}  {:?}  {:?}",
            Self::type_name(),
            series,
            range,
            bin_len_layers
        );
        // cases:
        // if this bin_len is a cachable bin_len:
        // - have to attempt to read from cache.
        // expect to read bins in a stream (randomize to small max len for testing).
        // also, if this bin_len is a cachable bin_len:
        // must produce bins missing in cache from separate stream.
        let bin_len = DtMs::from_ms_u64(range.bin_len.ms());
        if bin_len_layers.contains(&bin_len) {
            info!("{}::new  bin_len in layers", Self::type_name());
            let inp = super::gapfill::GapFill::new(series, range, do_time_weight, bin_len_layers)?;
            let ret = Self { inp: Box::pin(inp) };
            Ok(ret)
        } else {
            match find_next_finer_bin_len(bin_len, &bin_len_layers) {
                Some(finer) => {
                    // TODO
                    // produce from binned sub-stream with additional binner.
                    let range = BinnedRange::from_nano_range(range.to_nano_range(), finer);
                    info!("{}::new  next finer  {:?}  {:?}", Self::type_name(), finer, range);
                    let inp = super::gapfill::GapFill::new(series, range.clone(), do_time_weight, bin_len_layers)?;
                    let inp = super::basic::TimeBinnedStream::new(
                        Box::pin(inp),
                        BinnedRangeEnum::Time(range),
                        do_time_weight,
                    );
                    let ret = Self { inp: Box::pin(inp) };
                    Ok(ret)
                }
                None => {
                    info!("{}::new  NO next finer", Self::type_name());
                    // TODO
                    // produce from events
                    todo!()
                }
            }
        }
    }
}

impl Stream for TimeBinnedFromLayers {
    type Item = Sitemty<BinsDim0<f32>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
