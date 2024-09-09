use super::cached::reader::CacheReadProvider;
use crate::tcprawclient::OpenBoxedBytesStreamsBox;
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
use netpod::ChannelTypeConfigGen;
use netpod::DtMs;
use netpod::ReqCtx;
use netpod::TsNano;
use query::api4::events::EventsSubQuerySettings;
use query::transform::TransformQuery;
use std::pin::Pin;
use std::sync::Arc;
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
    ch_conf: ChannelTypeConfigGen,
    transform_query: TransformQuery,
    sub: EventsSubQuerySettings,
    log_level: String,
    ctx: Arc<ReqCtx>,
    open_bytes: OpenBoxedBytesStreamsBox,
    inp: BoxedInput,
}

impl TimeBinnedFromLayers {
    pub fn type_name() -> &'static str {
        core::any::type_name::<Self>()
    }

    pub fn new(
        ch_conf: ChannelTypeConfigGen,
        transform_query: TransformQuery,
        sub: EventsSubQuerySettings,
        log_level: String,
        ctx: Arc<ReqCtx>,
        open_bytes: OpenBoxedBytesStreamsBox,
        series: u64,
        range: BinnedRange<TsNano>,
        do_time_weight: bool,
        bin_len_layers: Vec<DtMs>,
        cache_read_provider: Arc<dyn CacheReadProvider + Send>,
    ) -> Result<Self, Error> {
        info!(
            "{}::new  {:?}  {:?}  {:?}",
            Self::type_name(),
            series,
            range,
            bin_len_layers
        );
        let bin_len = DtMs::from_ms_u64(range.bin_len.ms());
        if bin_len_layers.contains(&bin_len) {
            info!("{}::new  bin_len in layers", Self::type_name());
            let inp = super::gapfill::GapFill::new(
                ch_conf.clone(),
                transform_query.clone(),
                sub.clone(),
                log_level.clone(),
                ctx.clone(),
                open_bytes.clone(),
                series,
                range,
                do_time_weight,
                bin_len_layers,
                cache_read_provider,
            )?;
            let ret = Self {
                ch_conf,
                transform_query,
                sub,
                log_level,
                ctx,
                open_bytes,
                inp: Box::pin(inp),
            };
            Ok(ret)
        } else {
            match find_next_finer_bin_len(bin_len, &bin_len_layers) {
                Some(finer) => {
                    // TODO
                    // produce from binned sub-stream with additional binner.
                    let range = BinnedRange::from_nano_range(range.to_nano_range(), finer);
                    warn!("{}::new  next finer  {:?}  {:?}", Self::type_name(), finer, range);
                    let inp = super::gapfill::GapFill::new(
                        ch_conf.clone(),
                        transform_query.clone(),
                        sub.clone(),
                        log_level.clone(),
                        ctx.clone(),
                        open_bytes.clone(),
                        series,
                        range.clone(),
                        do_time_weight,
                        bin_len_layers,
                        cache_read_provider,
                    )?;
                    let inp = super::basic::TimeBinnedStream::new(
                        Box::pin(inp),
                        BinnedRangeEnum::Time(range),
                        do_time_weight,
                    );
                    let ret = Self {
                        ch_conf,
                        transform_query,
                        sub,
                        log_level,
                        ctx,
                        open_bytes,
                        inp: Box::pin(inp),
                    };
                    Ok(ret)
                }
                None => {
                    warn!("{}::new  NO next finer", Self::type_name());
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

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(x)) => Ready(Some(x)),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}
