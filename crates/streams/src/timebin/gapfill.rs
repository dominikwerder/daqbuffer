use super::cached::reader::CacheReadProvider;
use crate::tcprawclient::OpenBoxedBytesStreamsBox;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::binsdim0::BinsDim0;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::BinnedRange;
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
#[cstm(name = "BinCachedGapFill")]
pub enum Error {
    CacheReader(#[from] super::cached::reader::Error),
    GapFromFiner,
}

type INP = Pin<Box<dyn Stream<Item = Sitemty<BinsDim0<f32>>> + Send>>;

// Try to read from cache for the given bin len.
// For gaps in the stream, construct an alternative input from finer bin len with a binner.
pub struct GapFill {
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
    inp: INP,
    inp_buf: Option<BinsDim0<f32>>,
    inp_finer: Option<INP>,
    last_bin_ts2: Option<TsNano>,
    exp_finer_range: NanoRange,
    cache_read_provider: Arc<dyn CacheReadProvider>,
}

impl GapFill {
    // bin_len of the given range must be a cacheable bin_len.
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
        cache_read_provider: Arc<dyn CacheReadProvider>,
    ) -> Result<Self, Error> {
        // super::fromlayers::TimeBinnedFromLayers::new(series, range, do_time_weight, bin_len_layers)?;
        let inp = super::cached::reader::CachedReader::new(
            series,
            range.bin_len.to_dt_ms(),
            range.clone(),
            cache_read_provider.clone(),
        )?
        .map(|x| match x {
            Ok(x) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))),
            Err(e) => Err(::err::Error::from_string(e)),
        });
        let ret = Self {
            ch_conf,
            transform_query,
            sub,
            log_level,
            ctx,
            open_bytes,
            series,
            range,
            do_time_weight,
            bin_len_layers,
            inp: Box::pin(inp),
            inp_buf: None,
            inp_finer: None,
            last_bin_ts2: None,
            // TODO just dummy:
            exp_finer_range: NanoRange { beg: 0, end: 0 },
            cache_read_provider,
        };
        Ok(ret)
    }

    fn handle_bins_finer(mut self: Pin<&mut Self>, bins: BinsDim0<f32>) -> Result<BinsDim0<f32>, Error> {
        for (&ts1, &ts2) in bins.ts1s.iter().zip(&bins.ts2s) {
            if let Some(last) = self.last_bin_ts2 {
                if ts1 != last.ns() {
                    return Err(Error::GapFromFiner);
                }
            }
            self.last_bin_ts2 = Some(TsNano::from_ns(ts2));
        }

        // TODO keep bins from finer source.
        // Only write bins to cache if we receive another

        // TODO make sure that input does not send "made-up" empty future bins.
        // On the other hand, if the request is over past range, but the channel was silent ever since?
        // Then we should in principle know that from is-alive status checking.
        // So, until then, allow made-up bins?
        // Maybe, for now, only write those bins before some last non-zero-count bin. The only safe way.

        Ok(bins)
    }

    fn handle_bins(mut self: Pin<&mut Self>, bins: BinsDim0<f32>) -> Result<BinsDim0<f32>, Error> {
        // TODO could use an interface to iterate over opaque bin items that only expose
        // edge and count information with all remaining values opaque.
        for (i, (&ts1, &ts2)) in bins.ts1s.iter().zip(&bins.ts2s).enumerate() {
            if let Some(last) = self.last_bin_ts2 {
                if ts1 != last.ns() {
                    let mut ret = <BinsDim0<f32> as items_0::Empty>::empty();
                    let mut bins = bins;
                    bins.drain_into(&mut ret, 0..i);
                    self.inp_buf = Some(bins);
                    let range = NanoRange {
                        beg: last.ns(),
                        end: ts1,
                    };
                    self.setup_inp_finer(range)?;
                    return Ok(ret);
                }
            }
            self.last_bin_ts2 = Some(TsNano::from_ns(ts2));
        }
        Ok(bins)
    }

    fn setup_inp_finer(mut self: Pin<&mut Self>, range: NanoRange) -> Result<(), Error> {
        // Set up range to fill from finer.
        self.exp_finer_range = range.clone();
        if let Some(bin_len_finer) =
            super::grid::find_next_finer_bin_len(self.range.bin_len.to_dt_ms(), &self.bin_len_layers)
        {
            let range_finer = BinnedRange::from_nano_range(range, bin_len_finer);
            let inp_finer = GapFill::new(
                self.ch_conf.clone(),
                self.transform_query.clone(),
                self.sub.clone(),
                self.log_level.clone(),
                self.ctx.clone(),
                self.open_bytes.clone(),
                self.series,
                range_finer.clone(),
                self.do_time_weight,
                self.bin_len_layers.clone(),
                self.cache_read_provider.clone(),
            )?;
            let stream = Box::pin(inp_finer);
            let do_time_weight = self.do_time_weight;
            let range = BinnedRange::from_nano_range(range_finer.full_range(), self.range.bin_len.to_dt_ms());
            let stream =
                super::basic::TimeBinnedStream::new(stream, netpod::BinnedRangeEnum::Time(range), do_time_weight);
            self.inp_finer = Some(Box::pin(stream));
        } else {
            let do_time_weight = self.do_time_weight;
            let one_before_range = true;
            let range = BinnedRange::from_nano_range(range, self.range.bin_len.to_dt_ms());
            let stream = crate::timebinnedjson::TimeBinnableStream::new(
                range.full_range(),
                one_before_range,
                self.ch_conf.clone(),
                self.transform_query.clone(),
                self.sub.clone(),
                self.log_level.clone(),
                self.ctx.clone(),
                self.open_bytes.clone(),
            );
            // let stream: Pin<Box<dyn items_0::transform::TimeBinnableStreamTrait>> = stream;
            let stream = Box::pin(stream);
            // TODO rename TimeBinnedStream to make it more clear that it is the component which initiates the time binning.
            let stream =
                super::basic::TimeBinnedStream::new(stream, netpod::BinnedRangeEnum::Time(range), do_time_weight);
            let stream = stream.map(|item| match item {
                Ok(x) => match x {
                    StreamItem::DataItem(x) => match x {
                        RangeCompletableItem::Data(mut x) => {
                            // TODO need a typed time binner
                            if let Some(x) = x.as_any_mut().downcast_mut::<BinsDim0<f32>>() {
                                let y = x.clone();
                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(y)))
                            } else {
                                Err(::err::Error::with_msg_no_trace(
                                    "GapFill expects incoming BinsDim0<f32>",
                                ))
                            }
                        }
                        RangeCompletableItem::RangeComplete => {
                            Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                        }
                    },
                    StreamItem::Log(x) => Ok(StreamItem::Log(x)),
                    StreamItem::Stats(x) => Ok(StreamItem::Stats(x)),
                },
                Err(e) => Err(e),
            });
            // let stream: Pin<
            //     Box<dyn Stream<Item = Sitemty<Box<dyn items_0::timebin::TimeBinned>>> + Send>,
            // > = Box::pin(stream);
            self.inp_finer = Some(Box::pin(stream));
        }
        Ok(())
    }
}

impl Stream for GapFill {
    type Item = Sitemty<BinsDim0<f32>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let Some(inp_finer) = self.inp_finer.as_mut() {
                // TODO
                // detect also gaps here: if gap from finer, then error.
                // on CacheUsage Use or Rereate:
                // write these bins to cache because we did not find them in cache before.
                match inp_finer.poll_next_unpin(cx) {
                    Ready(Some(Ok(x))) => match x {
                        StreamItem::DataItem(RangeCompletableItem::Data(x)) => {
                            match self.as_mut().handle_bins_finer(x) {
                                Ok(x) => Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))))),
                                Err(e) => Ready(Some(Err(::err::Error::from_string(e)))),
                            }
                        }
                        StreamItem::DataItem(RangeCompletableItem::RangeComplete) => {
                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                        }
                        StreamItem::Log(x) => Ready(Some(Ok(StreamItem::Log(x)))),
                        StreamItem::Stats(x) => Ready(Some(Ok(StreamItem::Stats(x)))),
                    },
                    Ready(Some(Err(e))) => Ready(Some(Err(::err::Error::from_string(e)))),
                    Ready(None) => {
                        if let Some(j) = self.last_bin_ts2 {
                            if j.ns() != self.exp_finer_range.end() {
                                Ready(Some(Err(::err::Error::from_string(
                                    "finer input didn't deliver to the end",
                                ))))
                            } else {
                                self.last_bin_ts2 = None;
                                self.exp_finer_range = NanoRange { beg: 0, end: 0 };
                                self.inp_finer = None;
                                continue;
                            }
                        } else {
                            Ready(Some(Err(::err::Error::from_string("finer input delivered nothing"))))
                        }
                    }
                    Pending => Pending,
                }
            } else if let Some(x) = self.inp_buf.take() {
                match self.as_mut().handle_bins_finer(x) {
                    Ok(x) => Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))))),
                    Err(e) => Ready(Some(Err(::err::Error::from_string(e)))),
                }
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(Ok(x))) => match x {
                        StreamItem::DataItem(RangeCompletableItem::Data(x)) => match self.as_mut().handle_bins(x) {
                            Ok(x) => Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))))),
                            Err(e) => Ready(Some(Err(::err::Error::from_string(e)))),
                        },
                        StreamItem::DataItem(RangeCompletableItem::RangeComplete) => {
                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                        }
                        StreamItem::Log(x) => Ready(Some(Ok(StreamItem::Log(x)))),
                        StreamItem::Stats(x) => Ready(Some(Ok(StreamItem::Stats(x)))),
                    },
                    Ready(Some(Err(e))) => Ready(Some(Err(::err::Error::from_string(e)))),
                    Ready(None) => {
                        // TODO assert that we have emitted up to the requested range.
                        // If not, request the remaining range from "finer" input.
                        if let Some(j) = self.last_bin_ts2 {
                            if j.ns() != self.exp_finer_range.end() {
                                let range = NanoRange {
                                    beg: j.ns(),
                                    end: self.range.full_range().end(),
                                };
                                match self.as_mut().setup_inp_finer(range) {
                                    Ok(()) => {
                                        continue;
                                    }
                                    Err(e) => Ready(Some(Err(::err::Error::from_string(e)))),
                                }
                            } else {
                                // self.last_bin_ts2 = None;
                                // self.exp_finer_range = NanoRange { beg: 0, end: 0 };
                                // self.inp_finer = None;
                                // continue;
                                Ready(None)
                            }
                        } else {
                            warn!("-----   NOTHING IN CACHE, SETUP FULL FROM FINER");
                            let range = self.range.full_range();
                            match self.as_mut().setup_inp_finer(range) {
                                Ok(()) => {
                                    continue;
                                }
                                Err(e) => Ready(Some(Err(::err::Error::from_string(e)))),
                            }
                        }
                    }
                    Pending => Pending,
                }
            };
        }
        // When do we detect a gap:
        // - when the current item poses a gap to the last.
        // - when we see EOS before the requested range is filled.
        // Requirements:
        // Must always request fully cache-aligned ranges.
        // Must remember where the last bin ended.

        // When a gap is detected:
        // - buffer the current item, if there is one (can also be EOS).
        // - create a new producer of bin:
        //   - GapFillwith finer range? FromFiner(series, bin_len, range) ?
        //     - TimeBinnedFromLayers for a bin_len in layers would also go directly into GapFill.
        //     what does FromFiner bring to the table?
        //     It does not attempt to read the given bin-len from a cache, because we just did attempt that.
        //     It still requires that bin-len is cacheable. (NO! it must work with the layering that I passed!)
        //     Then it finds the next cacheable
        // Ready(None)
    }
}
