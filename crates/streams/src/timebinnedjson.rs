use crate::collect::Collect;
use crate::json_stream::JsonBytes;
use crate::json_stream::JsonStream;
use crate::rangefilter2::RangeFilter2;
use crate::tcprawclient::container_stream_from_bytes_stream;
use crate::tcprawclient::make_sub_query;
use crate::tcprawclient::OpenBoxedBytesStreamsBox;
use crate::timebin::cached::reader::EventsReadProvider;
use crate::timebin::CacheReadProvider;
use crate::timebin::TimeBinnedStream;
use crate::transform::build_merged_event_transform;
use crate::transform::EventsToTimeBinnable;
use err::Error;
use futures_util::future::BoxFuture;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::collect_s::Collectable;
use items_0::on_sitemty_data;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::timebin::TimeBinnable;
use items_0::timebin::TimeBinned;
use items_0::transform::TimeBinnableStreamBox;
use items_0::transform::TimeBinnableStreamTrait;
use items_0::Events;
use items_2::channelevents::ChannelEvents;
use items_2::merger::Merger;
use items_2::streams::PlainEventStream;
use netpod::log::*;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::BinnedRangeEnum;
use netpod::ChannelTypeConfigGen;
use netpod::DtMs;
use netpod::ReqCtx;
use query::api4::binned::BinnedQuery;
use query::api4::events::EventsSubQuerySettings;
use query::transform::TransformQuery;
use serde_json::Value as JsonValue;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

#[allow(unused)]
fn assert_stream_send<'u, R>(stream: impl 'u + Send + Stream<Item = R>) -> impl 'u + Send + Stream<Item = R> {
    stream
}

// TODO factor out, it is use now also from GapFill.
pub async fn timebinnable_stream(
    range: NanoRange,
    one_before_range: bool,
    ch_conf: ChannelTypeConfigGen,
    transform_query: TransformQuery,
    sub: EventsSubQuerySettings,
    log_level: String,
    ctx: Arc<ReqCtx>,
    open_bytes: OpenBoxedBytesStreamsBox,
) -> Result<TimeBinnableStreamBox, Error> {
    let subq = make_sub_query(
        ch_conf,
        range.clone().into(),
        one_before_range,
        transform_query,
        sub.clone(),
        log_level.clone(),
        &ctx,
    );
    let inmem_bufcap = subq.inmem_bufcap();
    let _wasm1 = subq.wasm1().map(ToString::to_string);
    let mut tr = build_merged_event_transform(subq.transform())?;
    let bytes_streams = open_bytes.open(subq, ctx.as_ref().clone()).await?;
    let mut inps = Vec::new();
    for s in bytes_streams {
        let s = container_stream_from_bytes_stream::<ChannelEvents>(s, inmem_bufcap.clone(), "TODOdbgdesc".into())?;
        let s = Box::pin(s) as Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>;
        inps.push(s);
    }
    // TODO propagate also the max-buf-len for the first stage event reader.
    // TODO use a mixture of count and byte-size as threshold.
    let stream = Merger::new(inps, sub.merger_out_len_max());

    let stream = RangeFilter2::new(stream, range, one_before_range);

    let stream = stream.map(move |k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn Events> = Box::new(k);
            // trace!("got len {}", k.len());
            let k = tr.0.transform(k);
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
        })
    });

    #[cfg(target_abi = "")]
    #[cfg(wasm_transform)]
    let stream = if let Some(wasmname) = wasm1 {
        debug!("make wasm transform");
        use httpclient::url::Url;
        use wasmer::Value;
        use wasmer::WasmSlice;
        let t = httpclient::http_get(
            Url::parse(&format!("http://data-api.psi.ch/distri/{}", wasmname)).unwrap(),
            "*/*",
            ctx,
        )
        .await
        .unwrap();
        let wasm = t.body;
        // let wasm = include_bytes!("dummy.wasm");
        let mut store = wasmer::Store::default();
        let module = wasmer::Module::new(&store, wasm).unwrap();
        // TODO assert that memory is large enough
        let memory = wasmer::Memory::new(&mut store, wasmer::MemoryType::new(10, Some(30), false)).unwrap();
        let import_object = wasmer::imports! {
            "env" => {
                "memory" => memory.clone(),
            }
        };
        let instance = wasmer::Instance::new(&mut store, &module, &import_object).unwrap();
        let get_buffer_ptr = instance.exports.get_function("get_buffer_ptr").unwrap();
        let buffer_ptr = get_buffer_ptr.call(&mut store, &[]).unwrap();
        let buffer_ptr = buffer_ptr[0].i32().unwrap();
        let stream = stream.map(move |x| {
            let memory = memory.clone();
            let item = on_sitemty_data!(x, |mut evs: Box<dyn Events>| {
                let x = {
                    use items_0::AsAnyMut;
                    if true {
                        let r1 = evs
                            .as_any_mut()
                            .downcast_mut::<items_2::eventsdim0::EventsDim0<f64>>()
                            .is_some();
                        let r2 = evs
                            .as_mut()
                            .as_any_mut()
                            .downcast_mut::<items_2::eventsdim0::EventsDim0<f64>>()
                            .is_some();
                        let r3 = evs
                            .as_any_mut()
                            .downcast_mut::<Box<items_2::eventsdim0::EventsDim0<f64>>>()
                            .is_some();
                        let r4 = evs
                            .as_mut()
                            .as_any_mut()
                            .downcast_mut::<Box<items_2::eventsdim0::EventsDim0<f64>>>()
                            .is_some();
                        let r5 = evs.as_mut().as_any_mut().downcast_mut::<ChannelEvents>().is_some();
                        let r6 = evs.as_mut().as_any_mut().downcast_mut::<Box<ChannelEvents>>().is_some();
                        debug!("wasm  castings:  {r1}  {r2}  {r3}  {r4}  {r5}  {r6}");
                    }
                    if let Some(evs) = evs.as_any_mut().downcast_mut::<ChannelEvents>() {
                        match evs {
                            ChannelEvents::Events(evs) => {
                                if let Some(evs) =
                                    evs.as_any_mut().downcast_mut::<items_2::eventsdim0::EventsDim0<f64>>()
                                {
                                    use items_0::WithLen;
                                    if evs.len() == 0 {
                                        debug!("wasm  empty EventsDim0<f64>");
                                    } else {
                                        debug!("wasm  see EventsDim0<f64>  len {}", evs.len());
                                        let max_len_needed = 16000;
                                        let dummy1 = instance.exports.get_function("dummy1").unwrap();
                                        let s = evs.values.as_mut_slices();
                                        for sl in [s.0, s.1] {
                                            if sl.len() > max_len_needed as _ {
                                                // TODO cause error
                                                panic!();
                                            }
                                            let wmemoff = buffer_ptr as u64;
                                            let view = memory.view(&store);
                                            // TODO is the offset bytes or elements?
                                            let wsl = WasmSlice::<f64>::new(&view, wmemoff, sl.len() as _).unwrap();
                                            // debug!("wasm pages {:?}  data size {:?}", view.size(), view.data_size());
                                            wsl.write_slice(&sl).unwrap();
                                            let ptr = wsl.as_ptr32();
                                            debug!("ptr {:?}  offset {}", ptr, ptr.offset());
                                            let params = [Value::I32(ptr.offset() as _), Value::I32(sl.len() as _)];
                                            let res = dummy1.call(&mut store, &params).unwrap();
                                            match res[0] {
                                                Value::I32(x) => {
                                                    debug!("wasm  dummy1 returned: {x:?}");
                                                    if x != 1 {
                                                        error!("unexpected return value {res:?}");
                                                    }
                                                }
                                                _ => {
                                                    error!("unexpected return type {res:?}");
                                                }
                                            }
                                            // Init the slice again because we need to drop ownership for the function call.
                                            let view = memory.view(&store);
                                            let wsl = WasmSlice::<f64>::new(&view, wmemoff, sl.len() as _).unwrap();
                                            wsl.read_slice(sl).unwrap();
                                        }
                                    }
                                } else {
                                    debug!("wasm  not EventsDim0<f64>");
                                }
                            }
                            ChannelEvents::Status(_) => {}
                        }
                    } else {
                        debug!("wasm  not ChannelEvents");
                    }
                    evs
                };
                Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))
            });
            // Box::new(item) as Box<dyn Framable + Send>
            item
        });
        Box::pin(stream) as Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Events>>> + Send>>
    } else {
        let stream = stream.map(|x| x);
        Box::pin(stream)
    };

    // let stream = stream.map(move |k| {
    //     on_sitemty_data!(k, |k| {
    //         let k: Box<dyn Collectable> = Box::new(k);
    //         Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
    //     })
    // });

    let stream = PlainEventStream::new(stream);
    let stream = EventsToTimeBinnable::new(stream);
    let stream = Box::pin(stream);
    Ok(TimeBinnableStreamBox(stream))
}

pub struct TimeBinnableStream {
    make_stream_fut: Option<Pin<Box<dyn Future<Output = Result<TimeBinnableStreamBox, Error>> + Send>>>,
    stream: Option<Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinnable>>> + Send>>>,
}

impl TimeBinnableStream {
    pub fn new(
        range: NanoRange,
        one_before_range: bool,
        ch_conf: ChannelTypeConfigGen,
        transform_query: TransformQuery,
        sub: EventsSubQuerySettings,
        log_level: String,
        // TODO take by Arc ref
        ctx: Arc<ReqCtx>,
        open_bytes: OpenBoxedBytesStreamsBox,
    ) -> Self {
        let fut = timebinnable_stream(
            range,
            one_before_range,
            ch_conf,
            transform_query,
            sub,
            log_level,
            ctx,
            open_bytes,
        );
        let fut = Box::pin(fut);
        Self {
            make_stream_fut: Some(fut),
            stream: None,
        }
    }
}

// impl WithTransformProperties + Send

impl Stream for TimeBinnableStream {
    type Item = Sitemty<Box<dyn TimeBinnable>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let Some(fut) = self.make_stream_fut.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(x) => match x {
                        Ok(x) => {
                            self.make_stream_fut = None;
                            self.stream = Some(Box::pin(x));
                            continue;
                        }
                        Err(e) => Ready(Some(Err(e))),
                    },
                    Pending => Pending,
                }
            } else if let Some(fut) = self.stream.as_mut() {
                match fut.poll_next_unpin(cx) {
                    Ready(Some(x)) => Ready(Some(x)),
                    Ready(None) => {
                        self.stream = None;
                        Ready(None)
                    }
                    Pending => Pending,
                }
            } else {
                Ready(None)
            };
        }
    }
}

async fn timebinned_stream(
    query: BinnedQuery,
    binned_range: BinnedRangeEnum,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    open_bytes: OpenBoxedBytesStreamsBox,
    cache_read_provider: Option<Arc<dyn CacheReadProvider>>,
    events_read_provider: Option<Arc<dyn EventsReadProvider>>,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinned>>> + Send>>, Error> {
    use netpod::query::CacheUsage;
    match (query.cache_usage(), cache_read_provider, events_read_provider) {
        (CacheUsage::Use | CacheUsage::Recreate, Some(cache_read_provider), Some(events_read_provider)) => {
            let series = if let Some(x) = query.channel().series() {
                x
            } else {
                return Err(Error::with_msg_no_trace(
                    "cached time binned only available given a series id",
                ));
            };
            info!("--- CACHING PATH ---");
            info!("{query:?}");
            info!("subgrids {:?}", query.subgrids());
            let range = binned_range.binned_range_time().to_nano_range();
            let do_time_weight = true;
            let bin_len_layers = if let Some(subgrids) = query.subgrids() {
                subgrids
                    .iter()
                    .map(|&x| DtMs::from_ms_u64(1000 * x.as_secs()))
                    .collect()
            } else {
                vec![
                    DtMs::from_ms_u64(1000 * 10),
                    DtMs::from_ms_u64(1000 * 60 * 60),
                    // DtMs::from_ms_u64(1000 * 60 * 60 * 12),
                    // DtMs::from_ms_u64(1000 * 10),
                ]
            };
            let stream = crate::timebin::TimeBinnedFromLayers::new(
                ch_conf,
                query.cache_usage(),
                query.transform().clone(),
                EventsSubQuerySettings::from(&query),
                query.log_level().into(),
                Arc::new(ctx.clone()),
                open_bytes.clone(),
                series,
                binned_range.binned_range_time(),
                do_time_weight,
                bin_len_layers,
                cache_read_provider,
                events_read_provider,
            )
            .map_err(Error::from_string)?;
            let stream = stream.map(|item| {
                on_sitemty_data!(item, |k| Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                    Box::new(k) as Box<dyn TimeBinned>
                ))))
            });
            let stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinned>>> + Send>> = Box::pin(stream);
            Ok(stream)
        }
        _ => {
            let range = binned_range.binned_range_time().to_nano_range();

            let do_time_weight = true;
            let one_before_range = true;

            let stream = timebinnable_stream(
                range,
                one_before_range,
                ch_conf,
                query.transform().clone(),
                (&query).into(),
                query.log_level().into(),
                Arc::new(ctx.clone()),
                open_bytes,
            )
            .await?;
            let stream: Pin<Box<dyn TimeBinnableStreamTrait>> = stream.0;
            let stream = Box::pin(stream);
            // TODO rename TimeBinnedStream to make it more clear that it is the component which initiates the time binning.
            let stream = TimeBinnedStream::new(stream, binned_range, do_time_weight);
            let stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinned>>> + Send>> = Box::pin(stream);
            Ok(stream)
        }
    }
}

fn timebinned_to_collectable(
    stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinned>>> + Send>>,
) -> Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Collectable>>> + Send>> {
    let stream = stream.map(|k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn Collectable> = Box::new(k);
            trace!("got len {}", k.len());
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
        })
    });
    let stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Collectable>>> + Send>> = Box::pin(stream);
    stream
}

pub async fn timebinned_json(
    query: BinnedQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    open_bytes: OpenBoxedBytesStreamsBox,
    cache_read_provider: Option<Arc<dyn CacheReadProvider>>,
    events_read_provider: Option<Arc<dyn EventsReadProvider>>,
) -> Result<JsonValue, Error> {
    let deadline = Instant::now() + query.timeout_content().unwrap_or(Duration::from_millis(5000));
    let binned_range = BinnedRangeEnum::covering_range(query.range().clone(), query.bin_count())?;
    // TODO derive better values, from query
    let collect_max = 10000;
    let bytes_max = 100 * collect_max;
    let stream = timebinned_stream(
        query.clone(),
        binned_range.clone(),
        ch_conf,
        ctx,
        open_bytes,
        cache_read_provider,
        events_read_provider,
    )
    .await?;
    let stream = timebinned_to_collectable(stream);
    let collected = Collect::new(stream, deadline, collect_max, bytes_max, None, Some(binned_range));
    let collected: BoxFuture<_> = Box::pin(collected);
    let collected = collected.await?;
    info!("timebinned_json collected type_name {:?}", collected.type_name());
    let collected = if let Some(bins) = collected
        .as_any_ref()
        .downcast_ref::<items_2::binsdim0::BinsDim0CollectedResult<netpod::EnumVariant>>()
    {
        info!("MATCHED");
        bins.boxed_collected_with_enum_fix()
    } else {
        collected
    };
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}

fn take_collector_result(coll: &mut Box<dyn items_0::collect_s::Collector>) -> Option<serde_json::Value> {
    match coll.result(None, None) {
        Ok(collres) => {
            let collres = if let Some(bins) = collres
                .as_any_ref()
                .downcast_ref::<items_2::binsdim0::BinsDim0CollectedResult<netpod::EnumVariant>>()
            {
                info!("MATCHED ENUM");
                bins.boxed_collected_with_enum_fix()
            } else {
                collres
            };
            match serde_json::to_value(&collres) {
                Ok(val) => Some(val),
                Err(e) => Some(serde_json::Value::String(format!("{e}"))),
            }
        }
        Err(e) => Some(serde_json::Value::String(format!("{e}"))),
    }
}

pub async fn timebinned_json_framed(
    query: BinnedQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    open_bytes: OpenBoxedBytesStreamsBox,
    cache_read_provider: Option<Arc<dyn CacheReadProvider>>,
    events_read_provider: Option<Arc<dyn EventsReadProvider>>,
) -> Result<JsonStream, Error> {
    trace!("timebinned_json_framed");
    let binned_range = BinnedRangeEnum::covering_range(query.range().clone(), query.bin_count())?;
    // TODO derive better values, from query
    let stream = timebinned_stream(
        query.clone(),
        binned_range.clone(),
        ch_conf,
        ctx,
        open_bytes,
        cache_read_provider,
        events_read_provider,
    )
    .await?;
    let stream = timebinned_to_collectable(stream);

    let mut coll = None;
    let interval = tokio::time::interval(Duration::from(
        query.timeout_content().unwrap_or(Duration::from_millis(1000)),
    ));
    let stream = stream.map(|x| Some(x)).chain(futures_util::stream::iter([None]));
    let stream = tokio_stream::StreamExt::timeout_repeating(stream, interval).map(move |x| match x {
        Ok(item) => match item {
            Some(x) => match x {
                Ok(x) => match x {
                    StreamItem::DataItem(x) => match x {
                        RangeCompletableItem::Data(mut item) => {
                            let coll = coll.get_or_insert_with(|| item.new_collector());
                            coll.ingest(&mut item);
                            if coll.len() >= 128 {
                                take_collector_result(coll)
                            } else {
                                None
                            }
                        }
                        RangeCompletableItem::RangeComplete => None,
                    },
                    StreamItem::Log(x) => {
                        info!("{x:?}");
                        None
                    }
                    StreamItem::Stats(x) => {
                        info!("{x:?}");
                        None
                    }
                },
                Err(e) => Some(serde_json::Value::String(format!("{e}"))),
            },
            None => {
                if let Some(coll) = coll.as_mut() {
                    take_collector_result(coll)
                } else {
                    None
                }
            }
        },
        Err(_) => {
            if let Some(coll) = coll.as_mut() {
                if coll.len() != 0 {
                    take_collector_result(coll)
                } else {
                    None
                }
            } else {
                None
            }
        }
    });
    // TODO skip the intermediate conversion to js value, go directly to string data
    let stream = stream.map(|x| match x {
        Some(x) => Some(JsonBytes::new(serde_json::to_string(&x).unwrap())),
        None => None,
    });
    let stream = stream.filter_map(|x| futures_util::future::ready(x));
    let stream = stream.map(|x| Ok(x));

    // let stream = dyn_events_stream(evq, ch_conf, ctx, open_bytes).await?;
    // let stream = events_stream_to_json_stream(stream);
    // let stream = non_empty(stream);
    // let stream = only_first_err(stream);
    Ok(Box::pin(stream))
}
