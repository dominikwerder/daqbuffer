use crate::collect::Collect;
use crate::rangefilter2::RangeFilter2;
use crate::tcprawclient::open_event_data_streams;
use crate::timebin::TimeBinnedStream;
use crate::transform::build_merged_event_transform;
use crate::transform::EventsToTimeBinnable;
use err::Error;
use futures_util::future::BoxFuture;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::collect_s::Collectable;
use items_0::on_sitemty_data;
use items_0::streamitem::Sitemty;
use items_0::timebin::TimeBinned;
use items_0::transform::TimeBinnableStreamBox;
use items_0::transform::TimeBinnableStreamTrait;
use items_0::Events;
use items_2::channelevents::ChannelEvents;
use items_2::merger::Merger;
use items_2::streams::PlainEventStream;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::BinnedRangeEnum;
use netpod::ChannelTypeConfigGen;
use netpod::Cluster;
use netpod::ReqCtx;
use query::api4::binned::BinnedQuery;
use query::api4::events::EventsSubQuery;
use query::api4::events::EventsSubQuerySelect;
use query::api4::events::EventsSubQuerySettings;
use serde_json::Value as JsonValue;
use std::pin::Pin;
use std::time::Instant;

#[allow(unused)]
fn assert_stream_send<'u, R>(stream: impl 'u + Send + Stream<Item = R>) -> impl 'u + Send + Stream<Item = R> {
    stream
}

async fn timebinnable_stream(
    query: BinnedQuery,
    range: NanoRange,
    one_before_range: bool,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    cluster: Cluster,
) -> Result<TimeBinnableStreamBox, Error> {
    let mut select = EventsSubQuerySelect::new(ch_conf, range.clone().into(), query.transform().clone());
    if let Some(wasm1) = query.test_do_wasm() {
        select.set_wasm1(wasm1.into());
    }
    let settings = EventsSubQuerySettings::from(&query);
    let subq = EventsSubQuery::from_parts(select.clone(), settings, ctx.reqid().into());
    let mut tr = build_merged_event_transform(subq.transform())?;
    let inps = open_event_data_streams::<ChannelEvents>(subq, ctx, &cluster).await?;
    // TODO propagate also the max-buf-len for the first stage event reader.
    // TODO use a mixture of count and byte-size as threshold.
    let stream = Merger::new(inps, query.merger_out_len_max());

    let stream = RangeFilter2::new(stream, range, one_before_range);

    let stream = stream.map(move |k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn Events> = Box::new(k);
            // trace!("got len {}", k.len());
            let k = tr.0.transform(k);
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
        })
    });

    let stream = if let Some(wasmname) = select.wasm1() {
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
        use futures_util::Stream;
        use items_0::streamitem::Sitemty;
        use std::pin::Pin;
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

async fn timebinned_stream(
    query: BinnedQuery,
    binned_range: BinnedRangeEnum,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    cluster: Cluster,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinned>>> + Send>>, Error> {
    let range = binned_range.binned_range_time().to_nano_range();

    let do_time_weight = true;
    let one_before_range = true;

    let stream = timebinnable_stream(query.clone(), range, one_before_range, ch_conf, ctx, cluster).await?;
    let stream: Pin<Box<dyn TimeBinnableStreamTrait>> = stream.0;
    let stream = Box::pin(stream);
    // TODO rename TimeBinnedStream to make it more clear that it is the component which initiates the time binning.
    let stream = TimeBinnedStream::new(stream, binned_range, do_time_weight);
    let stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TimeBinned>>> + Send>> = Box::pin(stream);
    Ok(stream)
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
    cluster: Cluster,
) -> Result<JsonValue, Error> {
    let deadline = Instant::now().checked_add(query.timeout_value()).unwrap();
    let binned_range = BinnedRangeEnum::covering_range(query.range().clone(), query.bin_count())?;
    let collect_max = 10000;
    let stream = timebinned_stream(query.clone(), binned_range.clone(), ch_conf, ctx, cluster).await?;
    let stream = timebinned_to_collectable(stream);
    let collected = Collect::new(stream, deadline, collect_max, None, Some(binned_range));
    let collected: BoxFuture<_> = Box::pin(collected);
    let collected = collected.await?;
    let jsval = serde_json::to_value(&collected)?;
    Ok(jsval)
}
