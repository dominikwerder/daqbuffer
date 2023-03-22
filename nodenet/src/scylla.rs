use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::Appendable;
use items_0::Empty;
use items_2::channelevents::ChannelEvents;
use netpod::log::*;
use netpod::NodeConfigCached;
use netpod::ScyllaConfig;
use query::api4::events::PlainEventsQuery;
use std::pin::Pin;

pub async fn scylla_channel_event_stream(
    evq: PlainEventsQuery,
    scyco: &ScyllaConfig,
    node_config: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    // TODO depends in general on the query
    // TODO why both in PlainEventsQuery and as separate parameter? Check other usages.
    let do_one_before_range = false;
    // TODO use better builder pattern with shortcuts for production and dev defaults
    let f = crate::channelconfig::channel_config(evq.range().try_into()?, evq.channel().clone(), node_config)
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?;
    let scy = scyllaconn::create_scy_session(scyco).await?;
    let series = f.series;
    let scalar_type = f.scalar_type;
    let shape = f.shape;
    let do_test_stream_error = false;
    let with_values = evq.need_value_data();
    debug!("Make EventsStreamScylla for {series} {scalar_type:?} {shape:?}");
    let stream = scyllaconn::events::EventsStreamScylla::new(
        series,
        evq.range().into(),
        do_one_before_range,
        scalar_type,
        shape,
        with_values,
        scy,
        do_test_stream_error,
    );
    let stream = stream
        .map({
            let is_pulse_id_diff = evq.transform().is_pulse_id_diff();
            let mut pulse_last = None;
            move |item| match item {
                Ok(item) => {
                    let x = if is_pulse_id_diff {
                        let x = match item {
                            ChannelEvents::Events(item) => {
                                let (tss, pulses) = items_0::EventsNonObj::into_tss_pulses(item);
                                let mut item = items_2::eventsdim0::EventsDim0::empty();
                                for (ts, pulse) in tss.into_iter().zip(pulses) {
                                    let value = if let Some(last) = pulse_last {
                                        pulse as i64 - last as i64
                                    } else {
                                        0
                                    };
                                    item.push(ts, pulse, value);
                                    pulse_last = Some(pulse);
                                }
                                ChannelEvents::Events(Box::new(item))
                            }
                            ChannelEvents::Status(x) => ChannelEvents::Status(x),
                        };
                        x
                    } else {
                        item
                    };
                    Ok(x)
                }
                Err(e) => Err(e),
            }
        })
        .map(move |item| match &item {
            Ok(k) => match k {
                ChannelEvents::Events(k) => {
                    let n = k.len();
                    let d = evq.event_delay();
                    (item, n, d.clone())
                }
                ChannelEvents::Status(_) => (item, 1, None),
            },
            Err(_) => (item, 1, None),
        })
        .then(|(item, n, d)| async move {
            if let Some(d) = d {
                warn!("sleep {} times {:?}", n, d);
                tokio::time::sleep(d.saturating_mul(n as _)).await;
            }
            item
        })
        .map(|item| {
            let item = match item {
                Ok(item) => match item {
                    ChannelEvents::Events(item) => {
                        let item = ChannelEvents::Events(item);
                        let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
                        item
                    }
                    ChannelEvents::Status(item) => {
                        let item = ChannelEvents::Status(item);
                        let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
                        item
                    }
                },
                Err(e) => Err(e),
            };
            item
        });
    Ok(Box::pin(stream))
}
