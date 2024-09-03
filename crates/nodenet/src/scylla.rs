use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::channelevents::ChannelEvents;
use netpod::log::*;
use netpod::ChConf;
use netpod::SeriesKind;
use query::api4::events::EventsSubQuery;
use scyllaconn::events2::events::EventReadOpts;
use scyllaconn::events2::mergert;
use scyllaconn::worker::ScyllaQueue;
use scyllaconn::SeriesId;
use std::pin::Pin;
use taskrun::tokio;

#[derive(Debug, ThisError)]
#[cstm(name = "ScyllaChannelEventStream")]
pub enum Error {
    MergeRt(#[from] mergert::Error),
}

pub async fn scylla_channel_event_stream(
    evq: EventsSubQuery,
    chconf: ChConf,
    scyqueue: &ScyllaQueue,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    debug!("scylla_channel_event_stream  {evq:?}");
    // TODO depends in general on the query
    // TODO why both in PlainEventsQuery and as separate parameter? Check other usages.
    let _series = SeriesId::new(chconf.series());
    let readopts = EventReadOpts::new(
        evq.need_one_before_range(),
        evq.need_value_data(),
        evq.transform().enum_as_string().unwrap_or(false),
    );
    let stream: Pin<Box<dyn Stream<Item = _> + Send>> = if let Some(rt) = evq.use_rt() {
        let x = scyllaconn::events2::events::EventsStreamRt::new(
            rt,
            chconf.clone(),
            evq.range().into(),
            readopts,
            scyqueue.clone(),
        )
        .map_err(|e| scyllaconn::events2::mergert::Error::from(e));
        Box::pin(x)
    } else {
        let x =
            scyllaconn::events2::mergert::MergeRts::new(chconf.clone(), evq.range().into(), readopts, scyqueue.clone());
        Box::pin(x)
    };
    let stream = stream
        .map(move |item| match item {
            Ok(k) => match k {
                ChannelEvents::Events(mut k) => {
                    if let SeriesKind::ChannelStatus = chconf.kind() {
                        use items_0::Empty;
                        type C1 = items_2::eventsdim0::EventsDim0<u64>;
                        type C2 = items_2::eventsdim0::EventsDim0<String>;
                        if let Some(j) = k.as_any_mut().downcast_mut::<C1>() {
                            let mut g = C2::empty();
                            let tss = j.tss();
                            let vals = j.private_values_ref();
                            for (&ts, &val) in tss.iter().zip(vals.iter()) {
                                use netpod::channelstatus as cs2;
                                let val = match cs2::ChannelStatus::from_kind(val as _) {
                                    Ok(x) => x.to_user_variant_string(),
                                    Err(_) => format!("{}", val),
                                };
                                if val.len() != 0 {
                                    g.push_back(ts, 0, val);
                                }
                            }
                            Ok(ChannelEvents::Events(Box::new(g)))
                            // Ok(ChannelEvents::Events(k))
                        } else {
                            Ok(ChannelEvents::Events(k))
                        }
                    } else {
                        Ok(ChannelEvents::Events(k))
                    }
                }
                ChannelEvents::Status(k) => Ok(ChannelEvents::Status(k)),
            },
            _ => item,
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
                Err(e) => Err(err::Error::with_msg_no_trace(format!(
                    "{}::scylla_channel_event_stream  {e}",
                    module_path!()
                ))),
            };
            item
        });
    Ok(Box::pin(stream))
}
