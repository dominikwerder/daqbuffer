use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::channelevents::ChannelEvents;
use netpod::log::*;
use netpod::ChConf;
use netpod::NodeConfigCached;
use netpod::ScyllaConfig;
use query::api4::events::PlainEventsQuery;
use std::pin::Pin;

pub async fn scylla_channel_event_stream(
    evq: PlainEventsQuery,
    chconf: ChConf,
    scyco: &ScyllaConfig,
    _ncc: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    // TODO depends in general on the query
    // TODO why both in PlainEventsQuery and as separate parameter? Check other usages.
    let do_one_before_range = false;
    // TODO use better builder pattern with shortcuts for production and dev defaults
    let scy = scyllaconn::create_scy_session(scyco).await?;
    let series = chconf.series();
    let scalar_type = chconf.scalar_type();
    let shape = chconf.shape();
    let do_test_stream_error = false;
    let with_values = evq.need_value_data();
    debug!("Make EventsStreamScylla for {series:?} {scalar_type:?} {shape:?}");
    let stream = scyllaconn::events::EventsStreamScylla::new(
        series,
        evq.range().into(),
        do_one_before_range,
        scalar_type.clone(),
        shape.clone(),
        with_values,
        scy,
        do_test_stream_error,
    );
    let stream = stream
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
