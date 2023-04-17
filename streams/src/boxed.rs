use futures_util::stream::StreamExt;
use futures_util::Stream;
use items_0::on_sitemty_data;
use items_0::streamitem::Sitemty;
use items_0::transform::EventStream;
use items_0::Events;

pub fn boxed_event_stream<T, S>(inp: S) -> EventStream
where
    T: Events,
    S: Stream<Item = Sitemty<T>> + Send + 'static,
{
    let stream = inp.map(|x| {
        let x = on_sitemty_data!(x, |x| Ok(StreamItem::DataItem(RangeCompletableItem::Data(
            Box::new(x) as Box<dyn Events>
        ))));
        x
    });
    EventStream(Box::pin(stream))
}
