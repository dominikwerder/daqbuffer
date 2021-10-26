use crate::archeng::datablockstream::DatablockStream;
use crate::events::{FrameMaker, FrameMakerTrait};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::Framable;
use netpod::ChannelConfigQuery;
use netpod::{query::RawEventsQuery, ChannelArchiver};
use std::pin::Pin;
use streams::rangefilter::RangeFilter;

pub async fn make_event_pipe(
    evq: &RawEventsQuery,
    conf: &ChannelArchiver,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error> {
    let range = evq.range.clone();
    let channel = evq.channel.clone();
    let expand = evq.agg_kind.need_expand();

    // TODO I need the numeric type here which I expect for that channel in order to construct FrameMaker.
    // TODO Need to pass that requirement down to disk reader: error if type changes.

    let channel_config = {
        let q = ChannelConfigQuery {
            channel: channel.clone(),
            range: range.clone(),
        };
        crate::archeng::channel_config(&q, conf).await?
    };

    let data = DatablockStream::for_channel_range(
        range.clone(),
        channel,
        conf.data_base_paths.clone().into(),
        expand,
        u64::MAX,
    );
    let filtered = RangeFilter::new(data, range, expand);
    let stream = filtered;
    let mut frame_maker = Box::new(FrameMaker::with_item_type(
        channel_config.scalar_type.clone(),
        channel_config.shape.clone(),
        evq.agg_kind.clone(),
    )) as Box<dyn FrameMakerTrait>;
    let ret = stream.map(move |j| frame_maker.make_frame(j));
    Ok(Box::pin(ret))
}
