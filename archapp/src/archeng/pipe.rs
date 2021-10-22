use crate::archeng::datablockstream::DatablockStream;
use crate::events::{FrameMaker, FrameMakerTrait};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::Framable;
use netpod::{query::RawEventsQuery, ChannelArchiver};
use std::pin::Pin;
use streams::rangefilter::RangeFilter;

pub async fn make_event_pipe(
    evq: &RawEventsQuery,
    conf: &ChannelArchiver,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error> {
    // In order to extract something from the channel, need to look up first the type of the channel.
    //let ci = channel_info(&evq.channel, aa).await?;
    /*let mut inps = vec![];
    for p1 in &aa.data_base_paths {
        let p2 = p1.clone();
        let p3 = make_single_event_pipe(evq, p2).await?;
        inps.push(p3);
    }
    let sm = StorageMerge {
        inprng: inps.len() - 1,
        current_inp_item: (0..inps.len()).into_iter().map(|_| None).collect(),
        completed_inps: vec![false; inps.len()],
        inps,
    };*/
    let range = evq.range.clone();
    let channel = evq.channel.clone();
    let expand = evq.agg_kind.need_expand();
    let data = DatablockStream::for_channel_range(range.clone(), channel, conf.data_base_paths.clone().into(), expand);
    let filtered = RangeFilter::new(data, range, expand);
    let stream = filtered;
    let mut frame_maker = Box::new(FrameMaker::untyped(evq.agg_kind.clone())) as Box<dyn FrameMakerTrait>;
    let ret = stream.map(move |j| frame_maker.make_frame(j));
    Ok(Box::pin(ret))
}
