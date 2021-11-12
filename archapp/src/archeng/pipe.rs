use crate::archeng::blockrefstream::blockref_stream;
use crate::archeng::blockstream::BlockStream;
use crate::events::{FrameMaker, FrameMakerTrait};
use err::Error;
use futures_util::{Stream, StreamExt};
use items::binnedevents::XBinnedEvents;
use items::eventsitem::EventsItem;
use items::plainevents::PlainEvents;
use items::{Framable, LogItem, RangeCompletableItem, StreamItem};
use netpod::query::RawEventsQuery;
use netpod::{log::*, AggKind, Shape};
use netpod::{ChannelArchiver, ChannelConfigQuery};
use std::pin::Pin;
use streams::rangefilter::RangeFilter;

pub async fn make_event_pipe(
    evq: &RawEventsQuery,
    conf: ChannelArchiver,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error> {
    debug!("make_event_pipe  {:?}", evq);
    let channel_config = {
        let q = ChannelConfigQuery {
            channel: evq.channel.clone(),
            range: evq.range.clone(),
            expand: evq.agg_kind.need_expand(),
        };
        crate::archeng::channel_config_from_db(&q, &conf).await?
    };
    debug!("Channel config: {:?}", channel_config);
    let ixpaths = crate::archeng::indexfiles::index_file_path_list(evq.channel.clone(), conf.database.clone()).await?;
    info!("got categorized ixpaths: {:?}", ixpaths);
    let ixpath = ixpaths.first().unwrap().clone();
    use crate::archeng::blockstream::BlockItem;
    let refs = blockref_stream(
        evq.channel.clone(),
        evq.range.clone(),
        evq.agg_kind.need_expand(),
        ixpath.clone(),
    );
    let blocks = BlockStream::new(Box::pin(refs), evq.range.clone(), 1);
    let blocks = blocks.map(|k| match k {
        Ok(item) => match item {
            BlockItem::EventsItem(item) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))),
            BlockItem::JsVal(jsval) => Ok(StreamItem::Log(LogItem::quick(Level::DEBUG, format!("{:?}", jsval)))),
        },
        Err(e) => Err(e),
    });
    let filtered = RangeFilter::new(blocks, evq.range.clone(), evq.agg_kind.need_expand());
    let xtrans = match channel_config.shape {
        Shape::Scalar => match evq.agg_kind {
            AggKind::Plain => Box::pin(filtered) as Pin<Box<dyn Stream<Item = _> + Send>>,
            AggKind::TimeWeightedScalar | AggKind::DimXBins1 => {
                let tr = filtered.map(|j| match j {
                    Ok(j) => match j {
                        StreamItem::DataItem(j) => match j {
                            RangeCompletableItem::RangeComplete => {
                                Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                            }
                            RangeCompletableItem::Data(j) => match j {
                                EventsItem::Plain(j) => match j {
                                    PlainEvents::Scalar(j) => {
                                        let item = XBinnedEvents::Scalar(j);
                                        let item = EventsItem::XBinnedEvents(item);
                                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                    }
                                    PlainEvents::Wave(_) => panic!(),
                                },
                                EventsItem::XBinnedEvents(_) => panic!(),
                            },
                        },
                        StreamItem::Log(j) => Ok(StreamItem::Log(j)),
                        StreamItem::Stats(j) => Ok(StreamItem::Stats(j)),
                    },
                    Err(e) => Err(e),
                });
                Box::pin(tr) as _
            }
            AggKind::DimXBinsN(_) => err::todoval(),
            AggKind::EventBlobs => err::todoval(),
        },
        _ => {
            error!("TODO shape {:?}", channel_config.shape);
            panic!()
        }
    };
    let mut frame_maker = Box::new(FrameMaker::with_item_type(
        channel_config.scalar_type.clone(),
        channel_config.shape.clone(),
        evq.agg_kind.clone(),
    )) as Box<dyn FrameMakerTrait>;
    let ret = xtrans.map(move |j| frame_maker.make_frame(j));
    Ok(Box::pin(ret))
}
