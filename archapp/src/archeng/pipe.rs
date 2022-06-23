use crate::archeng::blockrefstream::blockref_stream;
use crate::archeng::blockstream::BlockStream;
use crate::events::{FrameMaker, FrameMakerTrait};
use err::Error;
use futures_util::{Stream, StreamExt};
use items::binnedevents::{MultiBinWaveEvents, SingleBinWaveEvents, XBinnedEvents};
use items::eventsitem::EventsItem;
use items::plainevents::{PlainEvents, WavePlainEvents};
use items::waveevents::{WaveNBinner, WaveXBinner};
use items::{EventsNodeProcessor, Framable, LogItem, RangeCompletableItem, StreamItem};
use netpod::log::*;
use netpod::query::RawEventsQuery;
use netpod::{AggKind, NodeConfigCached, Shape};
use netpod::{ChannelArchiver, ChannelConfigQuery};
use std::pin::Pin;
use streams::rangefilter::RangeFilter;

pub async fn make_event_pipe(
    evq: &RawEventsQuery,
    node: NodeConfigCached,
    conf: ChannelArchiver,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable + Send>> + Send>>, Error> {
    debug!("make_event_pipe  {:?}", evq);
    let channel_config = {
        let q = ChannelConfigQuery {
            channel: evq.channel.clone(),
            range: evq.range.clone(),
            expand: evq.agg_kind.need_expand(),
        };
        crate::archeng::channel_config_from_db(&q, &conf, &node.node_config.cluster.database).await?
    };
    debug!("Channel config: {:?}", channel_config);
    let ixpaths = crate::archeng::indexfiles::index_file_path_list(
        evq.channel.clone(),
        node.node_config.cluster.database.clone(),
    )
    .await?;
    debug!("got categorized ixpaths: {:?}", ixpaths);
    let ixpath = if let Some(x) = ixpaths.first() {
        x.clone()
    } else {
        return Err(Error::with_msg_no_trace("no index file for channel")
            .mark_bad_request()
            .add_public_msg(format!("No index file for {}", evq.channel.name)));
    };
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
    let cfgshape = channel_config.shape.clone();
    let q_agg_kind = evq.agg_kind.clone();
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
            AggKind::Stats1 => err::todoval(),
        },
        Shape::Wave(_n1) => match evq.agg_kind {
            AggKind::Plain => Box::pin(filtered) as Pin<Box<dyn Stream<Item = _> + Send>>,
            AggKind::TimeWeightedScalar | AggKind::DimXBins1 => {
                let tr = filtered.map(move |j| match j {
                    Ok(j) => match j {
                        StreamItem::DataItem(j) => match j {
                            RangeCompletableItem::RangeComplete => {
                                Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                            }
                            RangeCompletableItem::Data(j) => match j {
                                EventsItem::Plain(j) => match j {
                                    PlainEvents::Scalar(_) => {
                                        warn!("EventsItem::Plain Scalar for {:?}  {:?}", cfgshape, q_agg_kind);
                                        panic!()
                                    }
                                    PlainEvents::Wave(j) => {
                                        trace!("EventsItem::Plain Wave for {:?}  {:?}", cfgshape, q_agg_kind);
                                        items_proc::tycases1!(j, WavePlainEvents, (j), {
                                            let binner =
                                                WaveXBinner::<$ty>::create(cfgshape.clone(), q_agg_kind.clone());
                                            let out = binner.process(j);
                                            let item = SingleBinWaveEvents::$id(out);
                                            let item = XBinnedEvents::SingleBinWave(item);
                                            let item = EventsItem::XBinnedEvents(item);
                                            Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                        })
                                        /*match j {
                                            WavePlainEvents::I8(j) => {
                                                let binner =
                                                    WaveXBinner::<i8>::create(cfgshape.clone(), q_agg_kind.clone());
                                                let out = binner.process(j);
                                                let item = SingleBinWaveEvents::I8(out);
                                                let item = XBinnedEvents::SingleBinWave(item);
                                                let item = EventsItem::XBinnedEvents(item);
                                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                            }
                                            WavePlainEvents::I16(j) => {
                                                let binner =
                                                    WaveXBinner::<i16>::create(cfgshape.clone(), q_agg_kind.clone());
                                                let out = binner.process(j);
                                                let item = SingleBinWaveEvents::I16(out);
                                                let item = XBinnedEvents::SingleBinWave(item);
                                                let item = EventsItem::XBinnedEvents(item);
                                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                            }
                                            WavePlainEvents::I32(j) => {
                                                let binner =
                                                    WaveXBinner::<i32>::create(cfgshape.clone(), q_agg_kind.clone());
                                                let out = binner.process(j);
                                                let item = SingleBinWaveEvents::I32(out);
                                                let item = XBinnedEvents::SingleBinWave(item);
                                                let item = EventsItem::XBinnedEvents(item);
                                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                            }
                                            WavePlainEvents::F32(j) => {
                                                let binner =
                                                    WaveXBinner::<f32>::create(cfgshape.clone(), q_agg_kind.clone());
                                                let out = binner.process(j);
                                                let item = SingleBinWaveEvents::F32(out);
                                                let item = XBinnedEvents::SingleBinWave(item);
                                                let item = EventsItem::XBinnedEvents(item);
                                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                            }
                                            WavePlainEvents::F64(j) => {
                                                let binner =
                                                    WaveXBinner::<f64>::create(cfgshape.clone(), q_agg_kind.clone());
                                                let out = binner.process(j);
                                                let item = SingleBinWaveEvents::F64(out);
                                                let item = XBinnedEvents::SingleBinWave(item);
                                                let item = EventsItem::XBinnedEvents(item);
                                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                            }
                                        }*/
                                    }
                                },
                                EventsItem::XBinnedEvents(j) => match j {
                                    XBinnedEvents::Scalar(j) => {
                                        warn!("XBinnedEvents::Scalar for {:?}  {:?}", cfgshape, q_agg_kind);
                                        let item = XBinnedEvents::Scalar(j);
                                        let item = EventsItem::XBinnedEvents(item);
                                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                    }
                                    XBinnedEvents::SingleBinWave(j) => {
                                        warn!("XBinnedEvents::SingleBinWave for {:?}  {:?}", cfgshape, q_agg_kind);
                                        let item = XBinnedEvents::SingleBinWave(j);
                                        let item = EventsItem::XBinnedEvents(item);
                                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                    }
                                    XBinnedEvents::MultiBinWave(_) => todo!(),
                                },
                            },
                        },
                        StreamItem::Log(j) => Ok(StreamItem::Log(j)),
                        StreamItem::Stats(j) => Ok(StreamItem::Stats(j)),
                    },
                    Err(e) => Err(e),
                });
                Box::pin(tr) as _
            }
            AggKind::DimXBinsN(_) => {
                let tr = filtered.map(move |j| match j {
                    Ok(j) => match j {
                        StreamItem::DataItem(j) => match j {
                            RangeCompletableItem::RangeComplete => {
                                Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                            }
                            RangeCompletableItem::Data(j) => match j {
                                EventsItem::Plain(j) => match j {
                                    PlainEvents::Scalar(_) => {
                                        warn!("EventsItem::Plain Scalar for {:?}  {:?}", cfgshape, q_agg_kind);
                                        panic!()
                                    }
                                    PlainEvents::Wave(j) => {
                                        trace!("EventsItem::Plain Wave for {:?}  {:?}", cfgshape, q_agg_kind);
                                        items_proc::tycases1!(j, WavePlainEvents, (j), {
                                            let binner =
                                                    WaveNBinner::<$ty>::create(cfgshape.clone(), q_agg_kind.clone());
                                                let out = binner.process(j);
                                                let item = MultiBinWaveEvents::$id(out);
                                                let item = XBinnedEvents::MultiBinWave(item);
                                                let item = EventsItem::XBinnedEvents(item);
                                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                        })
                                        /*match j {
                                            WavePlainEvents::I8(j) => {
                                                let binner =
                                                    WaveNBinner::<i8>::create(cfgshape.clone(), q_agg_kind.clone());
                                                let out = binner.process(j);
                                                let item = MultiBinWaveEvents::I8(out);
                                                let item = XBinnedEvents::MultiBinWave(item);
                                                let item = EventsItem::XBinnedEvents(item);
                                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                            }
                                            WavePlainEvents::I16(j) => {
                                                let binner =
                                                    WaveNBinner::<i16>::create(cfgshape.clone(), q_agg_kind.clone());
                                                let out = binner.process(j);
                                                let item = MultiBinWaveEvents::I16(out);
                                                let item = XBinnedEvents::MultiBinWave(item);
                                                let item = EventsItem::XBinnedEvents(item);
                                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                            }
                                            WavePlainEvents::I32(j) => {
                                                let binner =
                                                    WaveNBinner::<i32>::create(cfgshape.clone(), q_agg_kind.clone());
                                                let out = binner.process(j);
                                                let item = MultiBinWaveEvents::I32(out);
                                                let item = XBinnedEvents::MultiBinWave(item);
                                                let item = EventsItem::XBinnedEvents(item);
                                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                            }
                                            WavePlainEvents::F32(j) => {
                                                let binner =
                                                    WaveNBinner::<f32>::create(cfgshape.clone(), q_agg_kind.clone());
                                                let out = binner.process(j);
                                                let item = MultiBinWaveEvents::F32(out);
                                                let item = XBinnedEvents::MultiBinWave(item);
                                                let item = EventsItem::XBinnedEvents(item);
                                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                            }
                                            WavePlainEvents::F64(j) => {
                                                let binner =
                                                    WaveNBinner::<f64>::create(cfgshape.clone(), q_agg_kind.clone());
                                                let out = binner.process(j);
                                                let item = MultiBinWaveEvents::F64(out);
                                                let item = XBinnedEvents::MultiBinWave(item);
                                                let item = EventsItem::XBinnedEvents(item);
                                                Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                            }
                                        }*/
                                    }
                                },
                                EventsItem::XBinnedEvents(j) => match j {
                                    XBinnedEvents::Scalar(j) => {
                                        warn!("XBinnedEvents::Scalar for {:?}  {:?}", cfgshape, q_agg_kind);
                                        err::todo();
                                        let item = XBinnedEvents::Scalar(j);
                                        let item = EventsItem::XBinnedEvents(item);
                                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                    }
                                    XBinnedEvents::SingleBinWave(j) => {
                                        warn!("XBinnedEvents::SingleBinWave for {:?}  {:?}", cfgshape, q_agg_kind);
                                        err::todo();
                                        let item = XBinnedEvents::SingleBinWave(j);
                                        let item = EventsItem::XBinnedEvents(item);
                                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                                    }
                                    XBinnedEvents::MultiBinWave(_) => todo!(),
                                },
                            },
                        },
                        StreamItem::Log(j) => Ok(StreamItem::Log(j)),
                        StreamItem::Stats(j) => Ok(StreamItem::Stats(j)),
                    },
                    Err(e) => Err(e),
                });
                Box::pin(tr) as _
            }
            AggKind::EventBlobs => err::todoval(),
            AggKind::Stats1 => err::todoval(),
        },
        _ => {
            error!("TODO shape {:?}", channel_config.shape);
            let err = Error::with_msg_no_trace(format!("TODO shape {:?}", channel_config.shape))
                .mark_bad_request()
                .add_public_msg(format!("can not yet handle shape {:?}", channel_config.shape));
            Box::pin(futures_util::stream::iter([Err(err)]))
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
