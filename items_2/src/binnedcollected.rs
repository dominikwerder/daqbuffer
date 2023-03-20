use crate::channelevents::ChannelEvents;
use crate::empty::empty_events_dyn_ev;
use crate::ChannelEventsInput;
use crate::Error;
use futures_util::Future;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::collect_s::Collected;
use items_0::collect_s::Collector;
use items_0::collect_s::ToJsonResult;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::TimeBinnable;
use items_0::TimeBinner;
use items_0::Transformer;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::BinnedRangeEnum;
use netpod::ScalarType;
use netpod::Shape;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

fn flush_binned(
    binner: &mut Box<dyn TimeBinner>,
    coll: &mut Option<Box<dyn Collector>>,
    force: bool,
) -> Result<(), Error> {
    trace!("flush_binned  bins_ready_count: {}", binner.bins_ready_count());
    if force {
        if binner.bins_ready_count() == 0 {
            debug!("cycle the binner forced");
            binner.cycle();
        } else {
            debug!("bins ready, do not force");
        }
    }
    if binner.bins_ready_count() > 0 {
        let ready = binner.bins_ready();
        match ready {
            Some(mut ready) => {
                trace!("binned_collected ready {ready:?}");
                if coll.is_none() {
                    *coll = Some(ready.as_collectable_mut().new_collector());
                }
                let cl = coll.as_mut().unwrap();
                cl.ingest(ready.as_collectable_mut());
                Ok(())
            }
            None => Err(format!("bins_ready_count but no result").into()),
        }
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub struct BinnedCollectedResult {
    pub range_final: bool,
    pub did_timeout: bool,
    pub result: Box<dyn Collected>,
}

fn _old_binned_collected(
    scalar_type: ScalarType,
    shape: Shape,
    binrange: BinnedRangeEnum,
    transformer: &dyn Transformer,
    deadline: Instant,
    inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>,
) -> Result<BinnedCollectedResult, Error> {
    event!(Level::TRACE, "binned_collected");
    let transprops = transformer.query_transform_properties();
    // TODO use a trait to allow check of unfinished data [hcn2956jxhwsf]
    // TODO implement continue-at [hcn2956jxhwsf]
    // TODO maybe TimeBinner should take all ChannelEvents and handle this?
    let empty_item = empty_events_dyn_ev(&scalar_type, &shape)?;
    let tmp_item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(ChannelEvents::Events(
        empty_item,
    ))));
    let empty_stream = futures_util::stream::once(futures_util::future::ready(tmp_item));
    let mut stream = empty_stream.chain(inp);
    todo!()
}

enum BinnedCollectedState {
    Init,
    Run,
    Done,
}

pub struct BinnedCollected {
    state: BinnedCollectedState,
    binrange: BinnedRangeEnum,
    scalar_type: ScalarType,
    shape: Shape,
    do_time_weight: bool,
    did_timeout: bool,
    range_final: bool,
    coll: Option<Box<dyn Collector>>,
    binner: Option<Box<dyn TimeBinner>>,
    inp: Pin<Box<dyn ChannelEventsInput>>,
}

impl BinnedCollected {
    const fn self_name() -> &'static str {
        "BinnedCollected"
    }

    pub fn new(
        binrange: BinnedRangeEnum,
        scalar_type: ScalarType,
        shape: Shape,
        do_time_weight: bool,
        //transformer: &dyn Transformer,
        deadline: Instant,
        inp: Pin<Box<dyn ChannelEventsInput>>,
    ) -> Self {
        Self {
            state: BinnedCollectedState::Init,
            binrange,
            scalar_type,
            shape,
            do_time_weight,
            did_timeout: false,
            range_final: false,
            coll: None,
            binner: None,
            inp,
        }
    }

    fn handle_item(&mut self, item: StreamItem<RangeCompletableItem<ChannelEvents>>) -> Result<(), Error> {
        match item {
            StreamItem::DataItem(k) => match k {
                RangeCompletableItem::RangeComplete => {
                    self.range_final = true;
                }
                RangeCompletableItem::Data(k) => match k {
                    ChannelEvents::Events(events) => {
                        if self.binner.is_none() {
                            let bb = events
                                .as_time_binnable()
                                .time_binner_new(self.binrange.clone(), self.do_time_weight);
                            self.binner = Some(bb);
                        }
                        let binner = self.binner.as_mut().unwrap();
                        binner.ingest(events.as_time_binnable());
                        flush_binned(binner, &mut self.coll, false)?;
                    }
                    ChannelEvents::Status(item) => {
                        trace!("{:?}", item);
                    }
                },
            },
            StreamItem::Log(item) => {
                // TODO collect also errors here?
                trace!("{:?}", item);
            }
            StreamItem::Stats(item) => {
                // TODO do something with the stats
                trace!("{:?}", item);
            }
        }
        Ok(())
    }

    fn result(&mut self) -> Result<BinnedCollectedResult, Error> {
        if let Some(mut binner) = self.binner.take() {
            if self.range_final {
                trace!("range_final");
                binner.set_range_complete();
            } else {
                debug!("not range_final");
            }
            if self.did_timeout {
                warn!("did_timeout");
            } else {
                trace!("not did_timeout");
                binner.cycle();
            }
            flush_binned(&mut binner, &mut self.coll, false)?;
            if self.coll.is_none() {
                debug!("force a bin");
                flush_binned(&mut binner, &mut self.coll, true)?;
            } else {
                trace!("coll is already some");
            }
        } else {
            error!("no binner, should always have one");
        }
        let result = match self.coll.take() {
            Some(mut coll) => {
                let res = coll
                    .result(None, Some(self.binrange.clone()))
                    .map_err(|e| format!("{e}"))?;
                res
            }
            None => {
                error!("binned_collected nothing collected");
                return Err(Error::from(format!("binned_collected nothing collected")));
            }
        };
        let ret = BinnedCollectedResult {
            range_final: self.range_final,
            did_timeout: self.did_timeout,
            result,
        };
        Ok(ret)
    }
}

impl Future for BinnedCollected {
    type Output = Result<BinnedCollectedResult, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let span = span!(Level::INFO, BinnedCollected::self_name());
        let _spg = span.enter();
        use Poll::*;
        loop {
            break match &self.state {
                BinnedCollectedState::Init => {
                    self.state = BinnedCollectedState::Run;
                    continue;
                }
                BinnedCollectedState::Run => match self.inp.poll_next_unpin(cx) {
                    Ready(Some(Ok(item))) => match self.handle_item(item) {
                        Ok(()) => continue,
                        Err(e) => {
                            self.state = BinnedCollectedState::Done;
                            Ready(Err(e))
                        }
                    },
                    Ready(Some(Err(e))) => {
                        self.state = BinnedCollectedState::Done;
                        Ready(Err(e.into()))
                    }
                    Ready(None) => {
                        self.state = BinnedCollectedState::Done;
                        Ready(self.result())
                    }
                    Pending => Pending,
                },
                BinnedCollectedState::Done => Ready(Err(Error::from(format!("already done")))),
            };
        }
    }
}
