use crate::{ChannelEvents, Error, MergableEvents};
use futures_util::{Stream, StreamExt};
use items::{RangeCompletableItem, Sitemty, StreamItem};
use netpod::log::*;
use std::fmt;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::{Context, Poll};

type MergeInp = Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>;

pub struct ChannelEventsMerger {
    inps: Vec<Option<MergeInp>>,
    items: Vec<Option<ChannelEvents>>,
    range_complete: bool,
    done: bool,
    done2: bool,
    complete: bool,
}

impl fmt::Debug for ChannelEventsMerger {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let inps: Vec<_> = self.inps.iter().map(|x| x.is_some()).collect();
        fmt.debug_struct(std::any::type_name::<Self>())
            .field("inps", &inps)
            .field("items", &self.items)
            .field("range_complete", &self.range_complete)
            .field("done", &self.done)
            .field("done2", &self.done2)
            .finish()
    }
}

impl ChannelEventsMerger {
    pub fn new(inps: Vec<MergeInp>) -> Self {
        let n = inps.len();
        Self {
            done: false,
            done2: false,
            complete: false,
            inps: inps.into_iter().map(|x| Some(x)).collect(),
            items: (0..n).into_iter().map(|_| None).collect(),
            range_complete: false,
        }
    }

    fn process(mut self: Pin<&mut Self>, _cx: &mut Context) -> Result<ControlFlow<ChannelEvents>, Error> {
        use ControlFlow::*;
        let mut tslows = [None, None];
        for (i1, itemopt) in self.items.iter_mut().enumerate() {
            if let Some(item) = itemopt {
                let t1 = item.ts_min();
                if let Some(t1) = t1 {
                    if let Some((_, a)) = tslows[0] {
                        if t1 < a {
                            tslows[1] = tslows[0];
                            tslows[0] = Some((i1, t1));
                        } else {
                            if let Some((_, b)) = tslows[1] {
                                if t1 < b {
                                    tslows[1] = Some((i1, t1));
                                } else {
                                    // nothing to do
                                }
                            } else {
                                tslows[1] = Some((i1, t1));
                            }
                        }
                    } else {
                        tslows[0] = Some((i1, t1));
                    }
                } else {
                    match item {
                        ChannelEvents::Events(_) => {
                            trace!("events item without ts min discovered {item:?}");
                            itemopt.take();
                            return Ok(Continue(()));
                        }
                        ChannelEvents::Status(_) => {
                            return Err(format!("channel status without timestamp").into());
                        }
                    }
                }
            }
        }
        if let Some((il0, _tl0)) = tslows[0] {
            if let Some((_il1, tl1)) = tslows[1] {
                let item = self.items[il0].as_mut().unwrap();
                match item {
                    ChannelEvents::Events(item) => {
                        if let Some(th0) = item.ts_max() {
                            if th0 < tl1 {
                                let ret = self.items[il0].take().unwrap();
                                Ok(Break(ret))
                            } else {
                                let ritem = item.take_new_events_until_ts(tl1);
                                if item.len() == 0 {
                                    // TODO should never be here
                                    self.items[il0] = None;
                                }
                                Ok(Break(ChannelEvents::Events(ritem)))
                            }
                        } else {
                            // TODO should never be here because ts-max should always exist here.
                            let ritem = item.take_new_events_until_ts(tl1);
                            if item.len() == 0 {}
                            Ok(Break(ChannelEvents::Events(ritem)))
                        }
                    }
                    ChannelEvents::Status(_) => {
                        let ret = self.items[il0].take().unwrap();
                        Ok(Break(ret))
                    }
                }
            } else {
                let item = self.items[il0].as_mut().unwrap();
                match item {
                    ChannelEvents::Events(_) => {
                        let ret = self.items[il0].take().unwrap();
                        Ok(Break(ret))
                    }
                    ChannelEvents::Status(_) => {
                        let ret = self.items[il0].take().unwrap();
                        Ok(Break(ret))
                    }
                }
            }
        } else {
            Err(format!("after low ts search nothing found").into())
        }
    }

    fn refill(mut self: Pin<&mut Self>, cx: &mut Context) -> ControlFlow<Poll<Error>> {
        use ControlFlow::*;
        use Poll::*;
        let mut has_pending = false;
        for i1 in 0..self.inps.len() {
            let item = &self.items[i1];
            if item.is_none() {
                while let Some(inp) = &mut self.inps[i1] {
                    match inp.poll_next_unpin(cx) {
                        Ready(Some(Ok(k))) => {
                            match k {
                                StreamItem::DataItem(k) => match k {
                                    RangeCompletableItem::RangeComplete => {
                                        trace!("---------------------  ChannelEvents::RangeComplete \n======================");
                                        // TODO track range complete for all inputs, it's only complete if all inputs are complete.
                                        self.range_complete = true;
                                        eprintln!("TODO inp RangeComplete which does not fill slot");
                                    }
                                    RangeCompletableItem::Data(k) => {
                                        match &k {
                                            ChannelEvents::Events(events) => {
                                                if events.len() == 0 {
                                                    warn!("empty events item {events:?}");
                                                } else {
                                                    trace!(
                                                        "\nrefilled with events {}\nREFILLED\n{:?}\n\n",
                                                        events.len(),
                                                        events
                                                    );
                                                }
                                            }
                                            ChannelEvents::Status(_) => {
                                                eprintln!("TODO inp Status which does not fill slot");
                                            }
                                        }
                                        self.items[i1] = Some(k);
                                        break;
                                    }
                                },
                                StreamItem::Log(_) => {
                                    eprintln!("TODO inp Log which does not fill slot");
                                }
                                StreamItem::Stats(_) => {
                                    eprintln!("TODO inp Stats which does not fill slot");
                                }
                            }
                        }
                        Ready(Some(Err(e))) => return Break(Ready(e.into())),
                        Ready(None) => {
                            self.inps[i1] = None;
                        }
                        Pending => {
                            has_pending = true;
                        }
                    }
                }
            }
        }
        if has_pending {
            Break(Pending)
        } else {
            Continue(())
        }
    }

    fn poll2(mut self: Pin<&mut Self>, cx: &mut Context) -> ControlFlow<Poll<Option<Result<ChannelEvents, Error>>>> {
        use ControlFlow::*;
        use Poll::*;
        let mut has_pending = false;
        match Self::refill(Pin::new(&mut self), cx) {
            Break(Ready(e)) => return Break(Ready(Some(Err(e)))),
            Break(Pending) => {
                has_pending = true;
            }
            Continue(()) => {}
        }
        let ninps = self.inps.iter().filter(|a| a.is_some()).count();
        let nitems = self.items.iter().filter(|a| a.is_some()).count();
        let nitemsmissing = self
            .inps
            .iter()
            .zip(self.items.iter())
            .filter(|(a, b)| a.is_some() && b.is_none())
            .count();
        if ninps == 0 && nitems == 0 {
            self.done = true;
            Break(Ready(None))
        } else if nitemsmissing != 0 {
            if !has_pending {
                let e = Error::from(format!("missing but no pending"));
                Break(Ready(Some(Err(e))))
            } else {
                Break(Pending)
            }
        } else {
            match Self::process(Pin::new(&mut self), cx) {
                Ok(Break(item)) => Break(Ready(Some(Ok(item)))),
                Ok(Continue(())) => Continue(()),
                Err(e) => Break(Ready(Some(Err(e)))),
            }
        }
    }
}

impl Stream for ChannelEventsMerger {
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        const NAME: &str = "ChannelEventsMerger";
        let span = span!(Level::TRACE, NAME);
        let _spanguard = span.enter();
        loop {
            break if self.complete {
                panic!("poll after complete");
            } else if self.done2 {
                self.complete = true;
                Ready(None)
            } else if self.done {
                self.done2 = true;
                if self.range_complete {
                    trace!("MERGER EMITTING ChannelEvents::RangeComplete");
                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                } else {
                    continue;
                }
            } else {
                match Self::poll2(self.as_mut(), cx) {
                    ControlFlow::Continue(()) => continue,
                    ControlFlow::Break(k) => match k {
                        Ready(Some(Ok(ChannelEvents::Events(item)))) => {
                            trace!("\n\nMERGER EMITTING\n{:?}\n\n", item);
                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                                ChannelEvents::Events(item),
                            )))))
                        }
                        Ready(Some(Ok(ChannelEvents::Status(item)))) => {
                            trace!("\n\nMERGER EMITTING\n{:?}\n\n", item);
                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                                ChannelEvents::Status(item),
                            )))))
                        }
                        Ready(Some(Err(e))) => {
                            self.done = true;
                            Ready(Some(Err(e.into())))
                        }
                        Ready(None) => {
                            self.done = true;
                            continue;
                        }
                        Pending => Pending,
                    },
                }
            };
        }
    }
}
