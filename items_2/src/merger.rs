use crate::Error;
use futures_util::{Stream, StreamExt};
use items::sitem_data;
use items::{RangeCompletableItem, Sitemty, StreamItem};
use netpod::log::*;
use std::collections::VecDeque;
use std::fmt;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::{Context, Poll};

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => ();
    ($($arg:tt)*) => (trace!($($arg)*));
}

#[allow(unused)]
macro_rules! trace3 {
    ($($arg:tt)*) => ();
    ($($arg:tt)*) => (trace!($($arg)*));
}

#[allow(unused)]
macro_rules! trace4 {
    ($($arg:tt)*) => ();
    ($($arg:tt)*) => (trace!($($arg)*));
}

#[derive(Debug)]
pub enum MergeError {
    NotCompatible,
    Full,
}

impl From<MergeError> for err::Error {
    fn from(e: MergeError) -> Self {
        format!("{e:?}").into()
    }
}

pub trait Mergeable<Rhs = Self>: fmt::Debug + Unpin {
    fn len(&self) -> usize;
    fn ts_min(&self) -> Option<u64>;
    fn ts_max(&self) -> Option<u64>;
    // TODO remove, useless.
    fn is_compatible_target(&self, tgt: &Rhs) -> bool;

    // TODO rename to `append_*` to make it clear that they simply append, but not re-sort.
    fn move_into_fresh(&mut self, ts_end: u64) -> Rhs;
    fn move_into_existing(&mut self, tgt: &mut Rhs, ts_end: u64) -> Result<(), MergeError>;
}

type MergeInp<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;

pub struct Merger<T> {
    inps: Vec<Option<MergeInp<T>>>,
    items: Vec<Option<T>>,
    out: Option<T>,
    do_clear_out: bool,
    out_max_len: usize,
    range_complete: Vec<bool>,
    out_of_band_queue: VecDeque<Sitemty<T>>,
    done_data: bool,
    done_buffered: bool,
    done_range_complete: bool,
    complete: bool,
}

impl<T> fmt::Debug for Merger<T>
where
    T: Mergeable,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let inps: Vec<_> = self.inps.iter().map(|x| x.is_some()).collect();
        fmt.debug_struct(std::any::type_name::<Self>())
            .field("inps", &inps)
            .field("items", &self.items)
            .field("out_max_len", &self.out_max_len)
            .field("range_complete", &self.range_complete)
            .field("out_of_band_queue", &self.out_of_band_queue.len())
            .field("done_data", &self.done_data)
            .field("done_buffered", &self.done_buffered)
            .field("done_range_complete", &self.done_range_complete)
            .finish()
    }
}

impl<T> Merger<T>
where
    T: Mergeable,
{
    pub fn new(inps: Vec<MergeInp<T>>, out_max_len: usize) -> Self {
        let n = inps.len();
        Self {
            inps: inps.into_iter().map(|x| Some(x)).collect(),
            items: (0..n).into_iter().map(|_| None).collect(),
            out: None,
            do_clear_out: false,
            out_max_len,
            range_complete: vec![false; n],
            out_of_band_queue: VecDeque::new(),
            done_data: false,
            done_buffered: false,
            done_range_complete: false,
            complete: false,
        }
    }

    fn take_into_output_all(&mut self, src: &mut T) -> Result<(), MergeError> {
        // TODO optimize the case when some large batch should be added to some existing small batch already in out.
        // TODO maybe use two output slots?
        self.take_into_output_upto(src, u64::MAX)
    }

    fn take_into_output_upto(&mut self, src: &mut T, upto: u64) -> Result<(), MergeError> {
        // TODO optimize the case when some large batch should be added to some existing small batch already in out.
        // TODO maybe use two output slots?
        if self.out.is_none() {
            trace2!("move into fresh");
            self.out = Some(src.move_into_fresh(upto));
            Ok(())
        } else {
            let out = self.out.as_mut().unwrap();
            src.move_into_existing(out, upto)
        }
    }

    fn process(mut self: Pin<&mut Self>, _cx: &mut Context) -> Result<ControlFlow<()>, Error> {
        use ControlFlow::*;
        trace4!("process");
        let mut tslows = [None, None];
        for (i1, itemopt) in self.items.iter_mut().enumerate() {
            if let Some(item) = itemopt {
                if let Some(t1) = item.ts_min() {
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
                    // the item seems empty.
                    trace2!("empty item, something to do here?");
                    *itemopt = None;
                    return Ok(Continue(()));
                }
            }
        }
        trace4!("tslows {tslows:?}");
        if let Some((il0, _tl0)) = tslows[0] {
            if let Some((_il1, tl1)) = tslows[1] {
                // There is a second input, take only up to the second highest timestamp
                let item = self.items[il0].as_mut().unwrap();
                if let Some(th0) = item.ts_max() {
                    if th0 <= tl1 {
                        // Can take the whole item
                        let mut item = self.items[il0].take().unwrap();
                        trace3!("Take all from item {item:?}");
                        match self.take_into_output_all(&mut item) {
                            Ok(()) => Ok(Break(())),
                            Err(MergeError::Full) | Err(MergeError::NotCompatible) => {
                                // TODO count for stats
                                trace3!("Put item back");
                                self.items[il0] = Some(item);
                                self.do_clear_out = true;
                                Ok(Break(()))
                            }
                        }
                    } else {
                        // Take only up to the lowest ts of the second-lowest input
                        let mut item = self.items[il0].take().unwrap();
                        trace3!("Take up to {tl1} from item {item:?}");
                        match self.take_into_output_upto(&mut item, tl1) {
                            Ok(()) => {
                                if item.len() == 0 {
                                    // TODO should never be here because we should have taken the whole item
                                    Err(format!("Should have taken the whole item instead").into())
                                } else {
                                    self.items[il0] = Some(item);
                                    Ok(Break(()))
                                }
                            }
                            Err(MergeError::Full) | Err(MergeError::NotCompatible) => {
                                // TODO count for stats
                                trace3!("Put item back");
                                self.items[il0] = Some(item);
                                self.do_clear_out = true;
                                Ok(Break(()))
                            }
                        }
                    }
                } else {
                    // TODO should never be here because ts-max should always exist here.
                    Err(format!("selected input without max ts").into())
                }
            } else {
                // No other input, take the whole item
                let mut item = self.items[il0].take().unwrap();
                trace3!("Take all from item (no other input) {item:?}");
                match self.take_into_output_all(&mut item) {
                    Ok(()) => Ok(Break(())),
                    Err(_) => {
                        // TODO count for stats
                        trace3!("Put item back");
                        self.items[il0] = Some(item);
                        self.do_clear_out = true;
                        Ok(Break(()))
                    }
                }
            }
        } else {
            Err(format!("after low ts search nothing found").into())
        }
    }

    fn refill(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<Poll<()>, Error> {
        trace4!("refill");
        use Poll::*;
        let mut has_pending = false;
        for i in 0..self.inps.len() {
            if self.items[i].is_none() {
                while let Some(inp) = self.inps[i].as_mut() {
                    match inp.poll_next_unpin(cx) {
                        Ready(Some(Ok(k))) => match k {
                            StreamItem::DataItem(k) => match k {
                                RangeCompletableItem::Data(k) => {
                                    self.items[i] = Some(k);
                                    trace4!("refilled {}", i);
                                }
                                RangeCompletableItem::RangeComplete => {
                                    self.range_complete[i] = true;
                                    debug!("Merger  range_complete {:?}", self.range_complete);
                                    continue;
                                }
                            },
                            StreamItem::Log(item) => {
                                // TODO limit queue length
                                self.out_of_band_queue.push_back(Ok(StreamItem::Log(item)));
                                continue;
                            }
                            StreamItem::Stats(item) => {
                                // TODO limit queue length
                                self.out_of_band_queue.push_back(Ok(StreamItem::Stats(item)));
                                continue;
                            }
                        },
                        Ready(Some(Err(e))) => {
                            self.inps[i] = None;
                            return Err(e.into());
                        }
                        Ready(None) => {
                            self.inps[i] = None;
                        }
                        Pending => {
                            has_pending = true;
                        }
                    }
                    break;
                }
            }
        }
        if has_pending {
            Ok(Pending)
        } else {
            Ok(Ready(()))
        }
    }

    fn poll3(mut self: Pin<&mut Self>, cx: &mut Context) -> ControlFlow<Poll<Option<Result<T, Error>>>> {
        use ControlFlow::*;
        use Poll::*;
        #[allow(unused)]
        let ninps = self.inps.iter().filter(|a| a.is_some()).count();
        let nitems = self.items.iter().filter(|a| a.is_some()).count();
        let nitemsmissing = self
            .inps
            .iter()
            .zip(self.items.iter())
            .filter(|(a, b)| a.is_some() && b.is_none())
            .count();
        trace3!("ninps {ninps}  nitems {nitems}  nitemsmissing {nitemsmissing}");
        if nitemsmissing != 0 {
            let e = Error::from(format!("missing but no pending"));
            Break(Ready(Some(Err(e))))
        } else if nitems == 0 {
            Break(Ready(None))
        } else {
            match Self::process(Pin::new(&mut self), cx) {
                Ok(Break(())) => {
                    if let Some(o) = self.out.as_ref() {
                        // A good threshold varies according to scalar type and shape.
                        // TODO replace this magic number by a bound on the bytes estimate.
                        if o.len() >= self.out_max_len || self.do_clear_out {
                            trace3!("decide to output");
                            self.do_clear_out = false;
                            Break(Ready(Some(Ok(self.out.take().unwrap()))))
                        } else {
                            trace4!("output not yet");
                            Continue(())
                        }
                    } else {
                        trace3!("no output candidate");
                        Continue(())
                    }
                }
                Ok(Continue(())) => {
                    trace2!("process returned with Continue");
                    Continue(())
                }
                Err(e) => Break(Ready(Some(Err(e)))),
            }
        }
    }

    fn poll2(mut self: Pin<&mut Self>, cx: &mut Context) -> ControlFlow<Poll<Option<Result<T, Error>>>> {
        use ControlFlow::*;
        use Poll::*;
        match Self::refill(Pin::new(&mut self), cx) {
            Ok(Ready(())) => Self::poll3(self, cx),
            Ok(Pending) => Break(Pending),
            Err(e) => Break(Ready(Some(Err(e)))),
        }
    }
}

impl<T> Stream for Merger<T>
where
    T: Mergeable,
{
    type Item = Sitemty<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let span = span!(Level::TRACE, "merger");
        let _spanguard = span.enter();
        loop {
            trace3!("poll");
            break if self.complete {
                panic!("poll after complete");
            } else if self.done_range_complete {
                self.complete = true;
                Ready(None)
            } else if self.done_buffered {
                self.done_range_complete = true;
                if self.range_complete.iter().all(|x| *x) {
                    trace!("emit RangeComplete");
                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                } else {
                    continue;
                }
            } else if self.done_data {
                self.done_buffered = true;
                if let Some(out) = self.out.take() {
                    Ready(Some(sitem_data(out)))
                } else {
                    continue;
                }
            } else if let Some(item) = self.out_of_band_queue.pop_front() {
                trace4!("emit out-of-band");
                Ready(Some(item))
            } else {
                match Self::poll2(self.as_mut(), cx) {
                    ControlFlow::Continue(()) => continue,
                    ControlFlow::Break(k) => match k {
                        Ready(Some(Ok(item))) => Ready(Some(sitem_data(item))),
                        Ready(Some(Err(e))) => {
                            self.done_data = true;
                            Ready(Some(Err(e.into())))
                        }
                        Ready(None) => {
                            self.done_data = true;
                            continue;
                        }
                        Pending => Pending,
                    },
                }
            };
        }
    }
}
