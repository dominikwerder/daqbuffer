use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StatsItem;
use items_0::streamitem::StreamItem;
use items_0::MergeError;
use items_2::merger::Mergeable;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::RangeFilterStats;
use netpod::TsMsVecFmt;
use netpod::TsNano;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[cfg(test)]
use items_0::Events;
#[cfg(test)]
use std::collections::VecDeque;

#[allow(unused)]
macro_rules! trace_emit {
    ($det:expr, $($arg:tt)*) => {
        if $det {
            eprintln!($($arg)*);
        }
    };
}

pub struct RangeFilter2<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Mergeable,
{
    inp: S,
    range: NanoRange,
    range_str: String,
    one_before_range: bool,
    stats: RangeFilterStats,
    slot1: Option<ITY>,
    have_range_complete: bool,
    inp_done: bool,
    raco_done: bool,
    done: bool,
    complete: bool,
    trdet: bool,
}

impl<S, ITY> RangeFilter2<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Mergeable,
{
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(inp: S, range: NanoRange, one_before_range: bool) -> Self {
        let trdet = false;
        trace_emit!(
            trdet,
            "{}::new  range: {:?}  one_before_range {:?}",
            Self::type_name(),
            range,
            one_before_range
        );
        Self {
            inp,
            range_str: format!("{:?}", range),
            range,
            one_before_range,
            stats: RangeFilterStats::new(),
            slot1: None,
            have_range_complete: false,
            inp_done: false,
            raco_done: false,
            done: false,
            complete: false,
            trdet,
        }
    }

    fn prune_high(&mut self, mut item: ITY, ts: u64) -> Result<ITY, Error> {
        let ret = match item.find_highest_index_lt(ts) {
            Some(ihlt) => {
                let n = item.len();
                if ihlt + 1 == n {
                    // TODO gather stats, this should be the most common case.
                    self.stats.items_no_prune_high += 1;
                    item
                } else {
                    self.stats.items_part_prune_high += 1;
                    let mut dummy = item.new_empty();
                    match item.drain_into(&mut dummy, (ihlt + 1, n)) {
                        Ok(_) => {}
                        Err(e) => match e {
                            MergeError::NotCompatible => {
                                error!("logic error")
                            }
                            MergeError::Full => error!("full, logic error"),
                        },
                    }
                    item
                }
            }
            None => {
                self.stats.items_all_prune_high += 1;
                item.new_empty()
            }
        };
        Ok(ret)
    }

    fn handle_item(&mut self, item: ITY) -> Result<ITY, Error> {
        let min = item.ts_min().map(|x| TsNano::from_ns(x).fmt());
        let max = item.ts_max().map(|x| TsNano::from_ns(x).fmt());
        trace_emit!(
            self.trdet,
            "see event  len {}  min {:?}  max {:?}",
            item.len(),
            min,
            max
        );
        let mut item = self.prune_high(item, self.range.end)?;
        let ret = if self.one_before_range {
            let lige = item.find_lowest_index_ge(self.range.beg);
            trace_emit!(self.trdet, "YES one_before_range  ilge {:?}", lige);
            match lige {
                Some(lige) => {
                    if lige == 0 {
                        if let Some(sl1) = self.slot1.take() {
                            self.slot1 = Some(item);
                            sl1
                        } else {
                            item
                        }
                    } else {
                        trace_emit!(self.trdet, "discarding events  len {:?}", lige - 1);
                        let mut dummy = item.new_empty();
                        item.drain_into(&mut dummy, (0, lige - 1))
                            .map_err(|e| format!("{e} unexpected MergeError while remove of items"))?;
                        self.slot1 = None;
                        item
                    }
                }
                None => {
                    // TODO keep stats about this case
                    trace_emit!(self.trdet, "drain into to keep one before");
                    let n = item.len();
                    let mut keep = item.new_empty();
                    item.drain_into(&mut keep, (n.max(1) - 1, n))
                        .map_err(|e| format!("{e} unexpected MergeError while remove of items"))?;
                    self.slot1 = Some(keep);
                    item.new_empty()
                }
            }
        } else {
            let lige = item.find_lowest_index_ge(self.range.beg);
            trace_emit!(self.trdet, "NOT one_before_range  ilge {:?}", lige);
            match lige {
                Some(lige) => {
                    let mut dummy = item.new_empty();
                    item.drain_into(&mut dummy, (0, lige))?;
                    item
                }
                None => {
                    // TODO count case for stats
                    item.new_empty()
                }
            }
        };
        Ok(ret)
    }
}

impl<S, ITY> RangeFilter2<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Mergeable,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<<Self as Stream>::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                error!("{} poll_next on complete", Self::type_name());
                Ready(Some(Err(Error::with_msg_no_trace(format!(
                    "{} poll_next on complete",
                    Self::type_name()
                )))))
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if self.raco_done {
                self.done = true;
                let k = std::mem::replace(&mut self.stats, RangeFilterStats::new());
                let k = StatsItem::RangeFilterStats(k);
                Ready(Some(Ok(StreamItem::Stats(k))))
            } else if self.inp_done {
                self.raco_done = true;
                if self.have_range_complete {
                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                } else {
                    continue;
                }
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(item)) => match item {
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))) => match self.handle_item(item) {
                            Ok(item) => {
                                trace_emit!(self.trdet, "emit {}", TsMsVecFmt(Mergeable::tss(&item).iter()));
                                let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
                                Ready(Some(item))
                            }
                            Err(e) => {
                                error!("sees: {e}");
                                self.inp_done = true;
                                Ready(Some(Err(e)))
                            }
                        },
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)) => {
                            self.have_range_complete = true;
                            continue;
                        }
                        k => Ready(Some(k)),
                    },
                    Ready(None) => {
                        self.inp_done = true;
                        if let Some(sl1) = self.slot1.take() {
                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(sl1)))))
                        } else {
                            continue;
                        }
                    }
                    Pending => Pending,
                }
            };
        }
    }
}

impl<S, ITY> Stream for RangeFilter2<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Mergeable,
{
    type Item = Sitemty<ITY>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let span1 = span!(Level::INFO, "RangeFilter2", range = tracing::field::Empty);
        span1.record("range", &self.range_str.as_str());
        let _spg = span1.enter();
        RangeFilter2::poll_next(self, cx)
    }
}

impl<S, ITY> fmt::Debug for RangeFilter2<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Mergeable,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RangeFilter2").field("stats", &self.stats).finish()
    }
}

impl<S, ITY> Drop for RangeFilter2<S, ITY>
where
    S: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: Mergeable,
{
    fn drop(&mut self) {
        // Self::type_name()
        debug!("drop {:?}", self);
    }
}

#[test]
fn test_00() {
    use items_0::Empty;
    use items_2::eventsdim0::EventsDim0;
    let ms = 1_000_000;
    let beg = TsNano::from_ms(1000 * 10);
    let end = TsNano::from_ms(1000 * 20);
    let mut item1 = EventsDim0::<f32>::empty();
    item1.push_back(beg.ns() + 0 * ms, 0, 3.);
    item1.push_back(beg.ns() + 1 * ms, 0, 3.1);
    item1.push_back(beg.ns() + 2 * ms, 0, 3.2);
    item1.push_back(beg.ns() + 3 * ms, 0, 3.3);
    item1.push_back(beg.ns() + 4 * ms, 0, 3.4);
    item1.push_back(end.ns() - 1, 0, 4.0);
    item1.push_back(end.ns() + 0, 0, 4.1);
    item1.push_back(end.ns() + 1, 0, 4.1);
    let w1: Box<dyn Events> = Box::new(item1.clone());
    let e1 = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w1)));
    let inp = futures_util::stream::iter([e1]);
    let one_before_range = false;
    let range = NanoRange::from((beg.ns(), end.ns()));
    let stream = RangeFilter2::new(inp, range, one_before_range);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        let exp: &[&[u64]] = &[&[
            beg.ns() + 0 * ms,
            beg.ns() + 1 * ms,
            beg.ns() + 2 * ms,
            beg.ns() + 3 * ms,
            beg.ns() + 4 * ms,
            end.ns() - 1,
        ]];
        assert_eq!(&tss_items, &exp);
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}

#[test]
fn test_cut_before_00() {
    use items_0::Empty;
    use items_2::eventsdim0::EventsDim0;
    let ms = 1_000_000;
    let beg = TsNano::from_ms(1000 * 10);
    let end = TsNano::from_ms(1000 * 20);
    let mut items = Vec::new();
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() - 1, 0, 2.9);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() + 0 * ms, 0, 3.);
        item.push_back(beg.ns() + 1 * ms, 0, 3.1);
        item.push_back(beg.ns() + 2 * ms, 0, 3.2);
        item.push_back(beg.ns() + 3 * ms, 0, 3.3);
        item.push_back(beg.ns() + 4 * ms, 0, 3.4);
        item.push_back(end.ns() - 1, 0, 4.0);
        item.push_back(end.ns() + 0, 0, 4.1);
        item.push_back(end.ns() + 1, 0, 4.1);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    let inp = futures_util::stream::iter(items);
    let one_before_range = false;
    let range = NanoRange::from((beg.ns(), end.ns()));
    let stream = RangeFilter2::new(inp, range, one_before_range);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        let exp: &[&[u64]] = &[
            // TODO in the future this empty may be discarded
            &[],
            &[
                beg.ns() + 0 * ms,
                beg.ns() + 1 * ms,
                beg.ns() + 2 * ms,
                beg.ns() + 3 * ms,
                beg.ns() + 4 * ms,
                end.ns() - 1,
            ],
        ];
        assert_eq!(&tss_items, &exp);
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}

#[test]
fn test_one_before_00() {
    use items_0::Empty;
    use items_2::eventsdim0::EventsDim0;
    let ms = 1_000_000;
    let beg = TsNano::from_ms(1000 * 10);
    let end = TsNano::from_ms(1000 * 20);
    let mut items = Vec::new();
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() - 1, 0, 2.9);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() + 0 * ms, 0, 3.);
        item.push_back(beg.ns() + 1 * ms, 0, 3.1);
        item.push_back(beg.ns() + 2 * ms, 0, 3.2);
        item.push_back(beg.ns() + 3 * ms, 0, 3.3);
        item.push_back(beg.ns() + 4 * ms, 0, 3.4);
        item.push_back(end.ns() - 1, 0, 4.0);
        item.push_back(end.ns() + 0, 0, 4.1);
        item.push_back(end.ns() + 1, 0, 4.1);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    let inp = futures_util::stream::iter(items);
    let one_before_range = true;
    let range = NanoRange::from((beg.ns(), end.ns()));
    let stream = RangeFilter2::new(inp, range, one_before_range);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        let exp: &[&[u64]] = &[
            // TODO in the future this empty may be discarded
            &[],
            &[
                //
                beg.ns() - 1,
            ],
            &[
                beg.ns() + 0 * ms,
                beg.ns() + 1 * ms,
                beg.ns() + 2 * ms,
                beg.ns() + 3 * ms,
                beg.ns() + 4 * ms,
                end.ns() - 1,
            ],
        ];
        assert_eq!(&tss_items, &exp);
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}

#[test]
fn test_one_before_01() {
    use items_0::Empty;
    use items_2::eventsdim0::EventsDim0;
    let ms = 1_000_000;
    let beg = TsNano::from_ms(1000 * 10);
    let end = TsNano::from_ms(1000 * 20);
    let mut items = Vec::new();
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() - 1, 0, 2.9);
        item.push_back(beg.ns() + 0 * ms, 0, 3.);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() + 1 * ms, 0, 3.1);
        item.push_back(beg.ns() + 2 * ms, 0, 3.2);
        item.push_back(beg.ns() + 3 * ms, 0, 3.3);
        item.push_back(beg.ns() + 4 * ms, 0, 3.4);
        item.push_back(end.ns() - 1, 0, 4.0);
        item.push_back(end.ns() + 0, 0, 4.1);
        item.push_back(end.ns() + 1, 0, 4.1);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    let inp = futures_util::stream::iter(items);
    let one_before_range = true;
    let range = NanoRange::from((beg.ns(), end.ns()));
    let stream = RangeFilter2::new(inp, range, one_before_range);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        let exp: &[&[u64]] = &[
            // TODO in the future this empty may be discarded
            // &[],
            &[
                //
                beg.ns() - 1,
                beg.ns() + 0 * ms,
            ],
            &[
                beg.ns() + 1 * ms,
                beg.ns() + 2 * ms,
                beg.ns() + 3 * ms,
                beg.ns() + 4 * ms,
                end.ns() - 1,
            ],
        ];
        assert_eq!(&tss_items, &exp);
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}

#[test]
fn test_one_before_only() {
    use items_0::Empty;
    use items_2::eventsdim0::EventsDim0;
    let _ms = 1_000_000;
    let beg = TsNano::from_ms(1000 * 10);
    let end = TsNano::from_ms(1000 * 20);
    let mut items = Vec::new();
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() - 1, 0, 2.9);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    let inp = futures_util::stream::iter(items);
    let one_before_range = true;
    let range = NanoRange::from((beg.ns(), end.ns()));
    let stream = RangeFilter2::new(inp, range, one_before_range);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        let exp: &[&[u64]] = &[
            // TODO in the future this empty may be discarded
            &[],
            &[
                //
                beg.ns() - 1,
            ],
        ];
        assert_eq!(&tss_items, &exp);
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}

#[cfg(test)]
async fn fetch_into_tss_items<INP>(mut inp: INP) -> VecDeque<VecDeque<u64>>
where
    INP: Stream<Item = Sitemty<Box<dyn Events>>> + Unpin,
{
    let mut tss_items = VecDeque::new();
    while let Some(e) = inp.next().await {
        if let Ok(StreamItem::DataItem(RangeCompletableItem::Data(evs))) = e {
            eprintln!("{:?}", evs);
            tss_items.push_back(Events::tss(&evs).clone());
        } else {
            eprintln!("other item ----------: {:?}", e);
        }
    }
    tss_items
}
