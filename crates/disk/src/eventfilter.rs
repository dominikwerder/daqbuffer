use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::WithLen;
use items_2::eventfull::EventFull;
use netpod::Shape;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tracing::Level;

pub struct EventFullShapeFilter<INP> {
    inp: INP,
    shape_exp: Shape,
    node_ix: usize,
    log_items: VecDeque<LogItem>,
}

impl<INP> EventFullShapeFilter<INP> {
    fn filter_item(&mut self, item: &mut EventFull) {
        let node_ix = self.node_ix;
        let p: Vec<_> = (0..item.len())
            .map(|i| {
                let sh = item.shape_derived(i, &self.shape_exp);
                match sh {
                    Ok(sh) => {
                        if sh.ne(&self.shape_exp) {
                            let msg = format!("shape_derived mismatch  {:?}  {:?}", sh, self.shape_exp);
                            let item = LogItem::from_node(node_ix, Level::WARN, msg);
                            self.log_items.push_back(item);
                            false
                        } else {
                            true
                        }
                    }
                    Err(_) => {
                        let msg = format!("shape_derived mismatch  {:?}  {:?}", sh, self.shape_exp);
                        let item = LogItem::from_node(self.node_ix, Level::WARN, msg);
                        self.log_items.push_back(item);
                        false
                    }
                }
            })
            .collect();
        item.keep_ixs(&p);
    }
}

impl<INP> Stream for EventFullShapeFilter<INP>
where
    INP: Stream<Item = Sitemty<EventFull>> + Unpin,
{
    type Item = Sitemty<EventFull>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if let Some(item) = self.log_items.pop_front() {
            Ready(Some(Ok(StreamItem::Log(item))))
        } else {
            match self.inp.poll_next_unpin(cx) {
                Ready(Some(item)) => match item {
                    Ok(StreamItem::DataItem(RangeCompletableItem::Data(mut item))) => {
                        self.filter_item(&mut item);
                        Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))))
                    }
                    x => Ready(Some(x)),
                },
                Ready(None) => Ready(None),
                Pending => Pending,
            }
        }
    }
}
