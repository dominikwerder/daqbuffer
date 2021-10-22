use crate::EventsItem;
use futures_core::Stream;
use items::Sitemty;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct DataStream {}

impl Stream for DataStream {
    type Item = Sitemty<EventsItem>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let _ = self;
        let _ = cx;
        todo!()
    }
}
