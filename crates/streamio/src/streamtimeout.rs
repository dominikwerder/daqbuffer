use futures_util::Stream;
use std::pin::Pin;
use streams::streamtimeout::TimeoutableStream;

pub struct StreamTimeout {}

impl StreamTimeout {
    pub fn new() -> Self {
        Self {}
    }
}

impl<S> streams::streamtimeout::StreamTimeout<S> for StreamTimeout {
    fn timeout_intervals(&self, inp: Pin<Box<dyn Stream<Item = S> + Send>>) -> Pin<Box<dyn Stream<Item = S> + Send>> {
        todo!()
    }
}

impl<S> streams::streamtimeout::StreamTimeout2<S> for StreamTimeout {
    fn timeout_intervals(&self, inp: S) -> TimeoutableStream<S> {
        todo!()
    }
}
