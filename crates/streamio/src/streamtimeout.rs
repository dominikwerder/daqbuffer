#[cfg(test)]
mod test;

use std::time::Duration;
use streams::streamtimeout::BoxedTimeoutFuture;
use streams::streamtimeout::StreamTimeout2;

pub struct StreamTimeout {}

impl StreamTimeout {
    pub fn new() -> Self {
        Self {}
    }

    pub fn boxed() -> Box<dyn StreamTimeout2> {
        Box::new(Self::new())
    }
}

impl StreamTimeout2 for StreamTimeout {
    fn timeout_intervals(&self, ivl: Duration) -> BoxedTimeoutFuture {
        Box::pin(tokio::time::sleep(ivl))
    }
}
