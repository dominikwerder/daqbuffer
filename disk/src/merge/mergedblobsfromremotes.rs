use crate::eventchunker::EventFull;
use crate::merge::MergedStream;
use crate::raw::client::x_processed_event_blobs_stream_from_node;
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, StreamExt};
use items::Sitemty;
use netpod::query::RawEventsQuery;
use netpod::{log::*, NanoRange};
use netpod::{Cluster, PerfOpts};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

type T001<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;
type T002<T> = Pin<Box<dyn Future<Output = Result<T001<T>, Error>> + Send>>;

pub struct MergedBlobsFromRemotes {
    tcp_establish_futs: Vec<T002<EventFull>>,
    nodein: Vec<Option<T001<EventFull>>>,
    merged: Option<T001<EventFull>>,
    completed: bool,
    errored: bool,
    range: NanoRange,
    expand: bool,
}

impl MergedBlobsFromRemotes {
    pub fn new(evq: RawEventsQuery, perf_opts: PerfOpts, cluster: Cluster) -> Self {
        info!("MergedBlobsFromRemotes  evq {:?}", evq);
        let mut tcp_establish_futs = vec![];
        for node in &cluster.nodes {
            let f = x_processed_event_blobs_stream_from_node(evq.clone(), perf_opts.clone(), node.clone());
            let f: T002<EventFull> = Box::pin(f);
            tcp_establish_futs.push(f);
        }
        let n = tcp_establish_futs.len();
        Self {
            tcp_establish_futs,
            nodein: (0..n).into_iter().map(|_| None).collect(),
            merged: None,
            completed: false,
            errored: false,
            range: evq.range.clone(),
            expand: evq.agg_kind.need_expand(),
        }
    }
}

impl Stream for MergedBlobsFromRemotes {
    type Item = Sitemty<EventFull>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("poll_next on completed");
            } else if self.errored {
                self.completed = true;
                return Ready(None);
            } else if let Some(fut) = &mut self.merged {
                match fut.poll_next_unpin(cx) {
                    Ready(Some(Ok(k))) => Ready(Some(Ok(k))),
                    Ready(Some(Err(e))) => {
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Ready(None) => {
                        self.completed = true;
                        Ready(None)
                    }
                    Pending => Pending,
                }
            } else {
                let mut pend = false;
                let mut c1 = 0;
                for i1 in 0..self.tcp_establish_futs.len() {
                    if self.nodein[i1].is_none() {
                        let f = &mut self.tcp_establish_futs[i1];
                        pin_mut!(f);
                        match f.poll(cx) {
                            Ready(Ok(k)) => {
                                self.nodein[i1] = Some(k);
                            }
                            Ready(Err(e)) => {
                                self.errored = true;
                                return Ready(Some(Err(e)));
                            }
                            Pending => {
                                pend = true;
                            }
                        }
                    } else {
                        c1 += 1;
                    }
                }
                if pend {
                    Pending
                } else {
                    if c1 == self.tcp_establish_futs.len() {
                        let inps: Vec<_> = self.nodein.iter_mut().map(|k| k.take().unwrap()).collect();
                        let s1 = MergedStream::new(inps, self.range.clone(), self.expand);
                        self.merged = Some(Box::pin(s1));
                    }
                    continue 'outer;
                }
            };
        }
    }
}
