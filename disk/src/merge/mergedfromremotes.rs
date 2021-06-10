use crate::agg::streams::Appendable;
use crate::binned::{EventsNodeProcessor, PushableIndex};
use crate::frame::makeframe::FrameType;
use crate::merge::MergedStream2;
use crate::raw::{x_processed_stream_from_node2, EventsQuery};
use crate::Sitemty;
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, StreamExt};
use netpod::{Cluster, PerfOpts};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

type T001<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;
type T002<T> = Pin<Box<dyn Future<Output = Result<T001<T>, Error>> + Send>>;

pub struct MergedFromRemotes<ENP>
where
    ENP: EventsNodeProcessor,
{
    tcp_establish_futs: Vec<T002<<ENP as EventsNodeProcessor>::Output>>,
    nodein: Vec<Option<T001<<ENP as EventsNodeProcessor>::Output>>>,
    merged: Option<T001<<ENP as EventsNodeProcessor>::Output>>,
    completed: bool,
    errored: bool,
}

impl<ENP> MergedFromRemotes<ENP>
where
    ENP: EventsNodeProcessor + 'static,
    <ENP as EventsNodeProcessor>::Output: 'static,
    <ENP as EventsNodeProcessor>::Output: Unpin,
    Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType,
{
    pub fn new(evq: EventsQuery, perf_opts: PerfOpts, cluster: Cluster) -> Self {
        let mut tcp_establish_futs = vec![];
        for node in &cluster.nodes {
            let f = x_processed_stream_from_node2::<ENP>(evq.clone(), perf_opts.clone(), node.clone());
            let f: T002<<ENP as EventsNodeProcessor>::Output> = Box::pin(f);
            tcp_establish_futs.push(f);
        }
        let n = tcp_establish_futs.len();
        Self {
            tcp_establish_futs,
            nodein: (0..n).into_iter().map(|_| None).collect(),
            merged: None,
            completed: false,
            errored: false,
        }
    }
}

impl<ENP> Stream for MergedFromRemotes<ENP>
where
    ENP: EventsNodeProcessor + 'static,
    <ENP as EventsNodeProcessor>::Output: PushableIndex + Appendable,
{
    type Item = Sitemty<<ENP as EventsNodeProcessor>::Output>;

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
                        let s1 = MergedStream2::<_, ENP>::new(inps);
                        self.merged = Some(Box::pin(s1));
                    }
                    continue 'outer;
                }
            };
        }
    }
}
