use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::StreamItem;
use crate::binned::BinnedStreamKind;
use crate::cache::{node_ix_for_patch, HttpBodyAsAsyncRead, PreBinnedQuery};
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::{decode_frame, FrameType};
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, FutureExt};
use http::StatusCode;
use netpod::log::*;
use netpod::{NodeConfigCached, PerfOpts};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct PreBinnedScalarValueFetchedStream<BK>
where
    BK: BinnedStreamKind,
{
    uri: http::Uri,
    resfut: Option<hyper::client::ResponseFuture>,
    res: Option<InMemoryFrameAsyncReadStream<HttpBodyAsAsyncRead>>,
    errored: bool,
    completed: bool,
    stream_kind: BK,
}

impl<BK> PreBinnedScalarValueFetchedStream<BK>
where
    BK: BinnedStreamKind,
{
    pub fn new(query: &PreBinnedQuery, node_config: &NodeConfigCached, stream_kind: &BK) -> Result<Self, Error> {
        let nodeix = node_ix_for_patch(&query.patch, &query.channel, &node_config.node_config.cluster);
        let node = &node_config.node_config.cluster.nodes[nodeix as usize];
        let uri: hyper::Uri = format!(
            "http://{}:{}/api/4/prebinned?{}",
            node.host,
            node.port,
            query.make_query_string()
        )
        .parse()?;
        let ret = Self {
            uri,
            resfut: None,
            res: None,
            errored: false,
            completed: false,
            stream_kind: stream_kind.clone(),
        };
        Ok(ret)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PreBinnedScalarItem {
    Batch(MinMaxAvgScalarBinBatch),
    RangeComplete,
}

impl<BK> Stream for PreBinnedScalarValueFetchedStream<BK>
where
    BK: BinnedStreamKind,
    Result<StreamItem<<BK as BinnedStreamKind>::PreBinnedItem>, err::Error>: FrameType,
{
    type Item = Result<StreamItem<BK::PreBinnedItem>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("poll_next on completed");
            } else if self.errored {
                self.completed = true;
                return Ready(None);
            } else if let Some(res) = self.res.as_mut() {
                pin_mut!(res);
                match res.poll_next(cx) {
                    Ready(Some(Ok(item))) => match item {
                        StreamItem::Log(item) => Ready(Some(Ok(StreamItem::Log(item)))),
                        StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                        StreamItem::DataItem(item) => {
                            match decode_frame::<Result<StreamItem<<BK as BinnedStreamKind>::PreBinnedItem>, Error>>(
                                &item,
                            ) {
                                Ok(Ok(item)) => Ready(Some(Ok(item))),
                                Ok(Err(e)) => {
                                    self.errored = true;
                                    Ready(Some(Err(e)))
                                }
                                Err(e) => {
                                    self.errored = true;
                                    Ready(Some(Err(e)))
                                }
                            }
                        }
                    },
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
            } else if let Some(resfut) = self.resfut.as_mut() {
                match resfut.poll_unpin(cx) {
                    Ready(res) => match res {
                        Ok(res) => {
                            if res.status() == StatusCode::OK {
                                let perf_opts = PerfOpts { inmem_bufcap: 512 };
                                let s1 = HttpBodyAsAsyncRead::new(res);
                                let s2 = InMemoryFrameAsyncReadStream::new(s1, perf_opts.inmem_bufcap);
                                self.res = Some(s2);
                                continue 'outer;
                            } else {
                                error!(
                                    "PreBinnedValueFetchedStream  got non-OK result from sub request: {:?}",
                                    res
                                );
                                let e = Error::with_msg(format!(
                                    "PreBinnedValueFetchedStream  got non-OK result from sub request: {:?}",
                                    res
                                ));
                                self.errored = true;
                                Ready(Some(Err(e)))
                            }
                        }
                        Err(e) => {
                            error!("PreBinnedValueStream  error in stream {:?}", e);
                            self.errored = true;
                            Ready(Some(Err(e.into())))
                        }
                    },
                    Pending => Pending,
                }
            } else {
                match hyper::Request::builder()
                    .method(http::Method::GET)
                    .uri(&self.uri)
                    .body(hyper::Body::empty())
                {
                    Ok(req) => {
                        let client = hyper::Client::new();
                        self.resfut = Some(client.request(req));
                        continue 'outer;
                    }
                    Err(e) => {
                        self.errored = true;
                        Ready(Some(Err(e.into())))
                    }
                }
            };
        }
    }
}
