use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::cache::{node_ix_for_patch, HttpBodyAsAsyncRead, PreBinnedQuery};
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::decode_frame;
use crate::streamlog::LogItem;
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, FutureExt};
use http::StatusCode;
#[allow(unused_imports)]
use netpod::log::*;
use netpod::{EventDataReadStats, NodeConfigCached};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct PreBinnedValueFetchedStream {
    uri: http::Uri,
    resfut: Option<hyper::client::ResponseFuture>,
    res: Option<InMemoryFrameAsyncReadStream<HttpBodyAsAsyncRead>>,
    errored: bool,
    completed: bool,
}

impl PreBinnedValueFetchedStream {
    pub fn new(query: &PreBinnedQuery, node_config: &NodeConfigCached) -> Result<Self, Error> {
        let nodeix = node_ix_for_patch(&query.patch, &query.channel, &node_config.node_config.cluster);
        let node = &node_config.node_config.cluster.nodes[nodeix as usize];
        let uri: hyper::Uri = format!(
            "http://{}:{}/api/1/prebinned?{}",
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
        };
        Ok(ret)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PreBinnedItem {
    Batch(MinMaxAvgScalarBinBatch),
    RangeComplete,
    EventDataReadStats(EventDataReadStats),
    //ValuesExtractStats(ValuesExtractStats),
    Log(LogItem),
}

impl Stream for PreBinnedValueFetchedStream {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = Result<PreBinnedItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        'outer: loop {
            break if let Some(res) = self.res.as_mut() {
                pin_mut!(res);
                match res.poll_next(cx) {
                    Ready(Some(Ok(frame))) => match decode_frame::<Result<PreBinnedItem, Error>>(&frame) {
                        Ok(Ok(item)) => {
                            match &item {
                                PreBinnedItem::EventDataReadStats(stats) => {
                                    info!("PreBinnedValueFetchedStream  ✕ ✕ ✕ ✕ ✕ ✕ ✕ ✕ ✕  stats {:?}", stats);
                                }
                                _ => {}
                            }
                            Ready(Some(Ok(item)))
                        }
                        Ok(Err(e)) => {
                            self.errored = true;
                            Ready(Some(Err(e)))
                        }
                        Err(e) => {
                            self.errored = true;
                            Ready(Some(Err(e)))
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
                                let s1 = HttpBodyAsAsyncRead::new(res);
                                let s2 = InMemoryFrameAsyncReadStream::new(s1);
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
