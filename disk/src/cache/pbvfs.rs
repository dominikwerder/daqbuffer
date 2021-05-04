use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::cache::{node_ix_for_patch, HttpBodyAsAsyncRead, PreBinnedQuery};
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::decode_frame;
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, FutureExt};
#[allow(unused_imports)]
use netpod::log::*;
use netpod::NodeConfigCached;
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
        warn!("TODO defining property of a PreBinnedPatchCoord? patchlen + ix? binsize + patchix? binsize + patchsize + patchix?");
        let uri: hyper::Uri = format!(
            "http://{}:{}/api/1/prebinned?{}",
            node.host,
            node.port,
            query.make_query_string()
        )
        .parse()?;
        info!("PreBinnedValueFetchedStream  open uri  {}", uri);
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

#[derive(Serialize, Deserialize)]
pub enum PreBinnedItem {
    Batch(MinMaxAvgScalarBinBatch),
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
                        Ok(Ok(item)) => Ready(Some(Ok(item))),
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
                            info!("PreBinnedValueFetchedStream  GOT result from SUB REQUEST: {:?}", res);
                            let s1 = HttpBodyAsAsyncRead::new(res);
                            let s2 = InMemoryFrameAsyncReadStream::new(s1);
                            self.res = Some(s2);
                            continue 'outer;
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
                        info!("PreBinnedValueFetchedStream  START REQUEST FOR {:?}", req);
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
