use crate::agg::MinMaxAvgScalarBinBatch;
use crate::cache::{node_ix_for_patch, HttpBodyAsAsyncRead};
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::FrameType;
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, FutureExt};
#[allow(unused_imports)]
use netpod::log::*;
use netpod::{AggKind, Channel, NodeConfig, PreBinnedPatchCoord};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct PreBinnedValueFetchedStream {
    uri: http::Uri,
    resfut: Option<hyper::client::ResponseFuture>,
    res: Option<InMemoryFrameAsyncReadStream<HttpBodyAsAsyncRead>>,
}

impl PreBinnedValueFetchedStream {
    pub fn new(
        patch_coord: PreBinnedPatchCoord,
        channel: Channel,
        agg_kind: AggKind,
        node_config: Arc<NodeConfig>,
    ) -> Self {
        let nodeix = node_ix_for_patch(&patch_coord, &channel, &node_config.cluster);
        let node = &node_config.cluster.nodes[nodeix as usize];
        warn!("TODO defining property of a PreBinnedPatchCoord? patchlen + ix? binsize + patchix? binsize + patchsize + patchix?");
        // TODO encapsulate uri creation, how to express aggregation kind?
        let uri: hyper::Uri = format!(
            "http://{}:{}/api/1/prebinned?{}&channel_backend={}&channel_name={}&agg_kind={:?}",
            node.host,
            node.port,
            patch_coord.to_url_params_strings(),
            channel.backend,
            channel.name,
            agg_kind,
        )
        .parse()
        .unwrap();
        Self {
            uri,
            resfut: None,
            res: None,
        }
    }
}

// TODO use a newtype here to use a different FRAME_TYPE_ID compared to
// impl FrameType for BinnedBytesForHttpStreamFrame
pub type PreBinnedHttpFrame = Result<MinMaxAvgScalarBinBatch, Error>;

impl Stream for PreBinnedValueFetchedStream {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = PreBinnedHttpFrame;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if let Some(res) = self.res.as_mut() {
                pin_mut!(res);
                match res.poll_next(cx) {
                    Ready(Some(Ok(frame))) => {
                        assert!(frame.tyid() == <PreBinnedHttpFrame as FrameType>::FRAME_TYPE_ID);
                        match bincode::deserialize::<PreBinnedHttpFrame>(frame.buf()) {
                            Ok(item) => Ready(Some(item)),
                            Err(e) => Ready(Some(Err(e.into()))),
                        }
                    }
                    Ready(Some(Err(e))) => Ready(Some(Err(e.into()))),
                    Ready(None) => Ready(None),
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
                            Ready(Some(Err(e.into())))
                        }
                    },
                    Pending => Pending,
                }
            } else {
                let req = hyper::Request::builder()
                    .method(http::Method::GET)
                    .uri(&self.uri)
                    .body(hyper::Body::empty())?;
                let client = hyper::Client::new();
                info!("PreBinnedValueFetchedStream  START REQUEST FOR {:?}", req);
                self.resfut = Some(client.request(req));
                continue 'outer;
            };
        }
    }
}
