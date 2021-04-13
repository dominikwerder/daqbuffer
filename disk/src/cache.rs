#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use std::future::ready;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures_core::Stream;
use futures_util::{StreamExt, FutureExt, pin_mut};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use netpod::{Node, Cluster, AggKind, NanoRange, ToNanos, PreBinnedPatchGridSpec, PreBinnedPatchIterator, PreBinnedPatchCoord, Channel};
use crate::agg::MinMaxAvgScalarBinBatch;
use http::uri::Scheme;
use tiny_keccak::Hasher;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Query {
    range: NanoRange,
    count: u64,
    agg_kind: AggKind,
    channel: Channel,
}

impl Query {

    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let beg_date = params.get("beg_date").ok_or(Error::with_msg("missing beg_date"))?;
        let end_date = params.get("end_date").ok_or(Error::with_msg("missing end_date"))?;
        let ret = Query {
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            count: params.get("bin_count").ok_or(Error::with_msg("missing beg_date"))?.parse().unwrap(),
            agg_kind: AggKind::DimXBins1,
            channel: Channel {
                backend: params.get("channel_backend").unwrap().into(),
                keyspace: params.get("channel_keyspace").unwrap().parse().unwrap(),
                name: params.get("channel_name").unwrap().into(),
            },
        };
        info!("Query::from_request  {:?}", ret);
        Ok(ret)
    }

}


pub struct BinParams {
    pub node: Node,
    pub cluster: Cluster,
}

pub fn binned_bytes_for_http(params: BinParams, query: &Query) -> Result<BinnedBytesForHttpStream, Error> {

    // TODO
    // Translate the Query TimeRange + AggKind into an iterator over the pre-binned patches.
    let grid = PreBinnedPatchGridSpec::over_range(query.range.clone(), query.count);
    match grid {
        Some(spec) => {
            info!("GOT  PreBinnedPatchGridSpec:    {:?}", spec);
            warn!("Pass here to BinnedStream what kind of Agg, range, ...");
            let s1 = BinnedStream::new(PreBinnedPatchIterator::from_range(spec), query.channel.clone(), params.cluster.clone());
            // Iterate over the patches.
            // Request the patch from each node.
            // Merge.
            // Agg+Bin.
            // Deliver.
            let ret = BinnedBytesForHttpStream {
                inp: s1,
            };
            Ok(ret)
        }
        None => {
            // Merge raw data
            todo!()
        }
    }
}


pub struct BinnedBytesForHttpStream {
    inp: BinnedStream,
}

impl BinnedBytesForHttpStream {
}

impl Stream for BinnedBytesForHttpStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        error!("TODO  translate the structured stream into plain bytes for http");
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => {
                Ready(Some(Ok(Bytes::new())))
            }
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }

}


pub struct PreBinnedValueStream {
    uri: http::Uri,
    patch_coord: PreBinnedPatchCoord,
    resfut: Option<hyper::client::ResponseFuture>,
    res: Option<hyper::Response<hyper::Body>>,
}

impl PreBinnedValueStream {

    pub fn new(patch_coord: PreBinnedPatchCoord, channel: Channel, cluster: Cluster) -> Self {
        let nodeix = node_ix_for_patch(&patch_coord, &channel, &cluster);
        warn!("TODO PASS THE KIND OF AGG");
        let node = &cluster.nodes[nodeix];
        let uri: hyper::Uri = format!(
            "http://{}:{}/api/1/prebinned?beg={}&end={}&channel={}",
            node.host,
            node.port,
            patch_coord.range.beg,
            patch_coord.range.end,
            channel.name,
        ).parse().unwrap();
        Self {
            uri,
            patch_coord,
            resfut: None,
            res: None,
        }
    }

}


pub fn node_ix_for_patch(patch_coord: &PreBinnedPatchCoord, channel: &Channel, cluster: &Cluster) -> usize {
    let mut hash = tiny_keccak::Sha3::v256();
    hash.update(channel.name.as_bytes());
    0
}


impl Stream for PreBinnedValueStream {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = Result<MinMaxAvgScalarBinBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        // TODO when requested next, create the next http request, connect, check headers
        // and as soon as ready, wrap the body in the appropriate parser and return the stream.
        // The wire protocol is not yet defined.
        'outer: loop {
            break match self.res.as_mut() {
                Some(res) => {
                    pin_mut!(res);
                    use hyper::body::HttpBody;
                    match res.poll_data(cx) {
                        Ready(Some(Ok(k))) => {
                            Pending
                        }
                        Ready(Some(Err(e))) => Ready(Some(Err(e.into()))),
                        Ready(None) => Ready(None),
                        Pending => Pending,
                    }
                }
                None => {
                    match self.resfut.as_mut() {
                        Some(mut resfut) => {
                            match resfut.poll_unpin(cx) {
                                Ready(res) => {
                                    match res {
                                        Ok(res) => {
                                            info!("Got result from subrequest: {:?}", res);
                                            self.res = Some(res);
                                            continue 'outer;
                                        }
                                        Err(e) => Ready(Some(Err(e.into()))),
                                    }
                                }
                                Pending => {
                                    Pending
                                }
                            }
                        }
                        None => {

                            let req = hyper::Request::builder()
                            .method(http::Method::GET)
                            .uri(&self.uri)
                            .body(hyper::Body::empty())?;
                            let client = hyper::Client::new();
                            self.resfut = Some(client.request(req));
                            continue 'outer;
                        }
                    }
                }
            }
        }
    }

}


pub struct BinnedStream {
    inp: Pin<Box<dyn Stream<Item=Result<MinMaxAvgScalarBinBatch, Error>> + Send>>,
}

impl BinnedStream {

    pub fn new(patch_it: PreBinnedPatchIterator, channel: Channel, cluster: Cluster) -> Self {
        let mut patch_it = patch_it;
        let inp = futures_util::stream::iter(patch_it)
        .map(move |coord| {
            PreBinnedValueStream::new(coord, channel.clone(), cluster.clone())
        })
        .flatten();
        Self {
            inp: Box::pin(inp),
        }
    }

}

impl Stream for BinnedStream {
    // TODO make this generic over all possible things
    type Item = Result<MinMaxAvgScalarBinBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => {
                Ready(Some(Ok(k)))
            }
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }

}



pub struct SomeReturnThing {}

impl From<SomeReturnThing> for Bytes {

    fn from(k: SomeReturnThing) -> Self {
        todo!("TODO convert result to octets")
    }

}
