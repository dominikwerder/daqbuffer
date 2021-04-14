#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use std::future::{ready, Future};
use std::pin::Pin;
use std::task::{Context, Poll};
use futures_core::Stream;
use futures_util::{StreamExt, FutureExt, pin_mut, TryFutureExt};
use bytes::{Bytes, BytesMut, BufMut};
use chrono::{DateTime, Utc};
use netpod::{Node, Cluster, AggKind, NanoRange, ToNanos, PreBinnedPatchRange, PreBinnedPatchIterator, PreBinnedPatchCoord, Channel, NodeConfig};
use crate::agg::MinMaxAvgScalarBinBatch;
use http::uri::Scheme;
use tiny_keccak::Hasher;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::fs::OpenOptions;

#[derive(Clone, Debug, Serialize, Deserialize)]
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
                name: params.get("channel_name").unwrap().into(),
            },
        };
        info!("Query::from_request  {:?}", ret);
        Ok(ret)
    }

}


pub fn binned_bytes_for_http(node_config: Arc<NodeConfig>, query: &Query) -> Result<BinnedBytesForHttpStream, Error> {
    let agg_kind = AggKind::DimXBins1;

    // TODO
    // Translate the Query TimeRange + AggKind into an iterator over the pre-binned patches.
    let grid = PreBinnedPatchRange::covering_range(query.range.clone(), query.count);
    match grid {
        Some(spec) => {
            info!("GOT  PreBinnedPatchGridSpec:    {:?}", spec);
            warn!("Pass here to BinnedStream what kind of Agg, range, ...");
            let s1 = BinnedStream::new(PreBinnedPatchIterator::from_range(spec), query.channel.clone(), agg_kind, node_config.clone());
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
            error!("TODO merge raw data");
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
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => {
                let mut buf = BytesMut::with_capacity(250);
                buf.put(&b"TODO serialize to bytes\n"[..]);
                Ready(Some(Ok(buf.freeze())))
            }
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }

}





#[derive(Clone, Debug)]
pub struct PreBinnedQuery {
    patch: PreBinnedPatchCoord,
    agg_kind: AggKind,
    channel: Channel,
}

impl PreBinnedQuery {

    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let ret = PreBinnedQuery {
            patch: PreBinnedPatchCoord {
                range: NanoRange {
                    beg: params.get("beg").ok_or(Error::with_msg("missing beg"))?.parse()?,
                    end: params.get("end").ok_or(Error::with_msg("missing end"))?.parse()?,
                },
            },
            agg_kind: AggKind::DimXBins1,
            channel: Channel {
                backend: params.get("channel_backend").unwrap().into(),
                name: params.get("channel_name").unwrap().into(),
            },
        };
        info!("PreBinnedQuery::from_request  {:?}", ret);
        Ok(ret)
    }

}



// NOTE  This answers a request for a single valid pre-binned patch.
// A user must first make sure that the grid spec is valid, and that this node is responsible for it.
// Otherwise it is an error.
pub fn pre_binned_bytes_for_http(node_config: Arc<NodeConfig>, query: &PreBinnedQuery) -> Result<PreBinnedValueByteStream, Error> {
    info!("pre_binned_bytes_for_http  {:?}  {:?}", query, node_config.node);
    let ret = PreBinnedValueByteStream::new(query.patch.clone(), query.channel.clone(), query.agg_kind.clone(), node_config);
    Ok(ret)
}


pub struct PreBinnedValueByteStream {
    inp: PreBinnedValueStream,
}

impl PreBinnedValueByteStream {

    pub fn new(patch: PreBinnedPatchCoord, channel: Channel, agg_kind: AggKind, node_config: Arc<NodeConfig>) -> Self {
        warn!("PreBinnedValueByteStream");
        Self {
            inp: PreBinnedValueStream::new(patch, channel, agg_kind, node_config),
        }
    }

}

impl Stream for PreBinnedValueByteStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        error!("PreBinnedValueByteStream poll_next");
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => {
                error!("TODO convert item to Bytes");
                let buf = Bytes::new();
                Ready(Some(Ok(buf)))
            }
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }

}





pub struct PreBinnedValueStream {
    patch_coord: PreBinnedPatchCoord,
    channel: Channel,
    agg_kind: AggKind,
    node_config: Arc<NodeConfig>,
    open_check_local_file: Option<Pin<Box<dyn Future<Output=Result<tokio::fs::File, std::io::Error>> + Send>>>,
}

impl PreBinnedValueStream {

    pub fn new(patch_coord: PreBinnedPatchCoord, channel: Channel, agg_kind: AggKind, node_config: Arc<NodeConfig>) -> Self {
        let node_ix = node_ix_for_patch(&patch_coord, &channel, &node_config.cluster);
        assert!(node_ix == node_config.node.id);
        Self {
            patch_coord,
            channel,
            agg_kind,
            node_config,
            open_check_local_file: None,
        }
    }

}

impl Stream for PreBinnedValueStream {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = Result<MinMaxAvgScalarBinBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break match self.open_check_local_file.as_mut() {
                None => {
                    use std::os::unix::fs::OpenOptionsExt;
                    let mut opts = std::fs::OpenOptions::new();
                    opts.read(true);
                    let fut = async {
                        tokio::fs::OpenOptions::from(opts)
                        .open("/DOESNOTEXIST").await
                    };
                    self.open_check_local_file = Some(Box::pin(fut));
                    continue 'outer;
                }
                Some(fut) => {
                    match fut.poll_unpin(cx) {
                        Ready(Ok(file)) => {
                            todo!("IMPLEMENT READ FROM LOCAL CACHE");
                            Pending
                        }
                        Ready(Err(e)) => {
                            match e.kind() {
                                std::io::ErrorKind::NotFound => {
                                    warn!("TODO LOCAL CACHE FILE NOT FOUND");
                                }
                                _ => {
                                    error!("File I/O error: {:?}", e);
                                }
                            }
                            Ready(Some(Err(e.into())))
                        }
                        Pending => Pending,
                    }
                }
            }
        }
    }

}




pub struct PreBinnedValueFetchedStream {
    uri: http::Uri,
    patch_coord: PreBinnedPatchCoord,
    resfut: Option<hyper::client::ResponseFuture>,
    res: Option<hyper::Response<hyper::Body>>,
}

impl PreBinnedValueFetchedStream {

    pub fn new(patch_coord: PreBinnedPatchCoord, channel: Channel, agg_kind: AggKind, node_config: Arc<NodeConfig>) -> Self {
        let nodeix = node_ix_for_patch(&patch_coord, &channel, &node_config.cluster);
        let node = &node_config.cluster.nodes[nodeix as usize];
        let uri: hyper::Uri = format!(
            "http://{}:{}/api/1/prebinned?beg={}&end={}&channel_backend={}&channel_name={}&agg_kind={:?}",
            node.host,
            node.port,
            patch_coord.range.beg,
            patch_coord.range.end,
            channel.backend,
            channel.name,
            agg_kind,
        ).parse().unwrap();
        Self {
            uri,
            patch_coord,
            resfut: None,
            res: None,
        }
    }

}

impl Stream for PreBinnedValueFetchedStream {
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
                                            info!("GOT result from SUB REQUEST: {:?}", res);
                                            self.res = Some(res);
                                            continue 'outer;
                                        }
                                        Err(e) => {
                                            error!("PreBinnedValueStream  error in stream {:?}", e);
                                            Ready(Some(Err(e.into())))
                                        }
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
                            info!("START REQUEST FOR {:?}", req);
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

    pub fn new(patch_it: PreBinnedPatchIterator, channel: Channel, agg_kind: AggKind, node_config: Arc<NodeConfig>) -> Self {
        warn!("BinnedStream will open a PreBinnedValueStream");
        let mut patch_it = patch_it;
        let inp = futures_util::stream::iter(patch_it)
        .map(move |coord| {
            PreBinnedValueFetchedStream::new(coord, channel.clone(), agg_kind.clone(), node_config.clone())
        })
        .flatten()
        .map(|k| {
            info!("ITEM {:?}", k);
            k
        });
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
        error!("TODO convert result to octets");
        todo!("TODO convert result to octets")
    }

}



pub fn node_ix_for_patch(patch_coord: &PreBinnedPatchCoord, channel: &Channel, cluster: &Cluster) -> u32 {
    let mut hash = tiny_keccak::Sha3::v256();
    hash.update(channel.backend.as_bytes());
    hash.update(channel.name.as_bytes());
    hash.update(&patch_coord.range.beg.to_le_bytes());
    hash.update(&patch_coord.range.end.to_le_bytes());
    let mut out = [0; 32];
    hash.finalize(&mut out);
    let mut a = [out[0], out[1], out[2], out[3]];
    let ix = u32::from_le_bytes(a) % cluster.nodes.len() as u32;
    info!("node_ix_for_patch  {}", ix);
    ix
}
