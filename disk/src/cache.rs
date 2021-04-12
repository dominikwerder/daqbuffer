#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use netpod::{Node, Cluster, AggKind, NanoRange, ToNanos, PreBinnedPatchGridSpec};
use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use crate::agg::MinMaxAvgScalarBinBatch;
use futures_util::StreamExt;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Query {
    range: NanoRange,
    count: u64,
    agg_kind: AggKind,
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
            let mut it = netpod::PreBinnedPatchIterator::from_range(spec.clone());
            for coord in it {
                // Iterate over the patches.
                // Request the patch from each node.
                // Merge.
                // Agg+Bin.
                // Deliver.
                info!("coord: {:?}", coord)
            }
        }
        None => {
            // Merge raw data
            todo!()
        }
    }

    let ret = BinnedBytesForHttpStream {
        inp: todo!(),
    };
    Ok(ret)
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
        self.inp.poll_next_unpin(cx);
        Ready(None)
    }

}


pub struct PreBinnedViaHttpStreamStream {
}

impl Stream for PreBinnedViaHttpStreamStream {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = Result<Pin<Box<dyn Stream<Item=MinMaxAvgScalarBinBatch> + Send>>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        // TODO when requested next, create the next http request, connect, check headers
        // and as soon as ready, wrap the body in the appropriate parser and return the stream.
        // The wire protocol is not yet defined.
        todo!()
    }

}


// NOTE provides structures of merged, aggregated and binned to the final user grid
pub struct BinnedStream {

    // TODO what kind of stream do I need here, and who builds it?

    inp: Pin<Box<dyn Stream<Item=Result<MinMaxAvgScalarBinBatch, Error>> + Send>>,
}

impl Stream for BinnedStream {
    // TODO make this generic over all possible things
    type Item = MinMaxAvgScalarBinBatch;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        self.inp.poll_next_unpin(cx);
        todo!()
    }

}



pub struct SomeReturnThing {}

impl From<SomeReturnThing> for Bytes {

    fn from(k: SomeReturnThing) -> Self {
        todo!("TODO convert result to octets")
    }

}
