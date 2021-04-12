#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use netpod::{Node, Cluster, AggKind, NanoRange, ToNanos};
use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::Bytes;
use chrono::{DateTime, Utc};


#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Query {
    range: NanoRange,
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

pub fn binned_bytes_for_http(params: BinParams) -> Result<BinnedBytesForHttpStream, Error> {

    // TODO
    // Translate the Query TimeRange + AggKind into an iterator over the pre-binned patches.

    let ret = BinnedBytesForHttpStream {};
    Ok(ret)
}

pub struct BinnedBytesForHttpStream {
}

impl Stream for BinnedBytesForHttpStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        // TODO
        use Poll::*;
        Ready(None)
    }

}
