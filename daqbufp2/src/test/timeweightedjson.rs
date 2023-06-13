use crate::err::ErrConv;
use chrono::DateTime;
use chrono::Utc;
use err::Error;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::query::CacheUsage;
use netpod::range::evrange::NanoRange;
use netpod::AppendToUrl;
use netpod::Cluster;
use netpod::SfDbChannel;
use netpod::APP_JSON;
use query::api4::binned::BinnedQuery;
use std::time::Duration;
use url::Url;

const TEST_BACKEND: &str = "testbackend-00";

struct DataResult {
    avgs: Vec<f64>,
}

// TODO compare if I want to recycle some of this:
#[allow(unused)]
async fn get_json_common(
    channel_name: &str,
    beg_date: &str,
    end_date: &str,
    bin_count: u32,
    // TODO refactor for Transform
    //agg_kind: AggKind,
    cluster: &Cluster,
    expect_bin_count: u32,
    expect_finalised_range: bool,
) -> Result<DataResult, Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.parse()?;
    let end_date: DateTime<Utc> = end_date.parse()?;
    let channel_backend = TEST_BACKEND;
    let channel = SfDbChannel {
        backend: channel_backend.into(),
        name: channel_name.into(),
        series: None,
    };
    let range = NanoRange::from_date_time(beg_date, end_date).into();
    let mut query = BinnedQuery::new(channel, range, bin_count).for_time_weighted_scalar();
    query.set_timeout(Duration::from_millis(40000));
    query.set_cache_usage(CacheUsage::Ignore);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/binned", node0.host, node0.port))?;
    query.append_to_url(&mut url);
    let url = url;
    debug!("get_json_common  get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(http::header::ACCEPT, APP_JSON)
        .body(Body::empty())
        .ec()?;
    let client = hyper::Client::new();
    let res = client.request(req).await.ec()?;
    if res.status() != StatusCode::OK {
        error!("get_json_common client response {:?}", res);
    }
    let res = hyper::body::to_bytes(res.into_body()).await.ec()?;
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    debug!("get_json_common  DONE  time {} ms", ms);
    let res = String::from_utf8_lossy(&res).to_string();
    let res: serde_json::Value = serde_json::from_str(res.as_str())?;
    // TODO assert these:
    debug!(
        "result from endpoint: --------------\n{}\n--------------",
        serde_json::to_string_pretty(&res)?
    );
    // TODO enable in future:
    if false {
        if expect_finalised_range {
            if !res
                .get("rangeFinal")
                .ok_or(Error::with_msg("missing rangeFinal"))?
                .as_bool()
                .ok_or(Error::with_msg("key rangeFinal not bool"))?
            {
                return Err(Error::with_msg("expected rangeFinal"));
            }
        } else if res.get("rangeFinal").is_some() {
            return Err(Error::with_msg("expect absent rangeFinal"));
        }
    }
    let counts = res.get("counts").unwrap().as_array().unwrap();
    let mins = res.get("mins").unwrap().as_array().unwrap();
    let maxs = res.get("maxs").unwrap().as_array().unwrap();
    let avgs = res.get("avgs").unwrap().as_array().unwrap();
    if counts.len() != expect_bin_count as usize {
        return Err(Error::with_msg(format!(
            "expect_bin_count {}  got {}",
            expect_bin_count,
            counts.len()
        )));
    }
    if mins.len() != expect_bin_count as usize {
        return Err(Error::with_msg(format!("expect_bin_count {}", expect_bin_count)));
    }
    if maxs.len() != expect_bin_count as usize {
        return Err(Error::with_msg(format!("expect_bin_count {}", expect_bin_count)));
    }
    let avgs: Vec<_> = avgs.into_iter().map(|k| k.as_f64().unwrap()).collect();
    if avgs.len() != expect_bin_count as usize {
        return Err(Error::with_msg(format!("expect_bin_count {}", expect_bin_count)));
    }
    let ret = DataResult { avgs };
    Ok(ret)
}
