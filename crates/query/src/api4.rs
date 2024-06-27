pub mod binned;
pub mod events;

use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use err::Error;
use netpod::get_url_query_pairs;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::ttl::RetentionTime;
use netpod::AppendToUrl;
use netpod::FromUrl;
use netpod::HasBackend;
use netpod::HasTimeout;
use netpod::ToNanos;
use netpod::TsNano;
use netpod::DATETIME_FMT_6MS;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountingIngestedBytesQuery {
    backend: String,
    range: SeriesRange,
}

impl AccountingIngestedBytesQuery {
    pub fn range(&self) -> &SeriesRange {
        &self.range
    }
}

impl HasBackend for AccountingIngestedBytesQuery {
    fn backend(&self) -> &str {
        &self.backend
    }
}

impl HasTimeout for AccountingIngestedBytesQuery {
    fn timeout(&self) -> Duration {
        Duration::from_millis(10000)
    }

    fn set_timeout(&mut self, timeout: Duration) {
        // TODO
        // self.timeout = Some(timeout);
    }
}

impl FromUrl for AccountingIngestedBytesQuery {
    fn from_url(url: &Url) -> Result<Self, err::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let ret = Self {
            backend: pairs
                .get("backend")
                .ok_or_else(|| Error::with_public_msg_no_trace("missing backend"))?
                .to_string(),
            range: SeriesRange::from_pairs(pairs)?,
        };
        Ok(ret)
    }
}

impl AppendToUrl for AccountingIngestedBytesQuery {
    fn append_to_url(&self, url: &mut Url) {
        {
            let mut g = url.query_pairs_mut();
            g.append_pair("backend", &self.backend);
        }
        self.range.append_to_url(url);
    }
}

//

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountingToplistQuery {
    rt: RetentionTime,
    backend: String,
    ts: TsNano,
    limit: u32,
    sort: Option<String>,
}

impl AccountingToplistQuery {
    pub fn rt(&self) -> RetentionTime {
        self.rt.clone()
    }

    pub fn ts(&self) -> TsNano {
        self.ts.clone()
    }

    pub fn limit(&self) -> u32 {
        self.limit
    }

    pub fn sort(&self) -> Option<&str> {
        self.sort.as_ref().map(|x| x.as_str())
    }
}

impl HasBackend for AccountingToplistQuery {
    fn backend(&self) -> &str {
        &self.backend
    }
}

impl HasTimeout for AccountingToplistQuery {
    fn timeout(&self) -> Duration {
        Duration::from_millis(10000)
    }

    fn set_timeout(&mut self, timeout: Duration) {
        // TODO
        // self.timeout = Some(timeout);
    }
}

impl FromUrl for AccountingToplistQuery {
    fn from_url(url: &Url) -> Result<Self, err::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let fn1 = |pairs: &BTreeMap<String, String>| {
            let v = pairs
                .get("tsDate")
                .ok_or(Error::with_public_msg_no_trace("missing tsDate"))?;
            let mut w = v.parse::<DateTime<Utc>>();
            if w.is_err() && v.ends_with("ago") {
                let d = humantime::parse_duration(&v[..v.len() - 3])
                    .map_err(|_| Error::with_public_msg_no_trace(format!("can not parse {v}")))?;
                w = Ok(Utc::now() - d);
            }
            let w = w?;
            Ok::<_, Error>(TsNano::from_ns(w.to_nanos()))
        };
        let ret = Self {
            rt: pairs
                .get("retentionTime")
                .ok_or_else(|| Error::with_public_msg_no_trace("missing retentionTime"))
                .and_then(|x| {
                    x.parse()
                        .map_err(|_| Error::with_public_msg_no_trace("missing retentionTime"))
                })?,
            backend: pairs
                .get("backend")
                .ok_or_else(|| Error::with_public_msg_no_trace("missing backend"))?
                .to_string(),
            ts: fn1(pairs)?,
            limit: pairs.get("limit").map_or(None, |x| x.parse().ok()).unwrap_or(20),
            sort: pairs.get("sort").map(ToString::to_string),
        };
        Ok(ret)
    }
}

impl AppendToUrl for AccountingToplistQuery {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("backend", &self.backend);
        g.append_pair(
            "ts",
            &Utc.timestamp_nanos(self.ts.ns() as i64)
                .format(DATETIME_FMT_6MS)
                .to_string(),
        );
        g.append_pair("limit", &self.limit.to_string());
    }
}
