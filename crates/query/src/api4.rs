pub mod binned;
pub mod events;

use err::Error;
use netpod::get_url_query_pairs;
use netpod::AppendToUrl;
use netpod::FromUrl;
use netpod::HasBackend;
use netpod::HasTimeout;
use serde::Deserialize;
use serde::Serialize;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountingIngestedBytesQuery {
    backend: String,
}

impl HasBackend for AccountingIngestedBytesQuery {
    fn backend(&self) -> &str {
        &self.backend
    }
}

impl HasTimeout for AccountingIngestedBytesQuery {
    fn timeout(&self) -> Duration {
        Duration::from_millis(5000)
    }
}

impl FromUrl for AccountingIngestedBytesQuery {
    fn from_url(url: &url::Url) -> Result<Self, err::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &std::collections::BTreeMap<String, String>) -> Result<Self, Error> {
        let ret = Self {
            backend: pairs
                .get("backend")
                .ok_or_else(|| Error::with_msg_no_trace("missing backend"))?
                .to_string(),
        };
        Ok(ret)
    }
}

impl AppendToUrl for AccountingIngestedBytesQuery {
    fn append_to_url(&self, url: &mut url::Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("backend", &self.backend);
    }
}
