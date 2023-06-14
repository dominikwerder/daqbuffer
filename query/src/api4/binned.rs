use crate::transform::TransformQuery;
use err::Error;
use netpod::get_url_query_pairs;
use netpod::log::*;
use netpod::query::CacheUsage;
use netpod::query::PulseRangeQuery;
use netpod::query::TimeRangeQuery;
use netpod::range::evrange::SeriesRange;
use netpod::AppendToUrl;
use netpod::ByteSize;
use netpod::FromUrl;
use netpod::HasBackend;
use netpod::HasTimeout;
use netpod::SfDbChannel;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BinnedQuery {
    channel: SfDbChannel,
    range: SeriesRange,
    bin_count: u32,
    #[serde(
        default = "TransformQuery::default_time_binned",
        skip_serializing_if = "TransformQuery::is_default_time_binned"
    )]
    transform: TransformQuery,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_usage: Option<CacheUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bins_max: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timeout: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    buf_len_disk_io: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    disk_stats_every: Option<ByteSize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merger_out_len_max: Option<usize>,
}

impl BinnedQuery {
    pub fn new(channel: SfDbChannel, range: SeriesRange, bin_count: u32) -> Self {
        Self {
            channel,
            range,
            bin_count,
            transform: TransformQuery::default_time_binned(),
            cache_usage: None,
            bins_max: None,
            buf_len_disk_io: None,
            disk_stats_every: None,
            timeout: None,
            merger_out_len_max: None,
        }
    }

    pub fn range(&self) -> &SeriesRange {
        &self.range
    }

    pub fn channel(&self) -> &SfDbChannel {
        &self.channel
    }

    pub fn bin_count(&self) -> u32 {
        self.bin_count
    }

    pub fn transform(&self) -> &TransformQuery {
        &self.transform
    }

    pub fn cache_usage(&self) -> CacheUsage {
        self.cache_usage.as_ref().map_or(CacheUsage::Use, |x| x.clone())
    }

    pub fn disk_stats_every(&self) -> ByteSize {
        match &self.disk_stats_every {
            Some(x) => x.clone(),
            None => ByteSize(1024 * 1024 * 4),
        }
    }

    pub fn buf_len_disk_io(&self) -> usize {
        match self.buf_len_disk_io {
            Some(x) => x,
            None => 1024 * 16,
        }
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout.clone()
    }

    pub fn timeout_value(&self) -> Duration {
        match &self.timeout {
            Some(x) => x.clone(),
            None => Duration::from_millis(6000),
        }
    }

    pub fn bins_max(&self) -> u32 {
        self.bins_max.unwrap_or(2000)
    }

    pub fn merger_out_len_max(&self) -> usize {
        self.merger_out_len_max.unwrap_or(1024)
    }

    pub fn set_series_id(&mut self, series: u64) {
        self.channel.set_series(series);
    }

    pub fn channel_mut(&mut self) -> &mut SfDbChannel {
        &mut self.channel
    }

    pub fn set_cache_usage(&mut self, k: CacheUsage) {
        self.cache_usage = Some(k);
    }

    pub fn set_disk_stats_every(&mut self, k: ByteSize) {
        self.disk_stats_every = Some(k);
    }

    pub fn set_timeout(&mut self, k: Duration) {
        self.timeout = Some(k);
    }

    pub fn set_buf_len_disk_io(&mut self, k: usize) {
        self.buf_len_disk_io = Some(k);
    }

    pub fn for_time_weighted_scalar(self) -> Self {
        let mut v = self;
        v.transform = TransformQuery::for_time_weighted_scalar();
        v
    }
}

impl HasBackend for BinnedQuery {
    fn backend(&self) -> &str {
        self.channel.backend()
    }
}

impl HasTimeout for BinnedQuery {
    fn timeout(&self) -> Duration {
        self.timeout_value()
    }
}

impl FromUrl for BinnedQuery {
    fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let range = if let Ok(x) = TimeRangeQuery::from_pairs(pairs) {
            SeriesRange::TimeRange(x.into())
        } else if let Ok(x) = PulseRangeQuery::from_pairs(pairs) {
            SeriesRange::PulseRange(x.into())
        } else {
            return Err(Error::with_msg_no_trace("no series range in url"));
        };
        let ret = Self {
            channel: SfDbChannel::from_pairs(&pairs)?,
            range,
            bin_count: pairs
                .get("binCount")
                .ok_or(Error::with_msg("missing binCount"))?
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse binCount {:?}", e)))?,
            transform: TransformQuery::from_pairs(pairs)?,
            cache_usage: CacheUsage::from_pairs(&pairs)?,
            buf_len_disk_io: pairs
                .get("bufLenDiskIo")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            disk_stats_every: pairs
                .get("diskStatsEveryKb")
                .map(|k| k.parse().ok())
                .unwrap_or(None)
                .map(ByteSize::kb),
            /*report_error: pairs
            .get("reportError")
            .map_or("false", |k| k)
            .parse()
            .map_err(|e| Error::with_msg(format!("can not parse reportError {:?}", e)))?,*/
            timeout: pairs
                .get("timeout")
                .map(|x| x.parse::<u64>().map(Duration::from_millis).ok())
                .unwrap_or(None),
            bins_max: pairs.get("binsMax").map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            merger_out_len_max: pairs
                .get("mergerOutLenMax")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
        };
        debug!("BinnedQuery::from_url  {:?}", ret);
        Ok(ret)
    }
}

impl AppendToUrl for BinnedQuery {
    fn append_to_url(&self, url: &mut Url) {
        match &self.range {
            SeriesRange::TimeRange(k) => TimeRangeQuery::from(k).append_to_url(url),
            SeriesRange::PulseRange(k) => PulseRangeQuery::from(k).append_to_url(url),
        }
        self.channel.append_to_url(url);
        {
            let mut g = url.query_pairs_mut();
            g.append_pair("binCount", &format!("{}", self.bin_count));
        }
        self.transform.append_to_url(url);
        let mut g = url.query_pairs_mut();
        if let Some(x) = &self.cache_usage {
            g.append_pair("cacheUsage", &x.query_param_value());
        }
        if let Some(x) = &self.timeout {
            g.append_pair("timeout", &format!("{}", x.as_millis()));
        }
        if let Some(x) = self.bins_max {
            g.append_pair("binsMax", &format!("{}", x));
        }
        if let Some(x) = self.buf_len_disk_io {
            g.append_pair("bufLenDiskIo", &format!("{}", x));
        }
        if let Some(x) = &self.disk_stats_every {
            g.append_pair("diskStatsEveryKb", &format!("{}", x.bytes() / 1024));
        }
        if let Some(x) = self.merger_out_len_max.as_ref() {
            g.append_pair("mergerOutLenMax", &format!("{}", x));
        }
    }
}
