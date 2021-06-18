use crate::query::channel_from_params;
use chrono::{DateTime, TimeZone, Utc};
use err::Error;
use http::request::Parts;
use netpod::log::*;
use netpod::{AggKind, ByteSize, Channel, HostPort, NanoRange, PreBinnedPatchCoord, ToNanos};
use std::collections::BTreeMap;
use std::time::Duration;
use url::Url;

#[derive(Clone, Debug)]
pub struct PreBinnedQuery {
    patch: PreBinnedPatchCoord,
    agg_kind: AggKind,
    channel: Channel,
    cache_usage: CacheUsage,
    disk_stats_every: ByteSize,
    report_error: bool,
}

impl PreBinnedQuery {
    pub fn new(
        patch: PreBinnedPatchCoord,
        channel: Channel,
        agg_kind: AggKind,
        cache_usage: CacheUsage,
        disk_stats_every: ByteSize,
        report_error: bool,
    ) -> Self {
        Self {
            patch,
            agg_kind,
            channel,
            cache_usage,
            disk_stats_every,
            report_error,
        }
    }

    pub fn from_url(url: &Url) -> Result<Self, Error> {
        let mut pairs = BTreeMap::new();
        for (j, k) in url.query_pairs() {
            pairs.insert(j.to_string(), k.to_string());
        }
        let pairs = pairs;
        let bin_t_len = pairs
            .get("binTlen")
            .ok_or(Error::with_msg("missing binTlen"))?
            .parse()?;
        let patch_t_len = pairs
            .get("patchTlen")
            .ok_or(Error::with_msg("missing patchTlen"))?
            .parse()?;
        let patch_ix = pairs
            .get("patchIx")
            .ok_or(Error::with_msg("missing patchIx"))?
            .parse()?;
        let disk_stats_every = pairs
            .get("diskStatsEveryKb")
            .ok_or(Error::with_msg("missing diskStatsEveryKb"))?;
        let disk_stats_every = disk_stats_every
            .parse()
            .map_err(|e| Error::with_msg(format!("can not parse diskStatsEveryKb {:?}", e)))?;
        let ret = Self {
            patch: PreBinnedPatchCoord::new(bin_t_len, patch_t_len, patch_ix),
            channel: channel_from_params(&pairs)?,
            agg_kind: agg_kind_from_binning_scheme(&pairs).unwrap_or(AggKind::DimXBins1),
            cache_usage: CacheUsage::from_params(&pairs)?,
            disk_stats_every: ByteSize::kb(disk_stats_every),
            report_error: pairs
                .get("reportError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse reportError {:?}", e)))?,
        };
        Ok(ret)
    }

    pub fn from_request(head: &Parts) -> Result<Self, Error> {
        let s1 = format!("dummy:{}", head.uri);
        let url = Url::parse(&s1)?;
        Self::from_url(&url)
    }

    pub fn make_query_string(&self) -> String {
        format!(
            "{}&channelBackend={}&channelName={}&binningScheme={}&cacheUsage={}&diskStatsEveryKb={}&reportError={}",
            self.patch.to_url_params_strings(),
            self.channel.backend,
            self.channel.name,
            binning_scheme_query_string(&self.agg_kind),
            self.cache_usage,
            self.disk_stats_every.bytes() / 1024,
            self.report_error(),
        )
    }

    pub fn patch(&self) -> &PreBinnedPatchCoord {
        &self.patch
    }

    pub fn report_error(&self) -> bool {
        self.report_error
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn agg_kind(&self) -> &AggKind {
        &self.agg_kind
    }

    pub fn disk_stats_every(&self) -> ByteSize {
        self.disk_stats_every.clone()
    }

    pub fn cache_usage(&self) -> CacheUsage {
        self.cache_usage.clone()
    }
}

#[derive(Clone, Debug)]
pub enum CacheUsage {
    Use,
    Ignore,
    Recreate,
}

impl CacheUsage {
    pub fn query_param_value(&self) -> String {
        match self {
            CacheUsage::Use => "use",
            CacheUsage::Ignore => "ignore",
            CacheUsage::Recreate => "recreate",
        }
        .into()
    }

    pub fn from_params(params: &BTreeMap<String, String>) -> Result<Self, Error> {
        let ret = params.get("cacheUsage").map_or(Ok::<_, Error>(CacheUsage::Use), |k| {
            if k == "use" {
                Ok(CacheUsage::Use)
            } else if k == "ignore" {
                Ok(CacheUsage::Ignore)
            } else if k == "recreate" {
                Ok(CacheUsage::Recreate)
            } else {
                Err(Error::with_msg(format!("unexpected cacheUsage {:?}", k)))?
            }
        })?;
        Ok(ret)
    }

    pub fn from_string(s: &str) -> Result<Self, Error> {
        let ret = if s == "ignore" {
            CacheUsage::Ignore
        } else if s == "recreate" {
            CacheUsage::Recreate
        } else if s == "use" {
            CacheUsage::Use
        } else {
            return Err(Error::with_msg(format!("can not interpret cache usage string: {}", s)));
        };
        Ok(ret)
    }
}

impl std::fmt::Display for CacheUsage {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "{}", self.query_param_value())
    }
}

#[derive(Clone, Debug)]
pub struct BinnedQuery {
    channel: Channel,
    range: NanoRange,
    bin_count: u32,
    agg_kind: AggKind,
    cache_usage: CacheUsage,
    disk_stats_every: ByteSize,
    report_error: bool,
    timeout: Duration,
    abort_after_bin_count: u32,
}

impl BinnedQuery {
    pub fn new(channel: Channel, range: NanoRange, bin_count: u32, agg_kind: AggKind) -> Self {
        Self {
            channel,
            range,
            bin_count,
            agg_kind,
            cache_usage: CacheUsage::Use,
            disk_stats_every: ByteSize(1024 * 1024 * 4),
            report_error: false,
            timeout: Duration::from_millis(2000),
            abort_after_bin_count: 0,
        }
    }

    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let beg_date = params.get("begDate").ok_or(Error::with_msg("missing begDate"))?;
        let end_date = params.get("endDate").ok_or(Error::with_msg("missing endDate"))?;
        let disk_stats_every = params.get("diskStatsEveryKb").map_or("2000", |k| k);
        let disk_stats_every = disk_stats_every
            .parse()
            .map_err(|e| Error::with_msg(format!("can not parse diskStatsEveryKb {:?}", e)))?;
        let ret = Self {
            channel: channel_from_params(&params)?,
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            bin_count: params
                .get("binCount")
                .ok_or(Error::with_msg("missing binCount"))?
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse binCount {:?}", e)))?,
            agg_kind: agg_kind_from_binning_scheme(&params).unwrap_or(AggKind::DimXBins1),
            cache_usage: CacheUsage::from_params(&params)?,
            disk_stats_every: ByteSize::kb(disk_stats_every),
            report_error: params
                .get("reportError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse reportError {:?}", e)))?,
            timeout: params
                .get("timeout")
                .map_or("2000", |k| k)
                .parse::<u64>()
                .map(|k| Duration::from_millis(k))
                .map_err(|e| Error::with_msg(format!("can not parse timeout {:?}", e)))?,
            abort_after_bin_count: params
                .get("abortAfterBinCount")
                .map_or("0", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse abortAfterBinCount {:?}", e)))?,
        };
        info!("BinnedQuery::from_request  {:?}", ret);
        Ok(ret)
    }

    pub fn range(&self) -> &NanoRange {
        &self.range
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn bin_count(&self) -> u32 {
        self.bin_count
    }

    pub fn agg_kind(&self) -> &AggKind {
        &self.agg_kind
    }

    pub fn cache_usage(&self) -> &CacheUsage {
        &self.cache_usage
    }

    pub fn disk_stats_every(&self) -> &ByteSize {
        &self.disk_stats_every
    }

    pub fn report_error(&self) -> bool {
        self.report_error
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn abort_after_bin_count(&self) -> u32 {
        self.abort_after_bin_count
    }

    pub fn set_cache_usage(&mut self, k: CacheUsage) {
        self.cache_usage = k;
    }

    pub fn set_disk_stats_every(&mut self, k: ByteSize) {
        self.disk_stats_every = k;
    }

    pub fn set_timeout(&mut self, k: Duration) {
        self.timeout = k;
    }

    // TODO the BinnedQuery itself should maybe already carry the full HostPort?
    // On the other hand, want to keep the flexibility for the fail over possibility..
    pub fn url(&self, host: &HostPort) -> String {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
        format!(
            "http://{}:{}/api/4/binned?cacheUsage={}&channelBackend={}&channelName={}&binCount={}&begDate={}&endDate={}&binningScheme={}&diskStatsEveryKb={}&timeout={}&abortAfterBinCount={}",
            host.host,
            host.port,
            self.cache_usage,
            self.channel.backend,
            self.channel.name,
            self.bin_count,
            Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt),
            Utc.timestamp_nanos(self.range.end as i64).format(date_fmt),
            binning_scheme_query_string(&self.agg_kind),
            self.disk_stats_every.bytes() / 1024,
            self.timeout.as_millis(),
            self.abort_after_bin_count,
        )
    }
}

fn binning_scheme_query_string(agg_kind: &AggKind) -> String {
    match agg_kind {
        AggKind::Plain => "fullValue".into(),
        AggKind::DimXBins1 => "toScalarX".into(),
        AggKind::DimXBinsN(n) => format!("binnedX&binnedXcount={}", n),
    }
}

fn agg_kind_from_binning_scheme(params: &BTreeMap<String, String>) -> Result<AggKind, Error> {
    let key = "binningScheme";
    let s = params
        .get(key)
        .map_or(Err(Error::with_msg(format!("can not find {}", key))), |k| Ok(k))?;
    let ret = if s == "fullValue" {
        AggKind::Plain
    } else if s == "toScalarX" {
        AggKind::DimXBins1
    } else if s == "binnedX" {
        let u = params.get("binnedXcount").map_or("1", |k| k).parse()?;
        AggKind::DimXBinsN(u)
    } else {
        return Err(Error::with_msg("can not extract binningScheme"));
    };
    Ok(ret)
}
