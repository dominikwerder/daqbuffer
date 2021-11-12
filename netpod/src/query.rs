use crate::log::*;
use crate::{
    channel_from_pairs, get_url_query_pairs, AggKind, AppendToUrl, ByteSize, Channel, FromUrl, HasBackend, HasTimeout,
    NanoRange, ToNanos,
};
use chrono::{DateTime, TimeZone, Utc};
use err::Error;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;
use url::Url;

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

    pub fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let ret = pairs.get("cacheUsage").map_or(Ok::<_, Error>(CacheUsage::Use), |k| {
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

impl fmt::Display for CacheUsage {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.query_param_value())
    }
}

/**
Query parameters to request (optionally) X-processed, but not T-processed events.
*/
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawEventsQuery {
    pub channel: Channel,
    pub range: NanoRange,
    pub agg_kind: AggKind,
    pub disk_io_buffer_size: usize,
    pub do_decompress: bool,
}

#[derive(Clone, Debug)]
pub struct BinnedQuery {
    channel: Channel,
    range: NanoRange,
    bin_count: u32,
    agg_kind: AggKind,
    cache_usage: CacheUsage,
    disk_io_buffer_size: usize,
    disk_stats_every: ByteSize,
    report_error: bool,
    timeout: Duration,
    abort_after_bin_count: u32,
    do_log: bool,
}

impl BinnedQuery {
    pub fn new(channel: Channel, range: NanoRange, bin_count: u32, agg_kind: AggKind) -> Self {
        Self {
            channel,
            range,
            bin_count,
            agg_kind,
            cache_usage: CacheUsage::Use,
            disk_io_buffer_size: 1024 * 4,
            disk_stats_every: ByteSize(1024 * 1024 * 4),
            report_error: false,
            timeout: Duration::from_millis(2000),
            abort_after_bin_count: 0,
            do_log: false,
        }
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

    pub fn disk_io_buffer_size(&self) -> usize {
        self.disk_io_buffer_size
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

    pub fn do_log(&self) -> bool {
        self.do_log
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

    pub fn set_disk_io_buffer_size(&mut self, k: usize) {
        self.disk_io_buffer_size = k;
    }
}

impl HasBackend for BinnedQuery {
    fn backend(&self) -> &str {
        &self.channel.backend
    }
}

impl HasTimeout for BinnedQuery {
    fn timeout(&self) -> Duration {
        self.timeout.clone()
    }
}

impl FromUrl for BinnedQuery {
    fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        let beg_date = pairs.get("begDate").ok_or(Error::with_msg("missing begDate"))?;
        let end_date = pairs.get("endDate").ok_or(Error::with_msg("missing endDate"))?;
        let disk_stats_every = pairs.get("diskStatsEveryKb").map_or("2000", |k| k);
        let disk_stats_every = disk_stats_every
            .parse()
            .map_err(|e| Error::with_msg(format!("can not parse diskStatsEveryKb {:?}", e)))?;
        let ret = Self {
            channel: channel_from_pairs(&pairs)?,
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            bin_count: pairs
                .get("binCount")
                .ok_or(Error::with_msg("missing binCount"))?
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse binCount {:?}", e)))?,
            agg_kind: agg_kind_from_binning_scheme(&pairs).unwrap_or(AggKind::DimXBins1),
            cache_usage: CacheUsage::from_pairs(&pairs)?,
            disk_io_buffer_size: pairs
                .get("diskIoBufferSize")
                .map_or("4096", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse diskIoBufferSize {:?}", e)))?,
            disk_stats_every: ByteSize::kb(disk_stats_every),
            report_error: pairs
                .get("reportError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse reportError {:?}", e)))?,
            timeout: pairs
                .get("timeout")
                .map_or("6000", |k| k)
                .parse::<u64>()
                .map(|k| Duration::from_millis(k))
                .map_err(|e| Error::with_msg(format!("can not parse timeout {:?}", e)))?,
            abort_after_bin_count: pairs
                .get("abortAfterBinCount")
                .map_or("0", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse abortAfterBinCount {:?}", e)))?,
            do_log: pairs
                .get("doLog")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse doLog {:?}", e)))?,
        };
        debug!("BinnedQuery::from_url  {:?}", ret);
        Ok(ret)
    }
}

impl AppendToUrl for BinnedQuery {
    fn append_to_url(&self, url: &mut Url) {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
        {
            let mut g = url.query_pairs_mut();
            g.append_pair("cacheUsage", &self.cache_usage.to_string());
            g.append_pair("channelBackend", &self.channel.backend);
            g.append_pair("channelName", &self.channel.name);
            g.append_pair("binCount", &format!("{}", self.bin_count));
            g.append_pair(
                "begDate",
                &Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt).to_string(),
            );
            g.append_pair(
                "endDate",
                &Utc.timestamp_nanos(self.range.end as i64).format(date_fmt).to_string(),
            );
        }
        {
            binning_scheme_append_to_url(&self.agg_kind, url);
        }
        {
            let mut g = url.query_pairs_mut();
            g.append_pair("diskIoBufferSize", &format!("{}", self.disk_io_buffer_size));
            g.append_pair("diskStatsEveryKb", &format!("{}", self.disk_stats_every.bytes() / 1024));
            g.append_pair("timeout", &format!("{}", self.timeout.as_millis()));
            g.append_pair("abortAfterBinCount", &format!("{}", self.abort_after_bin_count));
            g.append_pair("doLog", &format!("{}", self.do_log));
        }
    }
}

pub fn binning_scheme_append_to_url(agg_kind: &AggKind, url: &mut Url) {
    let mut g = url.query_pairs_mut();
    match agg_kind {
        AggKind::EventBlobs => panic!(),
        AggKind::TimeWeightedScalar => {
            g.append_pair("binningScheme", "timeWeightedScalar");
        }
        AggKind::Plain => {
            g.append_pair("binningScheme", "fullValue");
        }
        AggKind::DimXBins1 => {
            g.append_pair("binningScheme", "unweightedScalar");
        }
        AggKind::DimXBinsN(n) => {
            g.append_pair("binningScheme", "binnedX");
            g.append_pair("binnedXcount", &format!("{}", n));
        }
    }
}

pub fn agg_kind_from_binning_scheme(pairs: &BTreeMap<String, String>) -> Result<AggKind, Error> {
    let key = "binningScheme";
    let s = pairs
        .get(key)
        .map_or(Err(Error::with_msg(format!("can not find {}", key))), |k| Ok(k))?;
    let ret = if s == "eventBlobs" {
        AggKind::EventBlobs
    } else if s == "fullValue" {
        AggKind::Plain
    } else if s == "timeWeightedScalar" {
        AggKind::TimeWeightedScalar
    } else if s == "unweightedScalar" {
        AggKind::DimXBins1
    } else if s == "binnedX" {
        let u = pairs.get("binnedXcount").map_or("1", |k| k).parse()?;
        AggKind::DimXBinsN(u)
    } else {
        return Err(Error::with_msg("can not extract binningScheme"));
    };
    Ok(ret)
}
