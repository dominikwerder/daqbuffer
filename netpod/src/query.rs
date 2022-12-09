pub mod api1;
pub mod datetime;
pub mod prebinned;

use crate::get_url_query_pairs;
use crate::log::*;
use crate::{AggKind, AppendToUrl, ByteSize, Channel, FromUrl, HasBackend, HasTimeout, NanoRange, ToNanos};
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlainEventsQuery {
    channel: Channel,
    range: NanoRange,
    agg_kind: AggKind,
    timeout: Duration,
    events_max: Option<u64>,
    stream_batch_len: Option<usize>,
    report_error: bool,
    do_log: bool,
    do_test_main_error: bool,
    do_test_stream_error: bool,
}

impl PlainEventsQuery {
    pub fn new(
        channel: Channel,
        range: NanoRange,
        agg_kind: AggKind,
        timeout: Duration,
        events_max: Option<u64>,
        do_log: bool,
    ) -> Self {
        Self {
            channel,
            range,
            agg_kind,
            timeout,
            events_max,
            stream_batch_len: None,
            report_error: false,
            do_log,
            do_test_main_error: false,
            do_test_stream_error: false,
        }
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn range(&self) -> &NanoRange {
        &self.range
    }

    pub fn agg_kind(&self) -> &AggKind {
        &self.agg_kind
    }

    pub fn report_error(&self) -> bool {
        self.report_error
    }

    pub fn disk_io_buffer_size(&self) -> usize {
        1024 * 8
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn events_max(&self) -> Option<u64> {
        self.events_max
    }

    pub fn do_log(&self) -> bool {
        self.do_log
    }

    pub fn do_test_main_error(&self) -> bool {
        self.do_test_main_error
    }

    pub fn do_test_stream_error(&self) -> bool {
        self.do_test_stream_error
    }

    pub fn set_series_id(&mut self, series: u64) {
        self.channel.series = Some(series);
    }

    pub fn set_timeout(&mut self, k: Duration) {
        self.timeout = k;
    }

    pub fn set_do_test_main_error(&mut self, k: bool) {
        self.do_test_main_error = k;
    }

    pub fn set_do_test_stream_error(&mut self, k: bool) {
        self.do_test_stream_error = k;
    }
}

impl HasBackend for PlainEventsQuery {
    fn backend(&self) -> &str {
        &self.channel.backend
    }
}

impl HasTimeout for PlainEventsQuery {
    fn timeout(&self) -> Duration {
        self.timeout.clone()
    }
}

impl FromUrl for PlainEventsQuery {
    fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &std::collections::BTreeMap<String, String>) -> Result<Self, Error> {
        let beg_date = pairs.get("begDate").ok_or(Error::with_public_msg("missing begDate"))?;
        let end_date = pairs.get("endDate").ok_or(Error::with_public_msg("missing endDate"))?;
        let ret = Self {
            channel: Channel::from_pairs(&pairs)?,
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            agg_kind: agg_kind_from_binning_scheme(&pairs).unwrap_or(AggKind::TimeWeightedScalar),
            timeout: pairs
                .get("timeout")
                .map_or("10000", |k| k)
                .parse::<u64>()
                .map(|k| Duration::from_millis(k))
                .map_err(|e| Error::with_public_msg(format!("can not parse timeout {:?}", e)))?,
            events_max: pairs
                .get("eventsMax")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            stream_batch_len: pairs
                .get("streamBatchLen")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            report_error: pairs
                .get("reportError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_public_msg(format!("can not parse reportError {:?}", e)))?,
            do_log: pairs
                .get("doLog")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_public_msg(format!("can not parse doLog {:?}", e)))?,
            do_test_main_error: pairs
                .get("doTestMainError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_public_msg(format!("can not parse doTestMainError {:?}", e)))?,
            do_test_stream_error: pairs
                .get("doTestStreamError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_public_msg(format!("can not parse doTestStreamError {:?}", e)))?,
        };
        Ok(ret)
    }
}

impl AppendToUrl for PlainEventsQuery {
    fn append_to_url(&self, url: &mut Url) {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
        self.channel.append_to_url(url);
        binning_scheme_append_to_url(&self.agg_kind, url);
        let mut g = url.query_pairs_mut();
        g.append_pair(
            "begDate",
            &Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt).to_string(),
        );
        g.append_pair(
            "endDate",
            &Utc.timestamp_nanos(self.range.end as i64).format(date_fmt).to_string(),
        );
        g.append_pair("timeout", &format!("{}", self.timeout.as_millis()));
        if let Some(x) = self.events_max.as_ref() {
            g.append_pair("eventsMax", &format!("{}", x));
        }
        if let Some(x) = self.stream_batch_len.as_ref() {
            g.append_pair("streamBatchLen", &format!("{}", x));
        }
        g.append_pair("doLog", &format!("{}", self.do_log));
    }
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

    pub fn set_series_id(&mut self, series: u64) {
        self.channel.series = Some(series);
    }

    pub fn channel_mut(&mut self) -> &mut Channel {
        &mut self.channel
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
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let beg_date = pairs.get("begDate").ok_or(Error::with_msg("missing begDate"))?;
        let end_date = pairs.get("endDate").ok_or(Error::with_msg("missing endDate"))?;
        let disk_stats_every = pairs.get("diskStatsEveryKb").map_or("2000", |k| k);
        let disk_stats_every = disk_stats_every
            .parse()
            .map_err(|e| Error::with_msg(format!("can not parse diskStatsEveryKb {:?}", e)))?;
        let ret = Self {
            channel: Channel::from_pairs(&pairs)?,
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            bin_count: pairs
                .get("binCount")
                .ok_or(Error::with_msg("missing binCount"))?
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse binCount {:?}", e)))?,
            agg_kind: agg_kind_from_binning_scheme(&pairs).unwrap_or(AggKind::TimeWeightedScalar),
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
            self.channel.append_to_url(url);
            let mut g = url.query_pairs_mut();
            g.append_pair("cacheUsage", &self.cache_usage.to_string());
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
        AggKind::Stats1 => {
            g.append_pair("binningScheme", "stats1");
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
    } else if s == "stats1" {
        AggKind::Stats1
    } else {
        return Err(Error::with_msg("can not extract binningScheme"));
    };
    Ok(ret)
}

#[derive(Clone, Debug)]
pub struct ChannelStateEventsQuery {
    channel: Channel,
    range: NanoRange,
}

impl ChannelStateEventsQuery {
    pub fn new(channel: Channel, range: NanoRange) -> Self {
        Self { channel, range }
    }

    pub fn range(&self) -> &NanoRange {
        &self.range
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn set_series_id(&mut self, series: u64) {
        self.channel.series = Some(series);
    }

    pub fn channel_mut(&mut self) -> &mut Channel {
        &mut self.channel
    }
}

impl HasBackend for ChannelStateEventsQuery {
    fn backend(&self) -> &str {
        &self.channel.backend
    }
}

impl HasTimeout for ChannelStateEventsQuery {
    fn timeout(&self) -> Duration {
        Duration::from_millis(6000)
    }
}

impl FromUrl for ChannelStateEventsQuery {
    fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let beg_date = pairs.get("begDate").ok_or(Error::with_msg("missing begDate"))?;
        let end_date = pairs.get("endDate").ok_or(Error::with_msg("missing endDate"))?;
        let ret = Self {
            channel: Channel::from_pairs(&pairs)?,
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
        };
        let self_name = std::any::type_name::<Self>();
        info!("{self_name}::from_url  {ret:?}");
        Ok(ret)
    }
}

impl AppendToUrl for ChannelStateEventsQuery {
    fn append_to_url(&self, url: &mut Url) {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
        self.channel.append_to_url(url);
        let mut g = url.query_pairs_mut();
        g.append_pair(
            "begDate",
            &Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt).to_string(),
        );
        g.append_pair(
            "endDate",
            &Utc.timestamp_nanos(self.range.end as i64).format(date_fmt).to_string(),
        );
    }
}
