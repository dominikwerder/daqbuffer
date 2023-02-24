pub mod api1;
pub mod datetime;
pub mod prebinned;

use crate::get_url_query_pairs;
use crate::is_false;
use crate::log::*;
use crate::transform::Transform;
use crate::AggKind;
use crate::AppendToUrl;
use crate::ByteSize;
use crate::Channel;
use crate::FromUrl;
use crate::HasBackend;
use crate::HasTimeout;
use crate::NanoRange;
use crate::ToNanos;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use err::Error;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize)]
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

    // Missing query parameter is not an error
    pub fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Option<Self>, Error> {
        pairs
            .get("cacheUsage")
            .map(|k| {
                if k == "use" {
                    Ok(Some(CacheUsage::Use))
                } else if k == "ignore" {
                    Ok(Some(CacheUsage::Ignore))
                } else if k == "recreate" {
                    Ok(Some(CacheUsage::Recreate))
                } else {
                    Err(Error::with_msg(format!("unexpected cacheUsage {:?}", k)))?
                }
            })
            .unwrap_or(Ok(None))
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agg_kind: Option<AggKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transform: Option<Transform>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "humantime_serde")]
    timeout: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    events_max: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "humantime_serde")]
    event_delay: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stream_batch_len: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    buf_len_disk_io: Option<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    do_test_main_error: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    do_test_stream_error: bool,
}

impl PlainEventsQuery {
    pub fn new(
        channel: Channel,
        range: NanoRange,
        agg_kind: Option<AggKind>,
        timeout: Option<Duration>,
        events_max: Option<u64>,
    ) -> Self {
        Self {
            channel,
            range,
            agg_kind,
            transform: None,
            timeout,
            events_max,
            event_delay: None,
            stream_batch_len: None,
            buf_len_disk_io: None,
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

    pub fn agg_kind(&self) -> &Option<AggKind> {
        &self.agg_kind
    }

    pub fn agg_kind_value(&self) -> AggKind {
        self.agg_kind.as_ref().map_or(AggKind::Plain, |x| x.clone())
    }

    pub fn one_before_range(&self) -> bool {
        match &self.agg_kind {
            Some(k) => k.need_expand(),
            None => false,
        }
    }

    pub fn buf_len_disk_io(&self) -> usize {
        self.buf_len_disk_io.unwrap_or(1024 * 8)
    }

    pub fn timeout(&self) -> Duration {
        self.timeout.unwrap_or(Duration::from_millis(10000))
    }

    pub fn events_max(&self) -> u64 {
        self.events_max.unwrap_or(1024 * 512)
    }

    pub fn event_delay(&self) -> &Option<Duration> {
        &self.event_delay
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
        self.timeout()
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
            channel: Channel::from_pairs(pairs)?,
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            agg_kind: agg_kind_from_binning_scheme(pairs)?,
            transform: Some(Transform::from_pairs(pairs)?),
            timeout: pairs
                .get("timeout")
                .map(|x| x.parse::<u64>().map(Duration::from_millis).ok())
                .unwrap_or(None),
            events_max: pairs
                .get("eventsMax")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            event_delay: pairs.get("eventDelay").map_or(Ok(None), |k| {
                k.parse::<u64>().map(|x| Duration::from_millis(x)).map(|k| Some(k))
            })?,
            stream_batch_len: pairs
                .get("streamBatchLen")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            buf_len_disk_io: pairs
                .get("bufLenDiskIo")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            do_test_main_error: pairs
                .get("doTestMainError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_public_msg(format!("can not parse doTestMainError: {}", e)))?,
            do_test_stream_error: pairs
                .get("doTestStreamError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_public_msg(format!("can not parse doTestStreamError: {}", e)))?,
        };
        Ok(ret)
    }
}

impl AppendToUrl for PlainEventsQuery {
    fn append_to_url(&self, url: &mut Url) {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%6fZ";
        self.channel.append_to_url(url);
        if let Some(x) = &self.transform {
            x.append_to_url(url);
        }
        if let Some(x) = &self.agg_kind {
            binning_scheme_append_to_url(x, url);
        }
        let mut g = url.query_pairs_mut();
        g.append_pair(
            "begDate",
            &Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt).to_string(),
        );
        g.append_pair(
            "endDate",
            &Utc.timestamp_nanos(self.range.end as i64).format(date_fmt).to_string(),
        );
        if let Some(x) = &self.timeout {
            g.append_pair("timeout", &format!("{}", x.as_millis()));
        }
        if let Some(x) = self.events_max.as_ref() {
            g.append_pair("eventsMax", &format!("{}", x));
        }
        if let Some(x) = self.event_delay.as_ref() {
            g.append_pair("eventDelay", &format!("{:.0}", x.as_secs_f64() * 1e3));
        }
        if let Some(x) = self.stream_batch_len.as_ref() {
            g.append_pair("streamBatchLen", &format!("{}", x));
        }
        if let Some(x) = self.buf_len_disk_io.as_ref() {
            g.append_pair("bufLenDiskIo", &format!("{}", x));
        }
        if self.do_test_main_error {
            g.append_pair("doTestMainError", "true");
        }
        if self.do_test_stream_error {
            g.append_pair("doTestStreamError", "true");
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BinnedQuery {
    channel: Channel,
    range: NanoRange,
    bin_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agg_kind: Option<AggKind>,
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
}

impl BinnedQuery {
    pub fn new(channel: Channel, range: NanoRange, bin_count: u32, agg_kind: Option<AggKind>) -> Self {
        Self {
            channel,
            range,
            bin_count,
            agg_kind,
            cache_usage: None,
            bins_max: None,
            buf_len_disk_io: None,
            disk_stats_every: None,
            timeout: None,
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

    pub fn agg_kind(&self) -> AggKind {
        match &self.agg_kind {
            Some(x) => x.clone(),
            None => AggKind::TimeWeightedScalar,
        }
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
            None => Duration::from_millis(10000),
        }
    }

    pub fn bins_max(&self) -> u32 {
        self.bins_max.unwrap_or(1024)
    }

    pub fn set_series_id(&mut self, series: u64) {
        self.channel.series = Some(series);
    }

    pub fn channel_mut(&mut self) -> &mut Channel {
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
}

impl HasBackend for BinnedQuery {
    fn backend(&self) -> &str {
        &self.channel.backend
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
        let beg_date = pairs.get("begDate").ok_or(Error::with_msg("missing begDate"))?;
        let end_date = pairs.get("endDate").ok_or(Error::with_msg("missing endDate"))?;
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
            agg_kind: agg_kind_from_binning_scheme(&pairs)?,
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
        };
        debug!("BinnedQuery::from_url  {:?}", ret);
        Ok(ret)
    }
}

impl AppendToUrl for BinnedQuery {
    fn append_to_url(&self, url: &mut Url) {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%6fZ";
        {
            self.channel.append_to_url(url);
            let mut g = url.query_pairs_mut();
            if let Some(x) = &self.cache_usage {
                g.append_pair("cacheUsage", &x.query_param_value());
            }
            g.append_pair(
                "begDate",
                &Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt).to_string(),
            );
            g.append_pair(
                "endDate",
                &Utc.timestamp_nanos(self.range.end as i64).format(date_fmt).to_string(),
            );
            g.append_pair("binCount", &format!("{}", self.bin_count));
        }
        if let Some(x) = &self.agg_kind {
            binning_scheme_append_to_url(x, url);
        }
        {
            let mut g = url.query_pairs_mut();
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
        }
    }
}

pub fn binning_scheme_append_to_url(agg_kind: &AggKind, url: &mut Url) {
    let mut g = url.query_pairs_mut();
    match agg_kind {
        AggKind::EventBlobs => {
            g.append_pair("binningScheme", "eventBlobs");
        }
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
        AggKind::PulseIdDiff => {
            g.append_pair("binningScheme", "pulseIdDiff");
        }
    }
}

// Absent AggKind is not considered an error.
pub fn agg_kind_from_binning_scheme(pairs: &BTreeMap<String, String>) -> Result<Option<AggKind>, Error> {
    let key = "binningScheme";
    if let Some(s) = pairs.get(key) {
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
        } else if s == "pulseIdDiff" {
            AggKind::PulseIdDiff
        } else {
            return Err(Error::with_msg("can not extract binningScheme"));
        };
        Ok(Some(ret))
    } else {
        Ok(None)
    }
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
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%6fZ";
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
