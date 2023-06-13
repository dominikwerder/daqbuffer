pub mod api1;
pub mod datetime;
pub mod prebinned;

use crate::get_url_query_pairs;
use crate::is_false;
use crate::log::*;
use crate::AggKind;
use crate::AppendToUrl;
use crate::ByteSize;
use crate::FromUrl;
use crate::HasBackend;
use crate::HasTimeout;
use crate::NanoRange;
use crate::PulseRange;
use crate::SeriesRange;
use crate::SfDbChannel;
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
pub struct TimeRangeQuery {
    range: NanoRange,
}

impl FromUrl for TimeRangeQuery {
    fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        if let (Some(beg), Some(end)) = (pairs.get("begDate"), pairs.get("endDate")) {
            let ret = Self {
                range: NanoRange {
                    beg: beg.parse::<DateTime<Utc>>()?.to_nanos(),
                    end: end.parse::<DateTime<Utc>>()?.to_nanos(),
                },
            };
            Ok(ret)
        } else if let (Some(beg), Some(end)) = (pairs.get("begNs"), pairs.get("endNs")) {
            let ret = Self {
                range: NanoRange {
                    beg: beg.parse()?,
                    end: end.parse()?,
                },
            };
            Ok(ret)
        } else {
            Err(Error::with_public_msg("missing date range"))
        }
    }
}

impl AppendToUrl for TimeRangeQuery {
    fn append_to_url(&self, url: &mut Url) {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%6fZ";
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

impl From<TimeRangeQuery> for NanoRange {
    fn from(k: TimeRangeQuery) -> Self {
        Self {
            beg: k.range.beg,
            end: k.range.end,
        }
    }
}

impl From<&NanoRange> for TimeRangeQuery {
    fn from(k: &NanoRange) -> Self {
        Self {
            range: NanoRange { beg: k.beg, end: k.end },
        }
    }
}

impl From<&PulseRange> for PulseRangeQuery {
    fn from(k: &PulseRange) -> Self {
        Self {
            range: PulseRange { beg: k.beg, end: k.end },
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulseRangeQuery {
    range: PulseRange,
}

impl FromUrl for PulseRangeQuery {
    fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        if let (Some(beg), Some(end)) = (pairs.get("begPulse"), pairs.get("endPulse")) {
            let ret = Self {
                range: PulseRange {
                    beg: beg.parse()?,
                    end: end.parse()?,
                },
            };
            Ok(ret)
        } else {
            Err(Error::with_public_msg("missing pulse range"))
        }
    }
}

impl AppendToUrl for PulseRangeQuery {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("begPulse", &self.range.beg.to_string());
        g.append_pair("endPulse", &self.range.end.to_string());
    }
}

impl From<PulseRangeQuery> for PulseRange {
    fn from(k: PulseRangeQuery) -> Self {
        Self {
            beg: k.range.beg,
            end: k.range.end,
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
    channel: SfDbChannel,
    range: NanoRange,
}

impl ChannelStateEventsQuery {
    pub fn new(channel: SfDbChannel, range: NanoRange) -> Self {
        Self { channel, range }
    }

    pub fn range(&self) -> &NanoRange {
        &self.range
    }

    pub fn channel(&self) -> &SfDbChannel {
        &self.channel
    }

    pub fn set_series_id(&mut self, series: u64) {
        self.channel.series = Some(series);
    }

    pub fn channel_mut(&mut self) -> &mut SfDbChannel {
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
            channel: SfDbChannel::from_pairs(&pairs)?,
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
