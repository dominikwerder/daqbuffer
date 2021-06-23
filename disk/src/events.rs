use chrono::{DateTime, TimeZone, Utc};
use err::Error;
use netpod::{
    channel_from_pairs, get_url_query_pairs, AppendToUrl, Channel, FromUrl, HasBackend, HasTimeout, HostPort,
    NanoRange, ToNanos,
};
use std::time::Duration;
use url::Url;

// TODO move this query type out of this `binned` mod
#[derive(Clone, Debug)]
pub struct PlainEventsBinaryQuery {
    channel: Channel,
    range: NanoRange,
    report_error: bool,
    timeout: Duration,
}

impl PlainEventsBinaryQuery {
    pub fn new(channel: Channel, range: NanoRange) -> Self {
        Self {
            channel,
            range,
            report_error: false,
            timeout: Duration::from_millis(10000),
        }
    }

    pub fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        let beg_date = pairs.get("begDate").ok_or(Error::with_msg("missing begDate"))?;
        let end_date = pairs.get("endDate").ok_or(Error::with_msg("missing endDate"))?;
        let ret = Self {
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            channel: channel_from_pairs(&pairs)?,
            report_error: pairs
                .get("reportError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse reportError {:?}", e)))?,
            timeout: pairs
                .get("timeout")
                .map_or("10000", |k| k)
                .parse::<u64>()
                .map(|k| Duration::from_millis(k))
                .map_err(|e| Error::with_msg(format!("can not parse timeout {:?}", e)))?,
        };
        Ok(ret)
    }

    pub fn range(&self) -> &NanoRange {
        &self.range
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn report_error(&self) -> bool {
        self.report_error
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn set_timeout(&mut self, k: Duration) {
        self.timeout = k;
    }
}

impl AppendToUrl for PlainEventsBinaryQuery {
    fn append_to_url(&self, url: &mut Url) {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
        let mut g = url.query_pairs_mut();
        g.append_pair("channelBackend", &self.channel.backend);
        g.append_pair("channelName", &self.channel.name);
        g.append_pair(
            "begDate",
            &Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt).to_string(),
        );
        g.append_pair(
            "endDate",
            &Utc.timestamp_nanos(self.range.end as i64).format(date_fmt).to_string(),
        );
        g.append_pair("timeout", &format!("{}", self.timeout.as_millis()));
    }
}

// TODO move this query type out of this `binned` mod
#[derive(Clone, Debug)]
pub struct PlainEventsJsonQuery {
    channel: Channel,
    range: NanoRange,
    report_error: bool,
    timeout: Duration,
}

impl PlainEventsJsonQuery {
    pub fn new(channel: Channel, range: NanoRange) -> Self {
        Self {
            channel,
            range,
            report_error: false,
            timeout: Duration::from_millis(10000),
        }
    }

    pub fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        let beg_date = pairs.get("begDate").ok_or(Error::with_msg("missing begDate"))?;
        let end_date = pairs.get("endDate").ok_or(Error::with_msg("missing endDate"))?;
        let ret = Self {
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            channel: channel_from_pairs(&pairs)?,
            report_error: pairs
                .get("reportError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_msg(format!("can not parse reportError {:?}", e)))?,
            timeout: pairs
                .get("timeout")
                .map_or("10000", |k| k)
                .parse::<u64>()
                .map(|k| Duration::from_millis(k))
                .map_err(|e| Error::with_msg(format!("can not parse timeout {:?}", e)))?,
        };
        Ok(ret)
    }

    pub fn from_request_head(head: &http::request::Parts) -> Result<Self, Error> {
        let s1 = format!("dummy:{}", head.uri);
        let url = Url::parse(&s1)?;
        Self::from_url(&url)
    }

    pub fn range(&self) -> &NanoRange {
        &self.range
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn report_error(&self) -> bool {
        self.report_error
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn set_timeout(&mut self, k: Duration) {
        self.timeout = k;
    }

    // TODO remove in favor of Self::append_to_url
    pub fn url(&self, host: &HostPort) -> String {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
        format!(
            "http://{}:{}/api/4/events?channelBackend={}&channelName={}&begDate={}&endDate={}&timeout={}",
            host.host,
            host.port,
            self.channel.backend,
            self.channel.name,
            Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt),
            Utc.timestamp_nanos(self.range.end as i64).format(date_fmt),
            self.timeout.as_millis(),
        )
    }

    pub fn append_to_url(&self, url: &mut Url) {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
        let mut g = url.query_pairs_mut();
        g.append_pair("channelBackend", &self.channel.backend);
        g.append_pair("channelName", &self.channel.name);
        g.append_pair(
            "begDate",
            &Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt).to_string(),
        );
        g.append_pair(
            "endDate",
            &Utc.timestamp_nanos(self.range.end as i64).format(date_fmt).to_string(),
        );
        g.append_pair("timeout", &format!("{}", self.timeout.as_millis()));
    }
}

impl HasBackend for PlainEventsJsonQuery {
    fn backend(&self) -> &str {
        &self.channel.backend
    }
}

impl HasTimeout for PlainEventsJsonQuery {
    fn timeout(&self) -> Duration {
        self.timeout.clone()
    }
}

impl FromUrl for PlainEventsJsonQuery {
    fn from_url(url: &Url) -> Result<Self, Error> {
        Self::from_url(url)
    }
}

impl AppendToUrl for PlainEventsJsonQuery {
    fn append_to_url(&self, url: &mut Url) {
        self.append_to_url(url)
    }
}
