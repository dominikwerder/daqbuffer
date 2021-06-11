use crate::query::channel_from_params;
use chrono::{DateTime, TimeZone, Utc};
use err::Error;
use netpod::{Channel, HostPort, NanoRange, ToNanos};
use std::time::Duration;

// TODO move this query type out of this `binned` mod
#[derive(Clone, Debug)]
pub struct PlainEventsQuery {
    channel: Channel,
    range: NanoRange,
    report_error: bool,
    timeout: Duration,
}

impl PlainEventsQuery {
    pub fn new(channel: Channel, range: NanoRange) -> Self {
        Self {
            channel,
            range,
            report_error: false,
            timeout: Duration::from_millis(2000),
        }
    }

    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let beg_date = params.get("begDate").ok_or(Error::with_msg("missing begDate"))?;
        let end_date = params.get("endDate").ok_or(Error::with_msg("missing endDate"))?;
        let ret = Self {
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            channel: channel_from_params(&params)?,
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

    pub fn url(&self, host: &HostPort) -> String {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
        format!(
            "http://{}:{}/api/4/plain_events?channelBackend={}&channelName={}&begDate={}&endDate={}&timeout={}",
            host.host,
            host.port,
            self.channel.backend,
            self.channel.name,
            Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt),
            Utc.timestamp_nanos(self.range.end as i64).format(date_fmt),
            self.timeout.as_millis(),
        )
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
            timeout: Duration::from_millis(2000),
        }
    }

    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let beg_date = params.get("begDate").ok_or(Error::with_msg("missing begDate"))?;
        let end_date = params.get("endDate").ok_or(Error::with_msg("missing endDate"))?;
        let ret = Self {
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            channel: channel_from_params(&params)?,
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

    pub fn url(&self, host: &HostPort) -> String {
        let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
        format!(
            "http://{}:{}/api/4/alpha_plain_events_json?channelBackend={}&channelName={}&begDate={}&endDate={}&timeout={}",
            host.host,
            host.port,
            self.channel.backend,
            self.channel.name,
            Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt),
            Utc.timestamp_nanos(self.range.end as i64).format(date_fmt),
            self.timeout.as_millis(),
        )
    }
}
