use chrono::{DateTime, TimeZone, Utc};
use err::Error;
use netpod::get_url_query_pairs;
use netpod::{AppendToUrl, Channel, FromUrl, HasBackend, HasTimeout, NanoRange, ToNanos};
use std::time::Duration;
use url::Url;

// TODO move this query type out of this `binned` mod
#[derive(Clone, Debug)]
pub struct PlainEventsQuery {
    channel: Channel,
    range: NanoRange,
    disk_io_buffer_size: usize,
    report_error: bool,
    timeout: Duration,
    events_max: Option<u64>,
    do_log: bool,
    do_test_main_error: bool,
    do_test_stream_error: bool,
}

impl PlainEventsQuery {
    pub fn new(
        channel: Channel,
        range: NanoRange,
        disk_io_buffer_size: usize,
        events_max: Option<u64>,
        do_log: bool,
    ) -> Self {
        Self {
            channel,
            range,
            disk_io_buffer_size,
            report_error: false,
            timeout: Duration::from_millis(10000),
            events_max,
            do_log,
            do_test_main_error: false,
            do_test_stream_error: false,
        }
    }

    pub fn from_request_head(head: &http::request::Parts) -> Result<Self, Error> {
        let s1 = format!("dummy:{}", head.uri);
        let url = Url::parse(&s1)?;
        Self::from_url(&url)
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn range(&self) -> &NanoRange {
        &self.range
    }

    pub fn report_error(&self) -> bool {
        self.report_error
    }

    pub fn disk_io_buffer_size(&self) -> usize {
        self.disk_io_buffer_size
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
            disk_io_buffer_size: pairs
                .get("diskIoBufferSize")
                .map_or("4096", |k| k)
                .parse()
                .map_err(|e| Error::with_public_msg(format!("can not parse diskIoBufferSize {:?}", e)))?,
            report_error: pairs
                .get("reportError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|e| Error::with_public_msg(format!("can not parse reportError {:?}", e)))?,
            timeout: pairs
                .get("timeout")
                .map_or("10000", |k| k)
                .parse::<u64>()
                .map(|k| Duration::from_millis(k))
                .map_err(|e| Error::with_public_msg(format!("can not parse timeout {:?}", e)))?,
            events_max: pairs
                .get("eventsMax")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
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
        let mut g = url.query_pairs_mut();
        g.append_pair(
            "begDate",
            &Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt).to_string(),
        );
        g.append_pair(
            "endDate",
            &Utc.timestamp_nanos(self.range.end as i64).format(date_fmt).to_string(),
        );
        g.append_pair("diskIoBufferSize", &format!("{}", self.disk_io_buffer_size));
        g.append_pair("timeout", &format!("{}", self.timeout.as_millis()));
        if let Some(x) = self.events_max.as_ref() {
            g.append_pair("eventsMax", &format!("{}", x));
        }
        g.append_pair("doLog", &format!("{}", self.do_log));
    }
}
