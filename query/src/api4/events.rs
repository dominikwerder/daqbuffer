use crate::transform::TransformQuery;
use err::Error;
use netpod::get_url_query_pairs;
use netpod::is_false;
use netpod::log::*;
use netpod::query::CacheUsage;
use netpod::query::PulseRangeQuery;
use netpod::query::TimeRangeQuery;
use netpod::range::evrange::SeriesRange;
use netpod::AppendToUrl;
use netpod::ByteSize;
use netpod::Channel;
use netpod::FromUrl;
use netpod::HasBackend;
use netpod::HasTimeout;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlainEventsQuery {
    channel: Channel,
    range: SeriesRange,
    #[serde(default, skip_serializing_if = "is_false", rename = "oneBeforeRange")]
    one_before_range: bool,
    #[serde(
        default = "TransformQuery::default_events",
        skip_serializing_if = "TransformQuery::is_default_events"
    )]
    transform: TransformQuery,
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
    #[serde(default, skip_serializing_if = "is_false")]
    test_do_wasm: bool,
}

impl PlainEventsQuery {
    pub fn new<R>(channel: Channel, range: R) -> Self
    where
        R: Into<SeriesRange>,
    {
        Self {
            channel,
            range: range.into(),
            one_before_range: false,
            transform: TransformQuery::default_events(),
            timeout: Some(Duration::from_millis(4000)),
            events_max: Some(10000),
            event_delay: None,
            stream_batch_len: None,
            buf_len_disk_io: None,
            do_test_main_error: false,
            do_test_stream_error: false,
            test_do_wasm: false,
        }
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    pub fn range(&self) -> &SeriesRange {
        &self.range
    }

    pub fn one_before_range(&self) -> bool {
        self.transform.need_one_before_range()
    }

    pub fn transform(&self) -> &TransformQuery {
        &self.transform
    }

    pub fn buf_len_disk_io(&self) -> usize {
        self.buf_len_disk_io.unwrap_or(1024 * 8)
    }

    pub fn timeout(&self) -> Duration {
        self.timeout.unwrap_or(Duration::from_millis(10000))
    }

    pub fn events_max(&self) -> u64 {
        self.events_max.unwrap_or(1024 * 128)
    }

    // A rough indication on how many bytes this request is allowed to return. Otherwise, the result should
    // be a partial result.
    pub fn bytes_max(&self) -> u64 {
        self.events_max.unwrap_or(1024 * 512)
    }

    pub fn event_delay(&self) -> &Option<Duration> {
        &self.event_delay
    }

    pub fn merger_out_len_max(&self) -> usize {
        1024
    }

    pub fn do_test_main_error(&self) -> bool {
        self.do_test_main_error
    }

    pub fn do_test_stream_error(&self) -> bool {
        self.do_test_stream_error
    }

    pub fn test_do_wasm(&self) -> bool {
        self.test_do_wasm
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

    pub fn for_event_blobs(mut self) -> Self {
        self.transform = TransformQuery::for_event_blobs();
        self
    }

    pub fn for_time_weighted_scalar(mut self) -> Self {
        self.transform = TransformQuery::for_time_weighted_scalar();
        self
    }

    pub fn for_pulse_id_diff(mut self) -> Self {
        self.transform = TransformQuery::for_pulse_id_diff();
        self
    }

    pub fn is_event_blobs(&self) -> bool {
        self.transform.is_event_blobs()
    }

    pub fn need_value_data(&self) -> bool {
        self.transform.need_value_data()
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

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let range = if let Ok(x) = TimeRangeQuery::from_pairs(pairs) {
            SeriesRange::TimeRange(x.into())
        } else if let Ok(x) = PulseRangeQuery::from_pairs(pairs) {
            SeriesRange::PulseRange(x.into())
        } else {
            return Err(Error::with_msg_no_trace("no series range in url"));
        };
        let ret = Self {
            channel: Channel::from_pairs(pairs)?,
            range,
            one_before_range: pairs.get("oneBeforeRange").map_or("false", |x| x.as_ref()) == "true",
            transform: TransformQuery::from_pairs(pairs)?,
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
            test_do_wasm: pairs
                .get("testDoWasm")
                .map(|x| x.parse::<bool>().ok())
                .unwrap_or(None)
                .unwrap_or(false),
        };
        Ok(ret)
    }
}

impl AppendToUrl for PlainEventsQuery {
    fn append_to_url(&self, url: &mut Url) {
        match &self.range {
            SeriesRange::TimeRange(k) => TimeRangeQuery::from(k).append_to_url(url),
            SeriesRange::PulseRange(_) => todo!(),
        }
        self.channel.append_to_url(url);
        {
            let mut g = url.query_pairs_mut();
            if self.one_before_range() {
                g.append_pair("oneBeforeRange", "true");
            }
        }
        self.transform.append_to_url(url);
        let mut g = url.query_pairs_mut();
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
        if self.test_do_wasm {
            g.append_pair("testDoWasm", "true");
        }
    }
}
