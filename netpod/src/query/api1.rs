use crate::{DiskIoTune, FileIoBufferSize, ReadSys};
use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone};
use serde::{Deserialize, Serialize};
use std::fmt;

fn u64_max() -> u64 {
    u64::MAX
}

fn is_u64_max(x: &u64) -> bool {
    *x == u64::MAX
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Api1Range {
    #[serde(rename = "type", default, skip_serializing_if = "String::is_empty")]
    ty: String,
    #[serde(
        rename = "startDate",
        serialize_with = "datetime_serde::ser",
        deserialize_with = "datetime_serde::de"
    )]
    beg: DateTime<FixedOffset>,
    #[serde(
        rename = "endDate",
        serialize_with = "datetime_serde::ser",
        deserialize_with = "datetime_serde::de"
    )]
    end: DateTime<FixedOffset>,
}

mod datetime_serde {
    // RFC 3339 / ISO 8601

    use super::*;
    use serde::de::Visitor;
    use serde::{Deserializer, Serializer};

    pub fn ser<S: Serializer>(val: &DateTime<FixedOffset>, ser: S) -> Result<S::Ok, S::Error> {
        let s = val.format("%Y-%m-%dT%H:%M:%S.%6f%:z").to_string();
        ser.serialize_str(&s)
    }

    struct DateTimeVisitor {}

    impl<'de> Visitor<'de> for DateTimeVisitor {
        type Value = DateTime<FixedOffset>;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> std::fmt::Result {
            write!(fmt, "DateTimeWithOffset")
        }

        fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            let res = DateTime::<FixedOffset>::parse_from_rfc3339(val);
            match res {
                Ok(res) => Ok(res),
                // TODO deliver better fine grained error
                Err(e) => Err(serde::de::Error::custom(format!("{e}"))),
            }
        }
    }

    pub fn de<'de, D>(de: D) -> Result<DateTime<FixedOffset>, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_str(DateTimeVisitor {})
    }
}

impl Api1Range {
    pub fn new<A, B>(beg: A, end: B) -> Self
    where
        A: Into<DateTime<FixedOffset>>,
        B: Into<DateTime<FixedOffset>>,
    {
        Self {
            ty: String::new(),
            beg: beg.into(),
            end: end.into(),
        }
    }

    pub fn beg(&self) -> &DateTime<FixedOffset> {
        &self.beg
    }

    pub fn end(&self) -> &DateTime<FixedOffset> {
        &self.end
    }
}

#[test]
fn serde_de_range_zulu() {
    let s = r#"{"startDate": "2022-11-22T10:15:12.412Z", "endDate": "2022-11-22T10:15:12.413556Z"}"#;
    let range: Api1Range = serde_json::from_str(s).unwrap();
    assert_eq!(range.beg().offset().local_minus_utc(), 0);
    assert_eq!(range.end().offset().local_minus_utc(), 0);
    assert_eq!(range.beg().timestamp_subsec_micros(), 412000);
    assert_eq!(range.end().timestamp_subsec_micros(), 413556);
}

#[test]
fn serde_de_range_offset() {
    let s = r#"{"startDate": "2022-11-22T10:15:12.412Z", "endDate": "2022-11-22T10:15:12.413556Z"}"#;
    let range: Api1Range = serde_json::from_str(s).unwrap();
    assert_eq!(range.beg().offset().local_minus_utc(), 0);
    assert_eq!(range.end().offset().local_minus_utc(), 0);
    assert_eq!(range.beg().timestamp_subsec_micros(), 412000);
    assert_eq!(range.end().timestamp_subsec_micros(), 413556);
}

#[test]
fn serde_ser_range_offset() {
    let beg = FixedOffset::east_opt(60 * 60 * 3)
        .unwrap()
        .from_local_datetime(
            &NaiveDate::from_ymd_opt(2022, 11, 22)
                .unwrap()
                .and_hms_milli_opt(13, 14, 15, 16)
                .unwrap(),
        )
        .earliest()
        .unwrap();
    let end = FixedOffset::east_opt(-60 * 60 * 1)
        .unwrap()
        .from_local_datetime(
            &NaiveDate::from_ymd_opt(2022, 11, 22)
                .unwrap()
                .and_hms_milli_opt(13, 14, 15, 800)
                .unwrap(),
        )
        .earliest()
        .unwrap();
    let range = Api1Range::new(beg, end);
    let js = serde_json::to_string(&range).unwrap();
    let exp = r#"{"startDate":"2022-11-22T13:14:15.016000+03:00","endDate":"2022-11-22T13:14:15.800000-01:00"}"#;
    assert_eq!(js, exp);
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Api1Query {
    channels: Vec<String>,
    range: Api1Range,
    // All following parameters are private and not to be used
    #[serde(default)]
    file_io_buffer_size: Option<FileIoBufferSize>,
    #[serde(default)]
    decompress: bool,
    #[serde(default = "u64_max", skip_serializing_if = "is_u64_max")]
    events_max: u64,
    #[serde(default)]
    io_queue_len: u64,
    #[serde(default)]
    log_level: String,
    #[serde(default)]
    read_sys: String,
}

impl Api1Query {
    pub fn disk_io_tune(&self) -> DiskIoTune {
        let mut k = DiskIoTune::default();
        if let Some(x) = &self.file_io_buffer_size {
            k.read_buffer_len = x.0;
        }
        if self.io_queue_len != 0 {
            k.read_queue_len = self.io_queue_len as usize;
        }
        let read_sys: ReadSys = self.read_sys.as_str().into();
        k.read_sys = read_sys;
        k
    }

    pub fn range(&self) -> &Api1Range {
        &self.range
    }

    pub fn channels(&self) -> &[String] {
        &self.channels
    }

    pub fn log_level(&self) -> &str {
        &self.log_level
    }

    pub fn decompress(&self) -> bool {
        self.decompress
    }

    pub fn events_max(&self) -> u64 {
        self.events_max
    }
}
