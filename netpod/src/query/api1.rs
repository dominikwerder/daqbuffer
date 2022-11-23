use crate::{DiskIoTune, FileIoBufferSize, ReadSys};
use chrono::{DateTime, FixedOffset};
use err::Error;
use serde::{Deserialize, Serialize};
use std::fmt;

fn bool_true() -> bool {
    true
}

fn bool_is_true(x: &bool) -> bool {
    *x
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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
    use super::*;
    use serde::de::Visitor;
    use serde::{Deserializer, Serializer};

    // RFC 3339 / ISO 8601

    pub fn ser<S: Serializer>(val: &DateTime<FixedOffset>, ser: S) -> Result<S::Ok, S::Error> {
        use fmt::Write;
        let mut s = String::with_capacity(64);
        write!(&mut s, "{}", val.format("%Y-%m-%dT%H:%M:%S")).map_err(|_| serde::ser::Error::custom("fmt"))?;
        let mus = val.timestamp_subsec_micros();
        if mus % 1000 != 0 {
            write!(&mut s, "{}", val.format(".%6f")).map_err(|_| serde::ser::Error::custom("fmt"))?;
        } else if mus != 0 {
            write!(&mut s, "{}", val.format(".%3f")).map_err(|_| serde::ser::Error::custom("fmt"))?;
        }
        if val.offset().local_minus_utc() == 0 {
            write!(&mut s, "Z").map_err(|_| serde::ser::Error::custom("fmt"))?;
        } else {
            write!(&mut s, "{}", val.format("%:z")).map_err(|_| serde::ser::Error::custom("fmt"))?;
        }
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
    pub fn new<A, B>(beg: A, end: B) -> Result<Self, Error>
    where
        A: TryInto<DateTime<FixedOffset>>,
        B: TryInto<DateTime<FixedOffset>>,
        <A as TryInto<DateTime<FixedOffset>>>::Error: fmt::Debug,
        <B as TryInto<DateTime<FixedOffset>>>::Error: fmt::Debug,
    {
        let ret = Self {
            ty: String::new(),
            beg: beg.try_into().map_err(|e| format!("{e:?}"))?,
            end: end.try_into().map_err(|e| format!("{e:?}"))?,
        };
        Ok(ret)
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
    use chrono::{NaiveDate, TimeZone};
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
    let range = Api1Range::new(beg, end).unwrap();
    let js = serde_json::to_string(&range).unwrap();
    let exp = r#"{"startDate":"2022-11-22T13:14:15.016+03:00","endDate":"2022-11-22T13:14:15.800-01:00"}"#;
    assert_eq!(js, exp);
}

#[test]
fn serde_ser_range_01() {
    let beg: DateTime<FixedOffset> = "2022-11-22T02:03:04Z".parse().unwrap();
    let end: DateTime<FixedOffset> = "2022-11-22T02:03:04.123Z".parse().unwrap();
    let range = Api1Range::new(beg, end).unwrap();
    let js = serde_json::to_string(&range).unwrap();
    let exp = r#"{"startDate":"2022-11-22T02:03:04Z","endDate":"2022-11-22T02:03:04.123Z"}"#;
    assert_eq!(js, exp);
}

#[test]
fn serde_ser_range_02() {
    let beg: DateTime<FixedOffset> = "2022-11-22T02:03:04.987654Z".parse().unwrap();
    let end: DateTime<FixedOffset> = "2022-11-22T02:03:04.777000Z".parse().unwrap();
    let range = Api1Range::new(beg, end).unwrap();
    let js = serde_json::to_string(&range).unwrap();
    let exp = r#"{"startDate":"2022-11-22T02:03:04.987654Z","endDate":"2022-11-22T02:03:04.777Z"}"#;
    assert_eq!(js, exp);
}

/// In Api1, the list of channels consists of either `BACKEND/CHANNELNAME`
/// or just `CHANNELNAME`.
#[derive(Debug, PartialEq)]
pub struct ChannelTuple {
    backend: Option<String>,
    name: String,
}

impl ChannelTuple {
    pub fn new(backend: String, name: String) -> Self {
        Self {
            backend: Some(backend),
            name,
        }
    }

    pub fn from_name(name: String) -> Self {
        Self { backend: None, name }
    }

    pub fn backend(&self) -> Option<&String> {
        self.backend.as_ref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

mod serde_channel_tuple {
    use super::*;
    use serde::de::{Deserialize, Deserializer, Visitor};
    use serde::ser::{Serialize, Serializer};

    impl Serialize for ChannelTuple {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            if let Some(backend) = self.backend.as_ref() {
                serializer.serialize_str(&format!("{}/{}", backend, self.name))
            } else {
                serializer.serialize_str(&self.name)
            }
        }
    }

    struct Vis;

    impl<'de> Visitor<'de> for Vis {
        type Value = ChannelTuple;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "[Backendname/]Channelname")
        }

        fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            let mut it = val.split("/");
            // Even empty string splits into one element of empty string
            let s0 = it.next().unwrap();
            if let Some(s1) = it.next() {
                let ret = ChannelTuple {
                    backend: Some(s0.into()),
                    name: s1.into(),
                };
                Ok(ret)
            } else {
                let ret = ChannelTuple {
                    backend: None,
                    name: s0.into(),
                };
                Ok(ret)
            }
        }
    }

    impl<'de> Deserialize<'de> for ChannelTuple {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_str(Vis)
        }
    }

    #[test]
    fn ser_name() {
        let x = ChannelTuple {
            backend: None,
            name: "temperature".into(),
        };
        let js = serde_json::to_string(&x).unwrap();
        assert_eq!(js, r#""temperature""#);
    }

    #[test]
    fn ser_backend_name() {
        let x = ChannelTuple {
            backend: Some("beach".into()),
            name: "temperature".into(),
        };
        let js = serde_json::to_string(&x).unwrap();
        assert_eq!(js, r#""beach/temperature""#);
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Api1Query {
    range: Api1Range,
    channels: Vec<ChannelTuple>,
    // All following parameters are private and not to be used
    #[serde(default, skip_serializing_if = "Option::is_none")]
    file_io_buffer_size: Option<FileIoBufferSize>,
    #[serde(default = "bool_true", skip_serializing_if = "bool_is_true")]
    decompress: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    events_max: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    io_queue_len: Option<u32>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    log_level: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    read_sys: String,
}

impl Api1Query {
    pub fn new(range: Api1Range, channels: Vec<ChannelTuple>) -> Self {
        Self {
            range,
            channels,
            decompress: true,
            events_max: None,
            file_io_buffer_size: None,
            io_queue_len: None,
            log_level: String::new(),
            read_sys: String::new(),
        }
    }

    pub fn disk_io_tune(&self) -> DiskIoTune {
        let mut k = DiskIoTune::default();
        if let Some(x) = &self.file_io_buffer_size {
            k.read_buffer_len = x.0;
        }
        if let Some(io_queue_len) = self.io_queue_len {
            k.read_queue_len = io_queue_len as usize;
        }
        let read_sys: ReadSys = self.read_sys.as_str().into();
        k.read_sys = read_sys;
        k
    }

    pub fn range(&self) -> &Api1Range {
        &self.range
    }

    pub fn channels(&self) -> &[ChannelTuple] {
        &self.channels
    }

    pub fn log_level(&self) -> &str {
        &self.log_level
    }

    pub fn decompress(&self) -> bool {
        self.decompress
    }

    pub fn events_max(&self) -> Option<u64> {
        self.events_max
    }
}

#[test]
fn serde_api1_query() {
    let beg: DateTime<FixedOffset> = "2022-11-22T08:09:10Z".parse().unwrap();
    let end: DateTime<FixedOffset> = "2022-11-23T08:11:05.455009+02:00".parse().unwrap();
    let range = Api1Range::new(beg, end).unwrap();
    let ch0 = ChannelTuple::from_name("nameonly".into());
    let ch1 = ChannelTuple::new("somebackend".into(), "somechan".into());
    let qu = Api1Query::new(range, vec![ch0, ch1]);
    let js = serde_json::to_string(&qu).unwrap();
    assert_eq!(
        js,
        r#"{"range":{"startDate":"2022-11-22T08:09:10Z","endDate":"2022-11-23T08:11:05.455009+02:00"},"channels":["nameonly","somebackend/somechan"]}"#
    );
}
