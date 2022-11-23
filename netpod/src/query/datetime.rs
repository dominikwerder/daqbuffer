use chrono::{DateTime, FixedOffset};
use err::Error;
use serde::{de::Visitor, Deserialize, Serialize};
use std::fmt;
use std::ops;

#[derive(Clone, Debug, PartialEq)]
pub struct Datetime(DateTime<FixedOffset>);

impl Datetime {}

impl From<DateTime<FixedOffset>> for Datetime {
    fn from(x: DateTime<FixedOffset>) -> Self {
        Datetime(x)
    }
}

impl TryFrom<&str> for Datetime {
    type Error = Error;

    fn try_from(val: &str) -> Result<Self, Self::Error> {
        let dt =
            DateTime::<FixedOffset>::parse_from_rfc3339(val).map_err(|e| Error::with_msg_no_trace(format!("{e}")))?;
        Ok(Datetime(dt))
    }
}

impl ops::Deref for Datetime {
    type Target = DateTime<FixedOffset>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// RFC 3339 (subset of ISO 8601)

impl Serialize for Datetime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use fmt::Write;
        let val = &self.0;
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
        serializer.collect_str(&s)
    }
}

struct Vis1;

impl<'de> Visitor<'de> for Vis1 {
    type Value = Datetime;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Datetime")
    }

    fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Datetime::try_from(val).map_err(|e| serde::de::Error::custom(format!("{e}")))
    }
}

impl<'de> Deserialize<'de> for Datetime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(Vis1)
    }
}
