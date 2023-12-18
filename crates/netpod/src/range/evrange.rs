use crate::timeunits::SEC;
use crate::Dim0Kind;
use crate::TsNano;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use err::Error;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TimeRange {
    Time { beg: DateTime<Utc>, end: DateTime<Utc> },
    Pulse { beg: u64, end: u64 },
    Nano { beg: u64, end: u64 },
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct NanoRange {
    pub beg: u64,
    pub end: u64,
}

impl fmt::Debug for NanoRange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if true {
            let beg = TsNano(self.beg);
            let end = TsNano(self.end);
            f.debug_struct("NanoRange")
                .field("beg", &beg)
                .field("end", &end)
                .finish()
        } else {
            let beg = chrono::Utc
                .timestamp_opt((self.beg / SEC) as i64, (self.beg % SEC) as u32)
                .earliest();
            let end = chrono::Utc
                .timestamp_opt((self.end / SEC) as i64, (self.end % SEC) as u32)
                .earliest();
            if let (Some(a), Some(b)) = (beg, end) {
                f.debug_struct("NanoRange").field("beg", &a).field("end", &b).finish()
            } else {
                f.debug_struct("NanoRange")
                    .field("beg", &beg)
                    .field("end", &end)
                    .finish()
            }
        }
    }
}

impl NanoRange {
    pub fn from_date_time(beg: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            beg: beg.timestamp_nanos_opt().unwrap_or(0) as u64,
            end: end.timestamp_nanos_opt().unwrap_or(0) as u64,
        }
    }

    pub fn delta(&self) -> u64 {
        self.end - self.beg
    }

    pub fn beg(&self) -> u64 {
        self.beg
    }

    pub fn end(&self) -> u64 {
        self.end
    }
}

impl TryFrom<&SeriesRange> for NanoRange {
    type Error = Error;

    fn try_from(val: &SeriesRange) -> Result<NanoRange, Self::Error> {
        match val {
            SeriesRange::TimeRange(x) => Ok(x.clone()),
            SeriesRange::PulseRange(_) => Err(Error::with_msg_no_trace("not a Time range")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PulseRange {
    pub beg: u64,
    pub end: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SeriesRange {
    TimeRange(NanoRange),
    PulseRange(PulseRange),
}

impl SeriesRange {
    pub fn dim0kind(&self) -> Dim0Kind {
        match self {
            SeriesRange::TimeRange(_) => Dim0Kind::Time,
            SeriesRange::PulseRange(_) => Dim0Kind::Pulse,
        }
    }

    pub fn is_time(&self) -> bool {
        match self {
            SeriesRange::TimeRange(_) => true,
            SeriesRange::PulseRange(_) => false,
        }
    }

    pub fn is_pulse(&self) -> bool {
        match self {
            SeriesRange::TimeRange(_) => false,
            SeriesRange::PulseRange(_) => true,
        }
    }

    pub fn beg_u64(&self) -> u64 {
        match self {
            SeriesRange::TimeRange(x) => x.beg,
            SeriesRange::PulseRange(x) => x.beg,
        }
    }

    pub fn end_u64(&self) -> u64 {
        match self {
            SeriesRange::TimeRange(x) => x.end,
            SeriesRange::PulseRange(x) => x.end,
        }
    }

    pub fn delta_u64(&self) -> u64 {
        match self {
            SeriesRange::TimeRange(x) => x.end - x.beg,
            SeriesRange::PulseRange(x) => x.end - x.beg,
        }
    }
}

impl From<NanoRange> for SeriesRange {
    fn from(k: NanoRange) -> Self {
        Self::TimeRange(k)
    }
}

impl From<PulseRange> for SeriesRange {
    fn from(k: PulseRange) -> Self {
        Self::PulseRange(k)
    }
}
