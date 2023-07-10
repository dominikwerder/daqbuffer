use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use netpod::DATETIME_FMT_3MS;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;

#[derive(Clone, Debug, Deserialize)]
pub struct IsoDateTime(DateTime<Utc>);

impl Serialize for IsoDateTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.format(DATETIME_FMT_3MS).to_string())
    }
}

pub fn make_iso_ts(tss: &[u64]) -> Vec<IsoDateTime> {
    tss.iter()
        .map(|&k| IsoDateTime(Utc.timestamp_nanos(k as i64)))
        .collect()
}
