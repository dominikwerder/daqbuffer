pub mod bincache;
pub mod config;
pub mod errconv;
pub mod events;
pub mod status;

use err::Error;
use errconv::ErrConv;
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
use netpod::ScyllaConfig;
use scylla::statement::Consistency;
use scylla::Session as ScySession;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ScyllaSeriesRange {
    beg: u64,
    end: u64,
}

impl From<&SeriesRange> for ScyllaSeriesRange {
    fn from(value: &SeriesRange) -> Self {
        match value {
            SeriesRange::TimeRange(k) => Self { beg: k.beg, end: k.end },
            SeriesRange::PulseRange(k) => Self { beg: k.beg, end: k.end },
        }
    }
}

pub async fn create_scy_session(scyconf: &ScyllaConfig) -> Result<Arc<ScySession>, Error> {
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyconf.hosts)
        .use_keyspace(&scyconf.keyspace, true)
        .default_consistency(Consistency::LocalOne)
        .build()
        .await
        .err_conv()?;
    let ret = Arc::new(scy);
    Ok(ret)
}
