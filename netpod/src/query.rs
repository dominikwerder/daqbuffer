use crate::{AggKind, Channel, NanoRange};
use serde::{Deserialize, Serialize};

/**
Query parameters to request (optionally) X-processed, but not T-processed events.
*/
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawEventsQuery {
    pub channel: Channel,
    pub range: NanoRange,
    pub agg_kind: AggKind,
    pub disk_io_buffer_size: usize,
    pub do_decompress: bool,
}
