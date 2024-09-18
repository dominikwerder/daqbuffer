pub mod timeweight_bins;
pub mod timeweight_events;

use super::___;
use netpod::log::*;

#[allow(unused)]
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest_detail { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }
