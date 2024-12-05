pub mod accounting;
pub mod bincache;
pub mod conn;
pub mod errconv;
pub mod events;
pub mod events2;
pub mod range;
pub mod schema;
pub mod status;
pub mod worker;

pub use daqbuf_series::SeriesId;
pub use scylla;

mod log {
    pub use netpod::log::*;
}

pub async fn test_log() {
    use netpod::log::*;
    error!("------");
    warn!("------");
    info!("------");
    debug!("------");
    trace!("------");
}
