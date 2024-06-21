pub mod accounting;
pub mod bincache;
pub mod conn;
pub mod errconv;
pub mod events;
pub mod events2;
pub mod range;
pub mod status;
pub mod worker;

pub use scylla;
pub use series::SeriesId;

pub async fn test_log() {
    use netpod::log::*;
    error!("------");
    warn!("------");
    info!("------");
    debug!("------");
    trace!("------");
}
