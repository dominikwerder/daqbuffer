use serde_derive::{Serialize, Deserialize};
//use std::pin::Pin;
use err::Error;

#[derive(Serialize, Deserialize)]
pub struct AggQuerySingleChannel {
    channel: String,
}

pub struct BodyStream {
    //pub receiver: async_channel::Receiver<Result<bytes::Bytes, Error>>,
    pub inner: Box<dyn futures_core::Stream<Item=Result<bytes::Bytes, Error>> + Send + Unpin>,
}
