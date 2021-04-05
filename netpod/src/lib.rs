use serde::{Serialize, Deserialize};
use err::Error;
//use std::pin::Pin;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Channel {
    pub backend: String,
    pub name: String,
}

impl Channel {
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[test]
fn serde_channel() {
    let _ex = "{\"name\":\"thechannel\",\"backend\":\"thebackend\"}";
}




#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggQuerySingleChannel {
    pub ksprefix: String,
    pub keyspace: u32,
    pub channel: Channel,
    pub timebin: u32,
    pub split: u32,
    pub tbsize: u32,
    pub buffer_size: u32,
    pub tb_file_count: u32,
}

pub struct BodyStream {
    //pub receiver: async_channel::Receiver<Result<bytes::Bytes, Error>>,
    pub inner: Box<dyn futures_core::Stream<Item=Result<bytes::Bytes, Error>> + Send + Unpin>,
}
