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

pub enum ScalarType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
}

impl ScalarType {

    pub fn from_dtype_index(ix: u8) -> Self {
        use ScalarType::*;
        match ix {
            0 => panic!("BOOL not supported"),
            1 => panic!("BOOL8 not supported"),
            3 => U8,
            5 => U16,
            8 => U32,
            10 => U64,
            2 => I8,
            4 => I16,
            7 => I32,
            9 => I64,
            11 => F32,
            12 => F64,
            6 => panic!("CHARACTER not supported"),
            13 => panic!("STRING not supported"),
            _ => panic!("unknown"),
        }
    }

    pub fn bytes(&self) -> u8 {
        use ScalarType::*;
        match self {
            U8 => 1,
            U16 => 2,
            U32 => 4,
            U64 => 8,
            I8 => 1,
            I16 => 2,
            I32 => 4,
            I64 => 8,
            F32 => 4,
            F64 => 8,
        }
    }

}
