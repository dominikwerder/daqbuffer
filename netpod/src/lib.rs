use serde::{Serialize, Deserialize};
use err::Error;
use std::path::PathBuf;



#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggQuerySingleChannel {
    pub channel_config: ChannelConfig,
    pub timebin: u32,
    pub tb_file_count: u32,
    pub buffer_size: u32,
}

pub struct BodyStream {
    //pub receiver: async_channel::Receiver<Result<bytes::Bytes, Error>>,
    pub inner: Box<dyn futures_core::Stream<Item=Result<bytes::Bytes, Error>> + Send + Unpin>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

    pub fn index(&self) -> u8 {
        use ScalarType::*;
        match self {
            U8 => 3,
            U16 => 5,
            U32 => 8,
            U64 => 10,
            I8 => 2,
            I16 => 4,
            I32 => 7,
            I64 => 9,
            F32 => 11,
            F64 => 12,
        }
    }

}

#[derive(Clone)]
pub struct Node {
    pub host: String,
    pub port: u16,
    pub split: u8,
    pub data_base_path: PathBuf,
    pub ksprefix: String,
}

impl Node {
    pub fn name(&self) -> String {
        format!("{}-{}", self.host, self.port)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Channel {
    pub keyspace: u8,
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
pub struct ChannelConfig {
    pub channel: Channel,
    pub time_bin_size: u64,
    pub scalar_type: ScalarType,
    pub shape: Shape,
    pub big_endian: bool,
    pub compression: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Shape {
    Scalar,
    Wave(usize),
}
