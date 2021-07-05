use bytes::BytesMut;
use err::Error;
use netpod::{log::Level, BoolNum, EventDataReadStats, EventQueryJsonStringFrame};
use serde::de::{self, Visitor};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Serialize, Deserialize)]
pub enum RangeCompletableItem<T> {
    RangeComplete,
    Data(T),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StatsItem {
    EventDataReadStats(EventDataReadStats),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StreamItem<T> {
    DataItem(T),
    Log(LogItem),
    Stats(StatsItem),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogItem {
    pub node_ix: u32,
    #[serde(with = "levelserde")]
    pub level: Level,
    pub msg: String,
}

impl LogItem {
    pub fn quick(level: Level, msg: String) -> Self {
        Self {
            level,
            msg,
            node_ix: 42,
        }
    }
}

pub type Sitemty<T> = Result<StreamItem<RangeCompletableItem<T>>, Error>;

struct VisitLevel;

impl<'de> Visitor<'de> for VisitLevel {
    type Value = u32;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "expect u32 Level code")
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
    }
}

mod levelserde {
    use super::Level;
    use super::VisitLevel;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(t: &Level, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let g = match *t {
            Level::ERROR => 1,
            Level::WARN => 2,
            Level::INFO => 3,
            Level::DEBUG => 4,
            Level::TRACE => 5,
        };
        s.serialize_u32(g)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        match d.deserialize_u32(VisitLevel) {
            Ok(level) => {
                let g = if level == 1 {
                    Level::ERROR
                } else if level == 2 {
                    Level::WARN
                } else if level == 3 {
                    Level::INFO
                } else if level == 4 {
                    Level::DEBUG
                } else if level == 5 {
                    Level::TRACE
                } else {
                    Level::TRACE
                };
                Ok(g)
            }
            Err(e) => Err(e),
        }
    }
}

pub const INMEM_FRAME_ENCID: u32 = 0x12121212;
pub const INMEM_FRAME_HEAD: usize = 20;
pub const INMEM_FRAME_FOOT: usize = 4;
pub const INMEM_FRAME_MAGIC: u32 = 0xc6c3b73d;

pub trait SubFrId {
    const SUB: u32;
}

impl SubFrId for u8 {
    const SUB: u32 = 3;
}

impl SubFrId for u16 {
    const SUB: u32 = 5;
}

impl SubFrId for u32 {
    const SUB: u32 = 8;
}

impl SubFrId for u64 {
    const SUB: u32 = 10;
}

impl SubFrId for i8 {
    const SUB: u32 = 2;
}

impl SubFrId for i16 {
    const SUB: u32 = 4;
}

impl SubFrId for i32 {
    const SUB: u32 = 7;
}

impl SubFrId for i64 {
    const SUB: u32 = 9;
}

impl SubFrId for f32 {
    const SUB: u32 = 11;
}

impl SubFrId for f64 {
    const SUB: u32 = 12;
}

impl SubFrId for BoolNum {
    const SUB: u32 = 13;
}

pub trait SitemtyFrameType {
    const FRAME_TYPE_ID: u32;
}

pub trait FrameType {
    const FRAME_TYPE_ID: u32;
}

impl FrameType for EventQueryJsonStringFrame {
    const FRAME_TYPE_ID: u32 = 0x100;
}

impl<T> FrameType for Sitemty<T>
where
    T: SitemtyFrameType,
{
    const FRAME_TYPE_ID: u32 = T::FRAME_TYPE_ID;
}

pub trait ProvidesFrameType {
    fn frame_type_id(&self) -> u32;
}

pub trait Framable: Send {
    fn typeid(&self) -> u32;
    fn make_frame(&self) -> Result<BytesMut, Error>;
}

// TODO need als Framable for those types defined in other crates.
impl<T> Framable for Sitemty<T>
where
    T: SitemtyFrameType + Send,
{
    fn typeid(&self) -> u32 {
        todo!()
    }

    fn make_frame(&self) -> Result<BytesMut, Error> {
        todo!()
    }
}

/*

impl Framable for Sitemty<serde_json::Value> {
    fn typeid(&self) -> u32 {
        EventQueryJsonStringFrame::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        panic!()
    }
}

impl Framable for Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarBinBatch>>, Error> {
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl Framable for Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarEventBatch>>, Error> {
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Result<StreamItem<RangeCompletableItem<EventValues<NTY>>>, err::Error>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Result<StreamItem<RangeCompletableItem<XBinnedScalarEvents<NTY>>>, err::Error>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Sitemty<MinMaxAvgBins<NTY>>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Sitemty<WaveEvents<NTY>>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Sitemty<XBinnedWaveEvents<NTY>>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Sitemty<MinMaxAvgWaveBins<NTY>>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Sitemty<MinMaxAvgDim1Bins<NTY>>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}
*/
