use crate::frame::make_error_frame;
use crate::frame::make_frame_2;
use crate::frame::make_log_frame;
use crate::frame::make_range_complete_frame;
use crate::frame::make_stats_frame;
use bytes::BytesMut;
use err::Error;
use items_0::framable::FrameTypeInnerDyn;
use items_0::framable::FrameTypeInnerStatic;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StatsItem;
use items_0::streamitem::StreamItem;
use items_0::streamitem::ERROR_FRAME_TYPE_ID;
use items_0::streamitem::EVENT_QUERY_JSON_STRING_FRAME;
use items_0::streamitem::SITEMTY_NONSPEC_FRAME_TYPE_ID;
use items_0::Events;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

pub const INMEM_FRAME_ENCID: u32 = 0x12121212;
pub const INMEM_FRAME_HEAD: usize = 20;
pub const INMEM_FRAME_FOOT: usize = 4;
pub const INMEM_FRAME_MAGIC: u32 = 0xc6c3b73d;

pub trait FrameTypeStatic {
    const FRAME_TYPE_ID: u32;
}

impl<T> FrameTypeStatic for Sitemty<T>
where
    T: FrameTypeInnerStatic,
{
    const FRAME_TYPE_ID: u32 = <T as FrameTypeInnerStatic>::FRAME_TYPE_ID;
}

// Framable trait objects need some inspection to handle the supposed-to-be common Err ser format:
// Meant to be implemented by Sitemty.
pub trait FrameType {
    fn frame_type_id(&self) -> u32;
}

impl<T> FrameType for Box<T>
where
    T: FrameType,
{
    fn frame_type_id(&self) -> u32 {
        self.as_ref().frame_type_id()
    }
}

impl FrameType for Box<dyn Events> {
    fn frame_type_id(&self) -> u32 {
        self.as_ref().frame_type_id()
    }
}

pub trait Framable {
    fn make_frame(&self) -> Result<BytesMut, Error>;
}

pub trait FramableInner: erased_serde::Serialize + FrameTypeInnerDyn + Send {
    fn _dummy(&self);
}

impl<T: erased_serde::Serialize + FrameTypeInnerDyn + Send> FramableInner for T {
    fn _dummy(&self) {}
}

impl<T> Framable for Sitemty<T>
where
    T: Sized + serde::Serialize + FrameType,
{
    fn make_frame(&self) -> Result<BytesMut, Error> {
        match self {
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k))) => {
                let frame_type_id = k.frame_type_id();
                make_frame_2(self, frame_type_id)
            }
            Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)) => make_range_complete_frame(),
            Ok(StreamItem::Log(item)) => make_log_frame(item),
            Ok(StreamItem::Stats(item)) => make_stats_frame(item),
            Err(e) => make_error_frame(e),
        }
    }
}

impl<T> Framable for Box<T>
where
    T: Framable + ?Sized,
{
    fn make_frame(&self) -> Result<BytesMut, Error> {
        self.as_ref().make_frame()
    }
}

pub trait FrameDecodable: FrameTypeStatic + DeserializeOwned {
    fn from_error(e: err::Error) -> Self;
    fn from_log(item: LogItem) -> Self;
    fn from_stats(item: StatsItem) -> Self;
    fn from_range_complete() -> Self;
}

impl<T> FrameDecodable for Sitemty<T>
where
    T: FrameTypeInnerStatic + DeserializeOwned,
{
    fn from_error(e: err::Error) -> Self {
        Err(e)
    }

    fn from_log(item: LogItem) -> Self {
        Ok(StreamItem::Log(item))
    }

    fn from_stats(item: StatsItem) -> Self {
        Ok(StreamItem::Stats(item))
    }

    fn from_range_complete() -> Self {
        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
    }
}

#[derive(Serialize, Deserialize)]
pub struct EventQueryJsonStringFrame(pub String);

impl FrameTypeInnerStatic for EventQueryJsonStringFrame {
    const FRAME_TYPE_ID: u32 = EVENT_QUERY_JSON_STRING_FRAME;
}

impl FrameType for EventQueryJsonStringFrame {
    fn frame_type_id(&self) -> u32 {
        EventQueryJsonStringFrame::FRAME_TYPE_ID
    }
}

impl<T> FrameType for Sitemty<T>
where
    T: FrameType,
{
    fn frame_type_id(&self) -> u32 {
        match self {
            Ok(item) => match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => SITEMTY_NONSPEC_FRAME_TYPE_ID,
                    RangeCompletableItem::Data(item) => item.frame_type_id(),
                },
                StreamItem::Log(_) => SITEMTY_NONSPEC_FRAME_TYPE_ID,
                StreamItem::Stats(_) => SITEMTY_NONSPEC_FRAME_TYPE_ID,
            },
            Err(_) => ERROR_FRAME_TYPE_ID,
        }
    }
}

#[test]
fn test_frame_log() {
    use crate::channelevents::ChannelEvents;
    use crate::frame::decode_from_slice;
    use netpod::log::Level;
    let item = LogItem {
        node_ix: 123,
        level: Level::TRACE,
        msg: format!("test-log-message"),
    };
    let item: Sitemty<ChannelEvents> = Ok(StreamItem::Log(item));
    let buf = Framable::make_frame(&item).unwrap();
    let len = u32::from_le_bytes(buf[12..16].try_into().unwrap());
    let item2: LogItem = decode_from_slice(&buf[20..20 + len as usize]).unwrap();
}

#[test]
fn test_frame_error() {
    use crate::channelevents::ChannelEvents;
    use crate::frame::decode_from_slice;
    let item: Sitemty<ChannelEvents> = Err(Error::with_msg_no_trace(format!("dummy-error-message")));
    let buf = Framable::make_frame(&item).unwrap();
    let len = u32::from_le_bytes(buf[12..16].try_into().unwrap());
    let tyid = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    if tyid != ERROR_FRAME_TYPE_ID {
        panic!("bad tyid");
    }
    eprintln!("buf len {}  len {}", buf.len(), len);
    let item2: Error = decode_from_slice(&buf[20..20 + len as usize]).unwrap();
}
