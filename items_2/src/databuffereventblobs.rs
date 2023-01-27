use items::FrameType;
use items::FrameTypeInnerStatic;
use serde::Serialize;

pub struct DatabufferEventBlob {}

impl FrameTypeInnerStatic for DatabufferEventBlob {
    const FRAME_TYPE_ID: u32 = items::DATABUFFER_EVENT_BLOB_FRAME_TYPE_ID;
}

impl FrameType for DatabufferEventBlob {
    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeInnerStatic>::FRAME_TYPE_ID
    }
}

impl Serialize for DatabufferEventBlob {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        todo!()
    }
}
