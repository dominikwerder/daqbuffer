use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Range {
    #[serde(rename = "type")]
    ty: String,
    #[serde(rename = "startDate")]
    beg: String,
    #[serde(rename = "endDate")]
    end: String,
}

// TODO implement Deserialize such that I recognize the different possible formats...
// I guess, when serializing, it's ok to use the fully qualified format throughout.
#[derive(Debug, Deserialize)]
pub struct ChannelList {}

#[derive(Debug, Deserialize)]
pub struct Query {
    range: Range,
    channels: ChannelList,
}