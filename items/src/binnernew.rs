pub enum ConnStatus {}

pub struct ConnStatusEvent {
    ts: u64,
    status: ConnStatus,
}

pub enum ChannelEvents {
    Status(ConnStatus),
    Data(),
}
