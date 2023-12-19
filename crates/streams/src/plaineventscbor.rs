use crate::cbor::events_stream_to_cbor_stream;
use crate::cbor::CborStream;
use crate::firsterr::non_empty;
use crate::firsterr::only_first_err;
use crate::plaineventsstream::dyn_events_stream;
use crate::tcprawclient::OpenBoxedBytesStreamsBox;
use err::Error;
use netpod::log::*;
use netpod::ChannelTypeConfigGen;
use netpod::ReqCtx;
use query::api4::events::PlainEventsQuery;

pub async fn plain_events_cbor(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    open_bytes: OpenBoxedBytesStreamsBox,
) -> Result<CborStream, Error> {
    let stream = dyn_events_stream(evq, ch_conf, ctx, open_bytes).await?;
    let stream = events_stream_to_cbor_stream(stream);
    let stream = non_empty(stream);
    let stream = only_first_err(stream);
    Ok(Box::pin(stream))
}
