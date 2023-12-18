//! Delivers event data.
//!
//! Delivers event data (not yet time-binned) from local storage and provides client functions
//! to request such data from nodes.

use crate::frames::eventsfromframes::EventsFromFrames;
use crate::frames::inmem::BoxedBytesStream;
use crate::frames::inmem::InMemoryFrameStream;
use crate::frames::inmem::TcpReadAsBytes;
use err::Error;
use futures_util::Future;
use futures_util::Stream;
use http::Uri;
use httpclient::body_bytes;
use httpclient::http;
use items_0::framable::FrameTypeInnerStatic;
use items_0::streamitem::sitem_data;
use items_0::streamitem::Sitemty;
use items_2::eventfull::EventFull;
use items_2::framable::EventQueryJsonStringFrame;
use items_2::framable::Framable;
use items_2::frame::make_term_frame;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::ByteSize;
use netpod::ChannelTypeConfigGen;
use netpod::Cluster;
use netpod::Node;
use netpod::ReqCtx;
use netpod::APP_OCTET;
use query::api4::events::EventsSubQuery;
use query::api4::events::EventsSubQuerySelect;
use query::api4::events::EventsSubQuerySettings;
use query::api4::events::Frame1Parts;
use query::transform::TransformQuery;
use serde::de::DeserializeOwned;
use std::fmt;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub const TEST_BACKEND: &str = "testbackend-00";

pub trait OpenBoxedBytesStreams {
    fn open(
        &self,
        subq: EventsSubQuery,
        ctx: ReqCtx,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<BoxedBytesStream>, Error>> + Send>>;
}

pub type OpenBoxedBytesStreamsBox = Pin<Box<dyn OpenBoxedBytesStreams + Send>>;

pub fn make_node_command_frame(query: EventsSubQuery) -> Result<EventQueryJsonStringFrame, Error> {
    let obj = Frame1Parts::new(query);
    let ret = serde_json::to_string(&obj)?;
    Ok(EventQueryJsonStringFrame(ret))
}

pub async fn x_processed_event_blobs_stream_from_node_tcp(
    subq: EventsSubQuery,
    node: Node,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    let addr = format!("{}:{}", node.host, node.port_raw);
    debug!("x_processed_event_blobs_stream_from_node  to: {addr}",);
    let frame1 = make_node_command_frame(subq.clone())?;
    let net = TcpStream::connect(addr.clone()).await?;
    let (netin, mut netout) = net.into_split();
    let item = sitem_data(frame1);
    let buf = item.make_frame()?;
    netout.write_all(&buf).await?;
    let buf = make_term_frame()?;
    netout.write_all(&buf).await?;
    netout.flush().await?;
    netout.forget();
    let inp = Box::pin(TcpReadAsBytes::new(netin)) as BoxedBytesStream;
    let frames = InMemoryFrameStream::new(inp, subq.inmem_bufcap());
    let frames = Box::pin(frames);
    let items = EventsFromFrames::new(frames, addr);
    Ok(Box::pin(items))
}

pub async fn x_processed_event_blobs_stream_from_node_http(
    subq: EventsSubQuery,
    node: Node,
    ctx: &ReqCtx,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    use http::header;
    use http::Method;
    use http::Request;
    use httpclient::hyper;
    use hyper::StatusCode;

    let frame1 = make_node_command_frame(subq.clone())?;
    let item = sitem_data(frame1.clone());
    let buf = item.make_frame()?;

    let url = node.baseurl().join("/api/4/private/eventdata/frames").unwrap();
    debug!("open_event_data_streams_http  post  {url}");
    let uri: Uri = url.as_str().parse().unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri(&uri)
        .header(header::HOST, uri.host().unwrap())
        .header(header::ACCEPT, APP_OCTET)
        .header(ctx.header_name(), ctx.header_value())
        .body(body_bytes(buf))
        .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
    let mut client = httpclient::connect_client(req.uri()).await?;
    let res = client
        .send_request(req)
        .await
        .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
    if res.status() != StatusCode::OK {
        error!("Server error  {:?}", res);
        let (head, body) = res.into_parts();
        let buf = httpclient::read_body_bytes(body).await?;
        let s = String::from_utf8_lossy(&buf);
        return Err(Error::with_msg(format!(
            concat!(
                "Server error  {:?}\n",
                "---------------------- message from http body:\n",
                "{}\n",
                "---------------------- end of http body",
            ),
            head, s
        )));
    }
    let (_head, body) = res.into_parts();
    let inp = Box::pin(httpclient::IncomingStream::new(body)) as BoxedBytesStream;
    let frames = InMemoryFrameStream::new(inp, subq.inmem_bufcap());
    let frames = Box::pin(frames);
    let stream = EventsFromFrames::new(frames, url.to_string());
    debug!("open_event_data_streams_http  done  {url}");
    Ok(Box::pin(stream))
}

// Currently used only for the python data api3 protocol endpoint.
// TODO merge with main method.
pub async fn x_processed_event_blobs_stream_from_node(
    subq: EventsSubQuery,
    node: Node,
    ctx: ReqCtx,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    if true {
        x_processed_event_blobs_stream_from_node_http(subq, node, &ctx).await
    } else {
        x_processed_event_blobs_stream_from_node_tcp(subq, node).await
    }
}

pub type BoxedStream<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;

#[allow(unused)]
async fn open_event_data_streams_tcp<T>(subq: EventsSubQuery, cluster: &Cluster) -> Result<Vec<BoxedStream<T>>, Error>
where
    // TODO group bounds in new trait
    T: FrameTypeInnerStatic + DeserializeOwned + Send + Unpin + fmt::Debug + 'static,
{
    // TODO when unit tests established, change to async connect:
    let frame1 = make_node_command_frame(subq.clone())?;
    let mut streams = Vec::new();
    for node in &cluster.nodes {
        let addr = format!("{}:{}", node.host, node.port_raw);
        debug!("open_tcp_streams  to: {addr}");
        let net = TcpStream::connect(addr.clone()).await?;
        let (netin, mut netout) = net.into_split();
        let item = sitem_data(frame1.clone());
        let buf = item.make_frame()?;
        netout.write_all(&buf).await?;
        let buf = make_term_frame()?;
        netout.write_all(&buf).await?;
        netout.flush().await?;
        netout.forget();
        // TODO for images, we need larger buffer capacity
        let inp = Box::pin(TcpReadAsBytes::new(netin)) as BoxedBytesStream;
        let frames = InMemoryFrameStream::new(inp, subq.inmem_bufcap());
        let frames = Box::pin(frames);
        let stream = EventsFromFrames::<T>::new(frames, addr);
        streams.push(Box::pin(stream) as _);
    }
    Ok(streams)
}

pub fn container_stream_from_bytes_stream<T>(
    inp: BoxedBytesStream,
    bufcap: ByteSize,
    dbgdesc: String,
) -> Result<impl Stream<Item = Sitemty<T>>, Error>
where
    T: FrameTypeInnerStatic + DeserializeOwned + Send + Unpin + fmt::Debug + 'static,
{
    let frames = InMemoryFrameStream::new(inp, bufcap);
    // TODO let EventsFromFrames accept also non-boxed input?
    let frames = Box::pin(frames);
    let stream = EventsFromFrames::<T>::new(frames, dbgdesc);
    Ok(stream)
}

pub fn make_sub_query<SUB>(
    ch_conf: ChannelTypeConfigGen,
    range: SeriesRange,
    transform: TransformQuery,
    test_do_wasm: Option<&str>,
    sub: SUB,
    ctx: &ReqCtx,
) -> EventsSubQuery
where
    SUB: Into<EventsSubQuerySettings>,
{
    let mut select = EventsSubQuerySelect::new(ch_conf, range, transform);
    if let Some(wasm1) = test_do_wasm {
        select.set_wasm1(wasm1.into());
    }
    let settings = sub.into();
    let subq = EventsSubQuery::from_parts(select, settings, ctx.reqid().into());
    subq
}
