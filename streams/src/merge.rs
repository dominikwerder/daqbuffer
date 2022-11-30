pub mod mergedstream;

use crate::frames::eventsfromframes::EventsFromFrames;
use crate::frames::inmem::InMemoryFrameAsyncReadStream;
use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items::frame::make_frame;
use items::frame::make_term_frame;
use items::sitem_data;
use items::EventQueryJsonStringFrame;
use items::Sitemty;
use netpod::log::*;
use netpod::Cluster;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub type BoxedStream<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;

pub async fn open_tcp_streams<Q, T>(query: Q, cluster: &Cluster) -> Result<Vec<BoxedStream<T>>, Error>
where
    Q: serde::Serialize,
    // Group bounds in new trait
    T: items::FrameTypeInnerStatic + serde::de::DeserializeOwned + Send + Unpin + 'static,
{
    // TODO when unit tests established, change to async connect:
    let mut streams = Vec::new();
    for node in &cluster.nodes {
        debug!("open_tcp_streams  to: {}:{}", node.host, node.port_raw);
        let net = TcpStream::connect(format!("{}:{}", node.host, node.port_raw)).await?;
        let qjs = serde_json::to_string(&query)?;
        let (netin, mut netout) = net.into_split();
        let item = EventQueryJsonStringFrame(qjs);
        let item = sitem_data(item);
        let buf = make_frame(&item)?;
        netout.write_all(&buf).await?;
        let buf = make_term_frame()?;
        netout.write_all(&buf).await?;
        netout.flush().await?;
        netout.forget();
        // TODO for images, we need larger buffer capacity
        let frames = InMemoryFrameAsyncReadStream::new(netin, 1024 * 128);
        let stream = EventsFromFrames::<_, T>::new(frames);
        let stream = stream.inspect(|x| {
            items::on_sitemty_range_complete!(x, warn!("RangeComplete SEEN IN RECEIVED TCP STREAM"));
        });
        streams.push(Box::pin(stream) as _);
    }
    Ok(streams)
}
