use crate::frames::inmem::BoxedBytesStream;
use crate::plaineventscbor::plain_events_cbor;
use crate::tcprawclient::OpenBoxedBytesStreams;
use crate::tcprawclient::TEST_BACKEND;
use err::Error;
use futures_util::Future;
use futures_util::StreamExt;
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
use netpod::ChConf;
use netpod::ReqCtx;
use netpod::ScalarType;
use netpod::SfDbChannel;
use netpod::Shape;
use query::api4::events::EventsSubQuery;
use query::api4::events::PlainEventsQuery;
use std::pin::Pin;

#[test]
fn merged_events_cbor() {
    crate::test::runfut(merged_events_inner()).unwrap();
}

async fn merged_events_inner() -> Result<(), Error> {
    let ctx = ReqCtx::for_test();
    let ch_conf = ChConf::new(TEST_BACKEND, 1, ScalarType::F64, Shape::Scalar, "test-gen-i32-dim0-v00");
    let channel = SfDbChannel::from_name(ch_conf.backend(), ch_conf.name());
    let range = SeriesRange::TimeRange(NanoRange::from_date_time(
        "2023-12-18T05:10:00Z".parse().unwrap(),
        "2023-12-18T05:10:10Z".parse().unwrap(),
    ));
    let evq = PlainEventsQuery::new(channel, range);
    let open_bytes = StreamOpener::new();
    let open_bytes = Box::pin(open_bytes);
    let mut res = plain_events_cbor(&evq, ch_conf.into(), &ctx, open_bytes).await.unwrap();
    // TODO parse the cbor stream and assert
    while let Some(x) = res.next().await {
        let item = x?;
        let bytes = item.into_inner();
        eprintln!("bytes  len {}", bytes.len());
    }
    Ok(())
}

struct StreamOpener {}

impl StreamOpener {
    fn new() -> Self {
        Self {}
    }
}

impl OpenBoxedBytesStreams for StreamOpener {
    fn open(
        &self,
        subq: EventsSubQuery,
        _ctx: ReqCtx,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<BoxedBytesStream>, Error>> + Send>> {
        Box::pin(stream_opener(subq))
    }
}

async fn stream_opener(subq: EventsSubQuery) -> Result<Vec<BoxedBytesStream>, Error> {
    let mut streams = Vec::new();
    let stream = crate::generators::make_test_channel_events_bytes_stream(subq, 1, 0)?;
    streams.push(stream);
    Ok(streams)
}
