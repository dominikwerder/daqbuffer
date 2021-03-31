use err::Error;
use std::task::{Context, Poll};
use std::pin::Pin;
use tokio::io::AsyncRead;
//use std::future::Future;
//use futures_core::Stream;

pub async fn read_test_1() -> Result<netpod::BodyStream, Error> {
    let path = "/data/sf-databuffer/daq_swissfel/daq_swissfel_3/byTime/S10CB01-RIQM-DCP10:FOR-AMPLT/0000000000000018714/0000000012/0000000000086400000_00000_Data";
    let fin = tokio::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .await?;
    let stream = netpod::BodyStream {
        inner: Box::new(FileReader { file: fin }),
    };
    Ok(stream)
}

struct FileReader {
    file: tokio::fs::File,
}

impl futures_core::Stream for FileReader {
    type Item = Result<bytes::Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut buf2 = bytes::BytesMut::with_capacity(13);
        if buf2.as_mut().len() != 13 {
            panic!("todo prepare slice");
        }
        let mut buf = tokio::io::ReadBuf::new(buf2.as_mut());
        let g = Pin::new(&mut self.file).poll_read(cx, &mut buf);
        match g {
            Poll::Ready(Ok(_)) => {
                Poll::Ready(Some(Ok(buf2.freeze())))
            }
            Poll::Ready(Err(e)) => {
                Poll::Ready(Some(Err(Error::from(e))))
            }
            Poll::Pending => Poll::Pending
        }
    }

}
