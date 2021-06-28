use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, StreamExt};
use netpod::log::*;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::net::TcpStream;

pub async fn zmtp_00() -> Result<(), Error> {
    // PV:BSREADCONFIG
    let addr = "S10-CPPM-MOT0991:9999";
    let conn = tokio::net::TcpStream::connect(addr).await?;
    let mut zmtp = Zmtp::new(conn);
    while let Some(ev) = zmtp.next().await {
        info!("got zmtp event: {:?}", ev);
    }
    Ok(())
}

enum ConnState {
    Init,
}

struct Zmtp {
    conn: TcpStream,
    conn_state: ConnState,
    buf1: Vec<u8>,
    need_min: usize,
}

impl Zmtp {
    fn new(conn: TcpStream) -> Self {
        Self {
            conn,
            conn_state: ConnState::Init,
            buf1: vec![0; 1024],
            need_min: 4,
        }
    }
}

#[derive(Debug)]
struct ZmtpEvent {}

impl Stream for Zmtp {
    type Item = Result<ZmtpEvent, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let ConnState::Init = self.conn_state {
                // can it be that we already have enough bytes received in the buffer?
                let mut buf1 = mem::replace(&mut self.buf1, vec![]);
                let mut rbuf = ReadBuf::new(&mut buf1);
                let w = &mut self.conn;
                pin_mut!(w);
                let m1 = w.poll_read(cx, &mut rbuf);
                self.buf1 = buf1;
                match m1 {
                    Ready(item) => Pending,
                    Pending => Pending,
                }
            } else {
                Pending
            };
        }
    }
}
