use async_channel::{bounded, Receiver};
use bytes::{BufMut, BytesMut};
use err::Error;
use futures_util::FutureExt;
use netpod::NodeConfigCached;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    cmd: u16,
    payload_len: u16,
    type_type: u16,
    data_len: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FetchItem {
    Log(String),
    Message(Message),
}

pub async fn ca_connect_1(
    addr: String,
    _node_config: &NodeConfigCached,
) -> Result<Receiver<Result<FetchItem, Error>>, Error> {
    let (tx, rx) = bounded(16);
    let tx2 = tx.clone();
    tokio::task::spawn(
        async move {
            let mut conn = tokio::net::TcpStream::connect(addr).await?;
            let (mut inp, mut out) = conn.split();
            tx.send(Ok(FetchItem::Log(format!("connected")))).await?;
            let mut b2 = BytesMut::with_capacity(128);
            b2.put_u16(0);
            b2.put_u16(0);
            b2.put_u16(0);
            b2.put_u16(0xb);
            b2.put_u32(0);
            b2.put_u32(0);
            out.write_all(&b2).await?;
            tx.send(Ok(FetchItem::Log(format!("written")))).await?;
            let mut buf = [0; 64];
            let n1 = inp.read(&mut buf).await?;
            tx.send(Ok(FetchItem::Log(format!("received: {} {:?}", n1, buf))))
                .await?;
            Ok::<_, Error>(())
        }
        .then({
            move |item| async move {
                match item {
                    Ok(_) => {}
                    Err(e) => {
                        tx2.send(Ok(FetchItem::Log(format!("Seeing error: {:?}", e)))).await?;
                    }
                }
                Ok::<_, Error>(())
            }
        }),
    );
    Ok(rx)
}
