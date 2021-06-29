use async_channel::{bounded, Receiver};
use err::Error;
use netpod::log::*;
use netpod::NodeConfigCached;
use protobuf::Message;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncReadExt;

pub mod generated;
#[cfg(test)]
mod test;

pub trait ItemSer {
    fn serialize(&self) -> Result<Vec<u8>, Error>;
}

impl<T> ItemSer for T
where
    T: Serialize,
{
    fn serialize(&self) -> Result<Vec<u8>, Error> {
        let u = serde_json::to_vec(self)?;
        Ok(u)
    }
}

fn unescape_archapp_msg(inp: &[u8]) -> Result<Vec<u8>, Error> {
    let mut ret = Vec::with_capacity(inp.len() * 5 / 4);
    let mut esc = false;
    for &k in inp.iter() {
        if k == 0x1b {
            esc = true;
        } else if esc {
            if k == 0x1 {
                ret.push(0x1b);
            } else if k == 0x2 {
                ret.push(0xa);
            } else if k == 0x3 {
                ret.push(0xd);
            } else {
                return Err(Error::with_msg("malformed escaped archapp message"));
            }
            esc = false;
        } else {
            ret.push(k);
        }
    }
    Ok(ret)
}

#[derive(Serialize)]
pub struct EpicsEventPayloadInfo {
    headers: Vec<(String, String)>,
    year: i32,
    pvname: String,
}

async fn read_pb_file(path: PathBuf) -> Result<EpicsEventPayloadInfo, Error> {
    let mut f1 = tokio::fs::File::open(path).await?;
    let mut buf = vec![0; 1024 * 4];
    {
        let mut i1 = 0;
        loop {
            let n = f1.read(&mut buf[i1..]).await?;
            if n == 0 {
                break;
            }
            i1 += n;
            if i1 >= buf.len() {
                break;
            }
        }
    }
    let mut j1 = 0;
    loop {
        let mut i2 = usize::MAX;
        for (i1, &k) in buf[j1..].iter().enumerate() {
            if k == 0xa {
                i2 = j1 + i1;
                break;
            }
        }
        if i2 != usize::MAX {
            //info!("got NL  {} .. {}", j1, i2);
            let m = unescape_archapp_msg(&buf[j1..i2])?;
            if j1 == 0 {
                let payload_info = crate::generated::EPICSEvent::PayloadInfo::parse_from_bytes(&m).unwrap();
                //info!("got payload_info: {:?}", payload_info);
                let z = EpicsEventPayloadInfo {
                    headers: payload_info
                        .get_headers()
                        .iter()
                        .map(|j| (j.get_name().to_string(), j.get_val().to_string()))
                        .collect(),
                    year: payload_info.get_year(),
                    pvname: payload_info.get_pvname().into(),
                };
                return Ok(z);
            } else {
                let scalar_double = crate::generated::EPICSEvent::ScalarDouble::parse_from_bytes(&m).unwrap();
                //info!("got scalar_double: {:?}", scalar_double);
            }
        } else {
            //info!("no more packets");
            break;
        }
        j1 = i2 + 1;
    }
    Err(Error::with_msg(format!("no header entry found in file")))
}

type RT1 = Box<dyn ItemSer + Send>;

pub async fn scan_files(
    _pairs: BTreeMap<String, String>,
    node_config: &NodeConfigCached,
) -> Result<Receiver<Result<RT1, Error>>, Error> {
    let aa = if let Some(aa) = &node_config.node.archiver_appliance {
        aa.clone()
    } else {
        return Err(Error::with_msg("no archiver appliance config"));
    };
    let (tx, rx) = bounded(16);
    let tx = Arc::new(tx);
    let tx2 = tx.clone();
    let block1 = async move {
        let mut paths = VecDeque::new();
        paths.push_back(aa.data_base_path);
        loop {
            if let Some(path) = paths.pop_back() {
                let meta = tokio::fs::metadata(&path).await?;
                if meta.is_dir() {
                    let mut rd = tokio::fs::read_dir(&path).await?;
                    loop {
                        match rd.next_entry().await {
                            Ok(item) => match item {
                                Some(item) => {
                                    paths.push_back(item.path());
                                }
                                None => {
                                    break;
                                }
                            },
                            Err(e) => {
                                tx.send(Err(e.into())).await.unwrap();
                            }
                        }
                    }
                } else if meta.is_file() {
                    //tx.send(Ok(Box::new(path.clone()) as RT1)).await?;
                    if path.to_str().unwrap().ends_with(".pb") {
                        let packet = read_pb_file(path.clone()).await?;
                        let pvn = packet.pvname.replace("-", "/");
                        let pvn = pvn.replace(":", "/");
                        let pre = "/arch/lts/ArchiverStore/";
                        let p3 = &path.to_str().unwrap()[pre.len()..];
                        let p3 = &p3[..p3.len() - 11];
                        if p3 != pvn {
                            tx.send(Ok(Box::new(serde_json::to_value(&packet)?) as RT1)).await?;
                            {
                                let s = format!("{} - {}", p3, packet.pvname);
                                tx.send(Ok(Box::new(serde_json::to_value(&s)?) as RT1)).await?;
                            }
                            tx.send(Ok(
                                Box::new(JsonValue::String(format!("MISMATCH --------------------"))) as RT1,
                            ))
                            .await?;
                        }
                    }
                }
            } else {
                break;
            }
        }
        Ok::<_, Error>(())
    };
    let block2 = async move {
        match block1.await {
            Ok(_) => {}
            Err(e) => match tx2.send(Err(e.into())).await {
                Ok(_) => {}
                Err(e) => {
                    error!("can not deliver error through channel: {:?}", e);
                }
            },
        }
    };
    tokio::spawn(block2);
    Ok(rx)
}
