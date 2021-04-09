#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use std::task::{Context, Poll};
use std::future::Future;
use futures_core::Stream;
use futures_util::future::FusedFuture;
use futures_util::{pin_mut, StreamExt};
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::fs::{OpenOptions, File};
use bytes::{Bytes, BytesMut, BufMut, Buf};
use std::path::{Path, PathBuf};
use bitshuffle::bitshuffle_compress;
use netpod::ScalarType;
use std::sync::Arc;
use crate::timeunits::*;
use netpod::{Node, Channel, Shape};

#[test]
fn test_gen_test_data() {
    taskrun::run(async {
        gen_test_data().await?;
        Ok(())
    }).unwrap();
}

pub async fn gen_test_data() -> Result<(), Error> {
    let data_base_path = PathBuf::from("../tmpdata");
    let ksprefix = String::from("ks");
    let mut ensemble = Ensemble {
        nodes: vec![],
        channels: vec![],
    };
    {
        let chan = Channel {
            backend: "test".into(),
            keyspace: 3,
            name: "wave1".into(),
            time_bin_size: DAY,
            scalar_type: ScalarType::F64,
            shape: Shape::Wave(9),
            compression: true,
            time_spacing: HOUR * 6,
        };
        ensemble.channels.push(chan);
    }
    let node0 = Node {
        host: "localhost".into(),
        port: 7780,
        split: 0,
        data_base_path: data_base_path.clone(),
        ksprefix: ksprefix.clone(),
    };
    let node1 = Node {
        host: "localhost".into(),
        port: 7781,
        split: 1,
        data_base_path: data_base_path.clone(),
        ksprefix: ksprefix.clone(),
    };
    ensemble.nodes.push(node0);
    ensemble.nodes.push(node1);
    for node in &ensemble.nodes {
        gen_node(node, &ensemble).await?;
    }
    Ok(())
}

struct Ensemble {
    nodes: Vec<Node>,
    channels: Vec<Channel>,
}

async fn gen_node(node: &Node, ensemble: &Ensemble) -> Result<(), Error> {
    tokio::fs::create_dir_all(&ensemble.direnv.path).await?;
    for channel in &ensemble.channels {
        gen_channel(channel, node, ensemble).await?
    }
    Ok(())
}

async fn gen_channel(channel: &Channel, node: &Node, ensemble: &Ensemble) -> Result<(), Error> {
    let channel_path = ensemble.direnv.path.clone()
    .join(node.name())
    .join(format!("{}_{}", ensemble.direnv.ksprefix, channel.keyspace))
    .join("byTime")
    .join(&channel.name);
    tokio::fs::create_dir_all(&channel_path).await?;
    let mut ts = 0;
    while ts < DAY {
        let res = gen_timebin(ts, &channel_path, channel, node, ensemble).await?;
        ts = res.ts;
    }
    Ok(())
}

struct GenTimebinRes {
    ts: u64,
}

async fn gen_timebin(ts: u64, channel_path: &Path, channel: &Channel, node: &Node, ensemble: &Ensemble) -> Result<GenTimebinRes, Error> {
    let tb = ts / channel.time_bin_size;
    let path = channel_path.join(format!("{:019}", tb)).join(format!("{:010}", node.split));
    tokio::fs::create_dir_all(&path).await?;
    let path = path.join(format!("{:019}_{:05}_Data", channel.time_bin_size / MS, 0));
    info!("open file {:?}", path);
    let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(path).await?;
    gen_datafile_header(&mut file, channel).await?;
    let mut ts = ts;
    let tsmax = (tb + 1) * channel.time_bin_size;
    while ts < tsmax {
        trace!("gen ts {}", ts);
        gen_event(&mut file, ts, channel).await?;
        ts += channel.time_spacing;
    }
    let ret = GenTimebinRes {
        ts,
    };
    Ok(ret)
}

async fn gen_datafile_header(file: &mut File, channel: &Channel) -> Result<(), Error> {
    let mut buf = BytesMut::with_capacity(1024);
    let cnenc = channel.name.as_bytes();
    let len1 = cnenc.len() + 8;
    buf.put_i16(0);
    buf.put_i32(len1 as i32);
    buf.put(cnenc);
    buf.put_i32(len1 as i32);
    file.write_all(&buf).await?;
    Ok(())
}

async fn gen_event(file: &mut File, ts: u64, channel: &Channel) -> Result<(), Error> {
    let mut buf = BytesMut::with_capacity(1024 * 16);
    buf.put_i32(0xcafecafe as u32 as i32);
    buf.put_u64(0xcafecafe);
    buf.put_u64(ts);
    buf.put_u64(2323);
    buf.put_u64(0xcafecafe);
    buf.put_u8(0);
    buf.put_u8(0);
    buf.put_i32(-1);
    use crate::dtflags::*;
    if channel.compression {
        match channel.shape {
            Shape::Wave(ele_count) => {
                buf.put_u8(COMPRESSION | ARRAY | SHAPE | BIG_ENDIAN);
                buf.put_u8(channel.scalar_type.index());
                let comp_method = 0 as u8;
                buf.put_u8(comp_method);
                buf.put_u8(1);
                buf.put_u32(ele_count as u32);
                match &channel.scalar_type {
                    ScalarType::F64 => {
                        let ele_size = 8;
                        let mut vals = vec![0; ele_size * ele_count];
                        for i1 in 0..ele_count {
                            let v = 1.22 as f64;
                            let a = v.to_be_bytes();
                            let mut c1 = std::io::Cursor::new(&mut vals);
                            use std::io::{Seek, SeekFrom};
                            c1.seek(SeekFrom::Start(i1 as u64 * ele_size as u64))?;
                            std::io::Write::write_all(&mut c1, &a)?;
                        }
                        let mut comp = vec![0u8; ele_size * ele_count + 64];
                        let n1 = bitshuffle_compress(&vals, &mut comp, ele_count, ele_size, 0).unwrap();
                        trace!("comp size {}   {}e-2", n1, 100 * n1 / vals.len());
                        buf.put_u64(vals.len() as u64);
                        let comp_block_size = 0;
                        buf.put_u32(comp_block_size);
                        buf.put(comp.as_slice());
                    }
                    _ => todo!()
                }
            }
            _ => todo!()
        }
    }
    else {
        todo!()
    }
    {
        let len = buf.len() as u32 + 4;
        buf.put_u32(len);
        buf.as_mut().put_u32(len);
    }
    file.write_all(buf.as_ref()).await?;
    Ok(())
}
