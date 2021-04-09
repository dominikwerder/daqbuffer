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
use crate::gen::Shape::Scalar;
use crate::timeunits::*;

#[test]
fn test_gen_test_data() {
    taskrun::run(async {
        gen_test_data().await?;
        Ok(())
    }).unwrap();
}

pub async fn gen_test_data() -> Result<(), Error> {
    let direnv = DirEnv {
        path: "../tmpdata".into(),
        ksprefix: "ks".into(),
    };
    let mut ensemble = Ensemble {
        nodes: vec![],
        channels: vec![],
        direnv: Arc::new(direnv),
    };
    {
        let chan = Channel {
            backend: "test".into(),
            keyspace: 3,
            name: "wave1".into(),
            time_bin_size: DAY,
            scalar_type: ScalarType::F32,
            shape: Shape::Wave(42),
            time_spacing: HOUR * 6,
        };
        ensemble.channels.push(chan);
    }
    let node0 = Node {
        host: "localhost".into(),
        port: 7780,
        split: 0,
    };
    let node1 = Node {
        host: "localhost".into(),
        port: 7781,
        split: 1,
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
    direnv: Arc<DirEnv>,
}

struct DirEnv {
    path: PathBuf,
    ksprefix: String,
}

struct Node {
    host: String,
    port: u16,
    split: u8,
}

impl Node {
    fn name(&self) -> String {
        format!("{}-{}", self.host, self.port)
    }
}

enum Shape {
    Scalar,
    Wave(usize),
}

struct Channel {
    keyspace: u8,
    backend: String,
    name: String,
    time_bin_size: u64,
    scalar_type: ScalarType,
    shape: Shape,
    time_spacing: u64,
}

async fn gen_node(node: &Node, ensemble: &Ensemble) -> Result<(), Error> {
    tokio::fs::create_dir_all(&ensemble.direnv.path).await?;
    for channel in &ensemble.channels {
        gen_channel(channel, node, ensemble).await?
    }
    Ok(())
}

async fn gen_channel(channel: &Channel, node: &Node, ensemble: &Ensemble) -> Result<(), Error> {
    let mut channel_path = ensemble.direnv.path.clone()
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
        ts += channel.time_spacing;
    }
    let ret = GenTimebinRes {
        ts,
    };
    Ok(ret)
}

async fn gen_datafile_header(file: &mut File, channel: &Channel) -> Result<(), Error> {
    let mut buf = BytesMut::with_capacity(1024);
    //use bytes::BufMut;
    let cnenc = channel.name.as_bytes();
    let len1 = cnenc.len() + 8;
    buf.put_i16(0);
    buf.put_i32(len1 as i32);
    buf.put(cnenc);
    buf.put_i32(len1 as i32);
    file.write_all(&buf).await?;
    Ok(())
}
