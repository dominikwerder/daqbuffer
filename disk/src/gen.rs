#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use std::task::{Context, Poll};
use std::future::Future;
use futures_core::Stream;
use futures_util::future::FusedFuture;
use futures_util::{pin_mut, StreamExt};
use std::pin::Pin;
use tokio::io::AsyncRead;
use tokio::fs::File;
use bytes::{Bytes, BytesMut, BufMut, Buf};
use std::path::PathBuf;
use bitshuffle::bitshuffle_compress;
use netpod::ScalarType;
use std::sync::Arc;
use crate::gen::Shape::Scalar;

pub async fn gen_test_data() -> Result<(), Error> {
    let mut ensemble = Ensemble {
        nodes: vec![],
        channels: vec![],
    };
    {
        let chan = Channel {
            backend: "test".into(),
            keyspace: 3,
            name: "wave1".into(),
            scalar_type: ScalarType::F32,
            shape: Shape::Wave(42),
            time_spacing: 420,
        };
        ensemble.channels.push(chan);
    }
    let direnv = DirEnv {
        path: "./tmpdata".into(),
    };
    let direnv = Arc::new(direnv);
    let node1 = Node {
        host: "localhost".into(),
        port: 7780,
        direnv: direnv.clone(),
    };
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

struct DirEnv {
    path: PathBuf,
}

struct Node {
    host: String,
    port: u16,
    direnv: Arc<DirEnv>,
}

impl Node {
    fn name(&self) -> String {
        format!("{}:{}", self.host, self.port)
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
    scalar_type: ScalarType,
    shape: Shape,
    time_spacing: u64,
}

async fn gen_node(node: &Node, ensemble: &Ensemble) -> Result<(), Error> {
    tokio::fs::create_dir_all(&node.direnv.path).await?;
    Ok(())
}
