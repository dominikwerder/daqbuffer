use crate::ChannelConfigExt;
use bitshuffle::bitshuffle_compress;
use bytes::{BufMut, BytesMut};
use err::Error;
use netpod::ScalarType;
use netpod::{timeunits::*, Channel, ChannelConfig, Node, Shape};
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

#[test]
fn test_gen_test_data() {
    let res = taskrun::run(async {
        gen_test_data().await?;
        Ok(())
    });
    info!("{:?}", res);
    res.unwrap();
}

pub async fn gen_test_data() -> Result<(), Error> {
    let data_base_path = PathBuf::from("../tmpdata");
    let ksprefix = String::from("ks");
    let mut ensemble = Ensemble {
        nodes: vec![],
        channels: vec![],
    };
    {
        let chn = ChannelGenProps {
            config: ChannelConfig {
                channel: Channel {
                    backend: "test".into(),
                    name: "wave1".into(),
                },
                keyspace: 3,
                time_bin_size: DAY,
                array: true,
                scalar_type: ScalarType::F64,
                shape: Shape::Wave(17),
                big_endian: true,
                compression: true,
            },
            time_spacing: MS * 2000,
        };
        ensemble.channels.push(chn);
    }
    for i1 in 0..13 {
        let node = Node {
            id: i1,
            host: "localhost".into(),
            port: 7780 + i1 as u16,
            port_raw: 7780 + i1 as u16 + 100,
            split: i1,
            data_base_path: data_base_path.join(format!("node{:02}", i1)),
            ksprefix: ksprefix.clone(),
        };
        ensemble.nodes.push(node);
    }
    for node in &ensemble.nodes {
        gen_node(node, &ensemble).await?;
    }
    Ok(())
}

struct Ensemble {
    nodes: Vec<Node>,
    channels: Vec<ChannelGenProps>,
}

pub struct ChannelGenProps {
    config: ChannelConfig,
    time_spacing: u64,
}

async fn gen_node(node: &Node, ensemble: &Ensemble) -> Result<(), Error> {
    for chn in &ensemble.channels {
        gen_channel(chn, node, ensemble).await?
    }
    Ok(())
}

async fn gen_channel(chn: &ChannelGenProps, node: &Node, ensemble: &Ensemble) -> Result<(), Error> {
    let config_path = node.data_base_path.join("config").join(&chn.config.channel.name);
    let channel_path = node
        .data_base_path
        .join(format!("{}_{}", node.ksprefix, chn.config.keyspace))
        .join("byTime")
        .join(&chn.config.channel.name);
    tokio::fs::create_dir_all(&channel_path).await?;
    gen_config(&config_path, &chn.config, node, ensemble)
        .await
        .map_err(|k| Error::with_msg(format!("can not generate config {:?}", k)))?;
    let mut evix = 0;
    let mut ts = 0;
    while ts < DAY {
        let res = gen_timebin(evix, ts, chn.time_spacing, &channel_path, &chn.config, node, ensemble).await?;
        evix = res.evix;
        ts = res.ts;
    }
    Ok(())
}

async fn gen_config(
    config_path: &Path,
    config: &ChannelConfig,
    _node: &Node,
    _ensemble: &Ensemble,
) -> Result<(), Error> {
    let path = config_path.join("latest");
    tokio::fs::create_dir_all(&path).await?;
    let path = path.join("00000_Config");
    info!("try to open  {:?}", path);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await?;
    let mut buf = BytesMut::with_capacity(1024 * 1);
    let ver = 0;
    buf.put_i16(ver);
    let cnenc = config.channel.name.as_bytes();
    let len1 = cnenc.len() + 8;
    buf.put_i32(len1 as i32);
    buf.put(cnenc);
    buf.put_i32(len1 as i32);

    let ts = 0;
    let pulse = 0;
    let sc = 0;
    let status = 0;
    let bb = 0;
    let modulo = 0;
    let offset = 0;
    let precision = 0;
    let p1 = buf.len();
    buf.put_i32(0x20202020);
    buf.put_i64(ts);
    buf.put_i64(pulse);
    buf.put_i32(config.keyspace as i32);
    buf.put_i64(config.time_bin_size as i64);
    buf.put_i32(sc);
    buf.put_i32(status);
    buf.put_i8(bb);
    buf.put_i32(modulo);
    buf.put_i32(offset);
    buf.put_i16(precision);

    {
        // this len does not include itself and there seems to be no copy of it afterwards.
        let p3 = buf.len();
        buf.put_i32(404040);
        buf.put_u8(config.dtflags());
        buf.put_u8(config.scalar_type.index());
        if config.compression {
            let method = 0;
            buf.put_i8(method);
        }
        match config.shape {
            Shape::Scalar => {}
            Shape::Wave(k) => {
                buf.put_i8(1);
                buf.put_i32(k as i32);
            }
        }
        let len = buf.len() - p3 - 4;
        buf.as_mut()[p3..].as_mut().put_i32(len as i32);
    }

    // source name
    buf.put_i32(-1);
    // unit
    buf.put_i32(-1);
    // description
    buf.put_i32(-1);
    // optional fields
    buf.put_i32(-1);
    // value converter
    buf.put_i32(-1);

    let p2 = buf.len();
    let len = p2 - p1 + 4;
    buf.put_i32(len as i32);
    buf.as_mut()[p1..].as_mut().put_i32(len as i32);
    file.write(&buf).await?;
    Ok(())
}

struct GenTimebinRes {
    evix: u64,
    ts: u64,
}

async fn gen_timebin(
    evix: u64,
    ts: u64,
    ts_spacing: u64,
    channel_path: &Path,
    config: &ChannelConfig,
    node: &Node,
    ensemble: &Ensemble,
) -> Result<GenTimebinRes, Error> {
    let tb = ts / config.time_bin_size;
    let path = channel_path
        .join(format!("{:019}", tb))
        .join(format!("{:010}", node.split));
    tokio::fs::create_dir_all(&path).await?;
    let path = path.join(format!("{:019}_{:05}_Data", config.time_bin_size / MS, 0));
    info!("open file {:?}", path);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await?;
    gen_datafile_header(&mut file, config).await?;
    let mut evix = evix;
    let mut ts = ts;
    let tsmax = (tb + 1) * config.time_bin_size;
    while ts < tsmax {
        if evix % ensemble.nodes.len() as u64 == node.split as u64 {
            gen_event(&mut file, evix, ts, config).await?;
        }
        evix += 1;
        ts += ts_spacing;
    }
    let ret = GenTimebinRes { evix, ts };
    Ok(ret)
}

async fn gen_datafile_header(file: &mut File, config: &ChannelConfig) -> Result<(), Error> {
    let mut buf = BytesMut::with_capacity(1024);
    let cnenc = config.channel.name.as_bytes();
    let len1 = cnenc.len() + 8;
    buf.put_i16(0);
    buf.put_i32(len1 as i32);
    buf.put(cnenc);
    buf.put_i32(len1 as i32);
    file.write_all(&buf).await?;
    Ok(())
}

async fn gen_event(file: &mut File, evix: u64, ts: u64, config: &ChannelConfig) -> Result<(), Error> {
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
    if config.compression {
        match config.shape {
            Shape::Wave(ele_count) => {
                buf.put_u8(COMPRESSION | ARRAY | SHAPE | BIG_ENDIAN);
                buf.put_u8(config.scalar_type.index());
                let comp_method = 0 as u8;
                buf.put_u8(comp_method);
                buf.put_u8(1);
                buf.put_u32(ele_count as u32);
                match &config.scalar_type {
                    ScalarType::F64 => {
                        let ele_size = 8;
                        let mut vals = vec![0; (ele_size * ele_count) as usize];
                        for i1 in 0..ele_count {
                            let v = evix as f64;
                            let a = v.to_be_bytes();
                            let mut c1 = std::io::Cursor::new(&mut vals);
                            use std::io::{Seek, SeekFrom};
                            c1.seek(SeekFrom::Start(i1 as u64 * ele_size as u64))?;
                            std::io::Write::write_all(&mut c1, &a)?;
                        }
                        let mut comp = vec![0u8; (ele_size * ele_count + 64) as usize];
                        let n1 =
                            bitshuffle_compress(&vals, &mut comp, ele_count as usize, ele_size as usize, 0).unwrap();
                        buf.put_u64(vals.len() as u64);
                        let comp_block_size = 0;
                        buf.put_u32(comp_block_size);
                        buf.put(&comp[..n1]);
                    }
                    _ => todo!(),
                }
            }
            _ => todo!(),
        }
    } else {
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
