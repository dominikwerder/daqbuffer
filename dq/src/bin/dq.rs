use clap::ArgAction;
use clap::Parser;
use err::Error;
#[allow(unused)]
use netpod::log::*;
use netpod::ByteOrder;
use netpod::ByteSize;
use netpod::Channel;
use netpod::ChannelConfig;
use netpod::NanoRange;
use netpod::Shape;
use std::path::PathBuf;
use streams::eventchunker::EventChunker;
use streams::eventchunker::EventChunkerConf;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

#[derive(Debug, Parser)]
#[command(name = "DAQ buffer tools", author, version)]
pub struct Opts {
    #[arg(short, long, action(ArgAction::Count))]
    pub verbose: u32,
    #[command(subcommand)]
    pub subcmd: SubCmd,
}

#[derive(Debug, Parser)]
pub enum SubCmd {
    ReadDatabufferConfigfile(ReadDatabufferConfigfile),
    ReadDatabufferDatafile(ReadDatabufferDatafile),
}

#[derive(Debug, Parser)]
pub struct ReadDatabufferConfigfile {
    #[arg(long)]
    configfile: PathBuf,
}

#[derive(Debug, Parser)]
pub struct ReadDatabufferDatafile {
    #[arg(long)]
    configfile: PathBuf,
    #[arg(long)]
    datafile: PathBuf,
}

pub fn main() -> Result<(), Error> {
    taskrun::run(async {
        if false {
            return Err(Error::with_msg_no_trace(format!("unknown command")));
        }
        let opts = Opts::parse();
        match opts.subcmd {
            SubCmd::ReadDatabufferConfigfile(sub) => {
                let mut file = File::open(&sub.configfile).await?;
                let meta = file.metadata().await?;
                let mut buf = vec![0; meta.len() as usize];
                file.read_exact(&mut buf).await?;
                drop(file);
                let config = match parse::channelconfig::parse_config(&buf) {
                    Ok(k) => k.1,
                    Err(e) => return Err(Error::with_msg_no_trace(format!("can not parse: {:?}", e))),
                };
                eprintln!("Read config: {:?}", config);
                Ok(())
            }
            SubCmd::ReadDatabufferDatafile(sub) => {
                let mut file = File::open(&sub.configfile).await?;
                let meta = file.metadata().await?;
                let mut buf = vec![0; meta.len() as usize];
                file.read_exact(&mut buf).await?;
                drop(file);
                let config = match parse::channelconfig::parse_config(&buf) {
                    Ok(k) => k.1,
                    Err(e) => return Err(Error::with_msg_no_trace(format!("can not parse: {:?}", e))),
                };
                eprintln!("Read config: {:?}", config);
                let path = sub.datafile;
                let file = File::open(&path).await?;
                let disk_io_tune = netpod::DiskIoTune::default();
                let inp = Box::pin(disk::file_content_stream(path.clone(), file, disk_io_tune));
                let ce = &config.entries[0];
                let channel_config = ChannelConfig {
                    channel: Channel {
                        backend: String::new(),
                        name: config.channel_name.clone(),
                        series: None,
                    },
                    keyspace: ce.ks as u8,
                    time_bin_size: ce.bs,
                    scalar_type: ce.scalar_type.clone(),
                    compression: false,
                    shape: Shape::Scalar,
                    array: false,
                    byte_order: ByteOrder::Little,
                };
                let range = NanoRange {
                    beg: u64::MIN,
                    end: u64::MAX,
                };
                let stats_conf = EventChunkerConf {
                    disk_stats_every: ByteSize::mb(2),
                };
                let _chunks =
                    EventChunker::from_start(inp, channel_config.clone(), range, stats_conf, path, false, true);
                err::todo();
                Ok(())
            }
        }
    })
}
