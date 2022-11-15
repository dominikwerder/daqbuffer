use clap::Parser;
use err::Error;
use netpod::log::*;
use netpod::{ByteOrder, ByteSize, Channel, ChannelConfig, NanoRange, Shape};
use std::path::PathBuf;
use streams::eventchunker::{EventChunker, EventChunkerConf};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

#[derive(Debug, Parser)]
#[clap(name = "DAQ buffer tools", version)]
pub struct Opts {
    #[clap(short, long, parse(from_occurrences))]
    pub verbose: u32,
    #[clap(subcommand)]
    pub subcmd: SubCmd,
}

#[derive(Debug, Parser)]
pub enum SubCmd {
    ReadDatabufferConfigfile(ReadDatabufferConfigfile),
    ReadDatabufferDatafile(ReadDatabufferDatafile),
}

#[derive(Debug, Parser)]
pub struct ReadDatabufferConfigfile {
    #[clap(long)]
    configfile: PathBuf,
}

#[derive(Debug, Parser)]
pub struct ReadDatabufferDatafile {
    #[clap(long)]
    configfile: PathBuf,
    #[clap(long)]
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
                let file = File::open(&sub.datafile).await?;
                let disk_io_tune = netpod::DiskIoTune::default();
                let inp = Box::pin(disk::file_content_stream(file, disk_io_tune));
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
                    byte_order: ByteOrder::LE,
                };
                let range = NanoRange {
                    beg: u64::MIN,
                    end: u64::MAX,
                };
                let stats_conf = EventChunkerConf {
                    disk_stats_every: ByteSize::mb(2),
                };
                let chunks = EventChunker::from_start(
                    inp,
                    channel_config.clone(),
                    range,
                    stats_conf,
                    sub.datafile.clone(),
                    false,
                    true,
                );
                use futures_util::stream::StreamExt;
                use items::WithLen;
                //let evs = EventValuesDim0Case::<f64>::new();
                let mut stream = disk::decode::EventsItemStream::new(Box::pin(chunks));
                while let Some(item) = stream.next().await {
                    let item = item?;
                    match item {
                        items::StreamItem::DataItem(item) => {
                            match item {
                                items::RangeCompletableItem::RangeComplete => {
                                    warn!("RangeComplete");
                                }
                                items::RangeCompletableItem::Data(item) => {
                                    info!("{:?}  ({} events)", item, item.len());
                                }
                            };
                        }
                        items::StreamItem::Log(k) => {
                            eprintln!("Log item {:?}", k);
                        }
                        items::StreamItem::Stats(k) => {
                            eprintln!("Stats item {:?}", k);
                        }
                    }
                }
                Ok(())
            }
        }
    })
}
