// TODO crate `err` pulls in all other dependencies in order to implement From<...> for Error.
// Refactor that...
// Crate `taskrun` also depends on `err`...

use archapp::events::PositionState;
use archapp::parse::PbFileReader;
use chrono::{TimeZone, Utc};
use clap::{crate_version, Parser};
use err::Error;
use netpod::query::RawEventsQuery;
use netpod::timeunits::*;
use netpod::AggKind;
use netpod::Channel;
use netpod::NanoRange;
use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;

#[derive(Debug)]
pub struct Error2;

#[derive(Debug, Parser)]
#[clap(name="DAQ buffer tools", version=crate_version!())]
pub struct Opts {
    #[clap(short, long, parse(from_occurrences))]
    pub verbose: u32,
    #[clap(subcommand)]
    pub subcmd: SubCmd,
}

#[derive(Debug, Parser)]
pub enum SubCmd {
    ConvertArchiverApplianceChannel(ConvertArchiverApplianceChannel),
}

#[derive(Debug, Parser)]
pub struct ConvertArchiverApplianceChannel {
    name: String,
    #[clap(about = "Look for archiver appliance data at given path")]
    input_dir: PathBuf,
    #[clap(about = "Generate Databuffer format at given path")]
    output_dir: PathBuf,
}

pub fn main() -> Result<(), Error> {
    taskrun::run(async {
        if false {
            return Err(Error::with_msg_no_trace(format!("unknown command")));
        }
        let opts = Opts::parse();
        eprintln!("Opts: {:?}", opts);
        match opts.subcmd {
            SubCmd::ConvertArchiverApplianceChannel(sub) => {
                //
                let channel = Channel {
                    backend: String::new(),
                    name: sub.name.into(),
                };
                let chandir = archapp::events::directory_for_channel_files(&channel, &sub.input_dir)?;
                eprintln!("channel path: {:?}", chandir);
                let files = archapp::events::find_files_for_channel(&sub.input_dir, &channel).await?;
                eprintln!("files: {:?}", files);
                let mut evstot = 0;
                for file in files {
                    eprintln!("try to open {:?}", file);
                    let fni = archapp::events::parse_data_filename(file.to_str().unwrap())?;
                    eprintln!("fni: {:?}", fni);
                    let ts0 = Utc.ymd(fni.year as i32, fni.month, 1).and_hms(0, 0, 0);
                    let ts1 = ts0.timestamp() as u64 * SEC + ts0.timestamp_subsec_nanos() as u64;
                    let _ = ts1;
                    let mut f1 = File::open(&file).await?;
                    let flen = f1.seek(SeekFrom::End(0)).await?;
                    eprintln!("flen: {}", flen);
                    f1.seek(SeekFrom::Start(0)).await?;
                    let mut pbr = PbFileReader::new(f1).await;
                    pbr.read_header().await?;
                    eprintln!("✓ read header  payload_type {:?}", pbr.payload_type());
                    let evq = RawEventsQuery {
                        channel: channel.clone(),
                        range: NanoRange {
                            beg: u64::MIN,
                            end: u64::MAX,
                        },
                        agg_kind: AggKind::Plain,
                        disk_io_buffer_size: 1024 * 4,
                        do_decompress: true,
                    };
                    let f1 = pbr.into_file();
                    // TODO can the positioning-logic maybe re-use the pbr?
                    let z = archapp::events::position_file_for_evq(f1, evq.clone(), fni.year).await?;
                    if let PositionState::Positioned(pos) = z.state {
                        let mut f1 = z.file;
                        f1.seek(SeekFrom::Start(0)).await?;
                        let mut pbr = PbFileReader::new(f1).await;
                        // TODO incorporate the read_header into the init. must not be forgotten.
                        pbr.read_header().await?;
                        pbr.file().seek(SeekFrom::Start(pos)).await?;
                        pbr.reset_io(pos);
                        eprintln!("POSITIONED 1 at {}", pbr.abspos());
                        let mut i1 = 0;
                        loop {
                            match pbr.read_msg().await {
                                Ok(Some(ei)) => {
                                    use items::{WithLen, WithTimestamps};
                                    let ei = ei.item;
                                    let tslast = if ei.len() > 0 { Some(ei.ts(ei.len() - 1)) } else { None };
                                    i1 += 1;
                                    if i1 < 20 {
                                        eprintln!("read msg from file {}  len {}  tslast {:?}", i1, ei.len(), tslast);
                                    }
                                    //let ei2 = ei.x_aggregate(&evq.agg_kind);
                                }
                                Ok(None) => {
                                    eprintln!("reached end of file");
                                    break;
                                }
                                Err(e) => {
                                    eprintln!("error while reading msg  {:?}", e);
                                    break;
                                }
                            }
                        }
                        eprintln!("read total {} events from file", i1);
                        evstot += i1;
                    } else {
                        eprintln!("Position fail.");
                    }
                }
                eprintln!("evstot {}", evstot);
                Ok(())
            }
        }
    })
}
