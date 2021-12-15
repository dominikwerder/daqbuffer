use clap::{crate_version, Parser};
use err::Error;
use std::path::PathBuf;

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
    #[clap(about = "Convert a channel from the Archiver Appliance into Databuffer format.")]
    ConvertArchiverApplianceChannel(ConvertArchiverApplianceChannel),
}

#[derive(Debug, Parser)]
pub struct ConvertArchiverApplianceChannel {
    #[clap(
        long,
        about = "Prefix for keyspaces, e.g. specify `daq` to get scalar keyspace directory `daq_2`"
    )]
    keyspace_prefix: String,
    #[clap(long, about = "Name of the channel to convert")]
    channel_name: String,
    #[clap(long, about = "Look for archiver appliance data at given path")]
    input_dir: PathBuf,
    #[clap(long, about = "Generate Databuffer format at given path")]
    output_dir: PathBuf,
}

pub fn main() -> Result<(), Error> {
    taskrun::run(async {
        if false {
            return Err(Error::with_msg_no_trace(format!("unknown command")));
        }
        let opts = Opts::parse();
        match opts.subcmd {
            SubCmd::ConvertArchiverApplianceChannel(sub) => {
                let params = dq::ConvertParams {
                    keyspace_prefix: sub.keyspace_prefix,
                    channel_name: sub.channel_name,
                    input_dir: sub.input_dir,
                    output_dir: sub.output_dir,
                };
                dq::convert(params).await
            }
        }
    })
}
