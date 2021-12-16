use clap::{crate_version, Parser};
use err::Error;
use netpod::{timeunits::*, Nanos};
use std::{path::PathBuf, str::FromStr};

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
        about = "Prefix for keyspaces, e.g. specify `daq` to get scalar keyspace directory `daq_2`."
    )]
    keyspace_prefix: String,
    #[clap(long, about = "Name of the channel to convert.")]
    channel_name: String,
    #[clap(long, about = "Look for archiver appliance data at given path.")]
    input_dir: PathBuf,
    #[clap(long, about = "Generate Databuffer format at given path.")]
    output_dir: PathBuf,
    #[clap(
        default_value = "1d",
        long,
        about = "Size of the time-bins in the generated Databuffer format.\nUnit-suffixes: `h` (hours), `d` (days)"
    )]
    time_bin_size: TimeBinSize,
}

#[derive(Clone, Debug)]
pub struct TimeBinSize {
    nanos: u64,
}

impl FromStr for TimeBinSize {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(Error::with_msg_no_trace("Malformed time-bin size"));
        }
        let suff = s.chars().last().unwrap();
        if suff.is_numeric() {
            Err(Error::with_msg_no_trace("Malformed time-bin size"))
        } else if suff == 'h' {
            let bs: u64 = s[..s.len() - 1].parse()?;
            Ok(Self { nanos: bs * HOUR })
        } else if suff == 'd' {
            let bs: u64 = s[..s.len() - 1].parse()?;
            Ok(Self { nanos: bs * DAY })
        } else {
            Err(Error::with_msg_no_trace("Malformed time-bin size"))
        }
    }
}

pub fn main() -> Result<(), Error> {
    taskrun::run(async {
        let opts = Opts::parse();
        match opts.subcmd {
            SubCmd::ConvertArchiverApplianceChannel(sub) => {
                let params = dq::ConvertParams {
                    keyspace_prefix: sub.keyspace_prefix,
                    channel_name: sub.channel_name,
                    input_dir: sub.input_dir,
                    output_dir: sub.output_dir,
                    time_bin_size: Nanos::from_ns(sub.time_bin_size.nanos),
                };
                dq::convert(params).await
            }
        }
    })
}
