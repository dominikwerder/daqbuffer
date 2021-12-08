// TODO crate `err` pulls in all other dependencies in order to implement From<...> for Error.
// Refactor that...
// Crate `taskrun` also depends on `err`...

use std::path::PathBuf;

use err::Error;

#[derive(Debug)]
pub struct Error2;

use clap::{crate_version, Parser};

#[derive(Debug, Parser)]
#[clap(name="DAQ tools", author="Dominik Werder <dominik.werder@psi.ch>", version=crate_version!())]
pub struct Opts {
    #[clap(short, long, parse(from_occurrences))]
    pub verbose: i32,
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
    //taskrun::run(async { Ok(()) })
    let opts = Opts::parse();
    eprintln!("Opts: {:?}", opts);
    Err(Error::with_msg_no_trace(format!("123")))
}
