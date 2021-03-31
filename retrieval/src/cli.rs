use clap::{Clap, crate_version};

#[derive(Debug, Clap)]
#[clap(name="retrieval", author="Dominik Werder <dominik.werder@gmail.com>", version=crate_version!())]
pub struct Opts {
    #[clap(short, long, parse(from_occurrences))]
    pub verbose: i32,
    #[clap(subcommand)]
    pub subcmd: SubCmd,
}

#[derive(Debug, Clap)]
pub enum SubCmd {
    Version,
    Retrieval(Retrieval),
}

#[derive(Debug, Clap)]
pub struct Retrieval {
}
