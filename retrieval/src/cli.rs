use clap::{crate_version, Clap};

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
    Retrieval(Retrieval),
    Client(Client),
    GenerateTestData,
}

#[derive(Debug, Clap)]
pub struct Retrieval {
    #[clap(long)]
    pub config: String,
}

#[derive(Debug, Clap)]
pub struct Client {
    #[clap(subcommand)]
    pub client_type: ClientType,
}

#[derive(Debug, Clap)]
pub enum ClientType {
    Binned(BinnedClient),
}

#[derive(Debug, Clap)]
pub struct BinnedClient {
    #[clap(long)]
    pub host: String,
    #[clap(long)]
    pub port: u16,
    #[clap(long)]
    pub channel: String,
    #[clap(long)]
    pub beg: String,
    #[clap(long)]
    pub end: String,
    #[clap(long)]
    pub bins: u32,
}
