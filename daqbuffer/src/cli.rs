use clap::{crate_version, Clap};

#[derive(Debug, Clap)]
#[clap(name="daqbuffer", author="Dominik Werder <dominik.werder@gmail.com>", version=crate_version!())]
pub struct Opts {
    #[clap(short, long, parse(from_occurrences))]
    pub verbose: i32,
    #[clap(subcommand)]
    pub subcmd: SubCmd,
}

#[derive(Debug, Clap)]
pub enum SubCmd {
    Retrieval(Retrieval),
    Proxy(Proxy),
    Client(Client),
    GenerateTestData,
    Zmtp(Zmtp),
    Test,
}

#[derive(Debug, Clap)]
pub struct Retrieval {
    #[clap(long)]
    pub config: String,
}

#[derive(Debug, Clap)]
pub struct Proxy {
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
    Status(StatusClient),
}

#[derive(Debug, Clap)]
pub struct StatusClient {
    #[clap(long)]
    pub host: String,
    #[clap(long)]
    pub port: u16,
}

#[derive(Debug, Clap)]
pub struct BinnedClient {
    #[clap(long)]
    pub host: String,
    #[clap(long)]
    pub port: u16,
    #[clap(long)]
    pub backend: String,
    #[clap(long)]
    pub channel: String,
    #[clap(long)]
    pub beg: String,
    #[clap(long)]
    pub end: String,
    #[clap(long)]
    pub bins: u32,
    #[clap(long, default_value = "use")]
    pub cache: String,
    #[clap(long, default_value = "1048576")]
    pub disk_stats_every_kb: u32,
}

#[derive(Debug, Clap)]
pub struct Zmtp {
    #[clap(long)]
    pub addr: String,
}
