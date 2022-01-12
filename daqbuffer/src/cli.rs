use clap::Parser;

#[derive(Debug, Parser)]
#[clap(name = "daqbuffer", author, version)]
pub struct Opts {
    #[clap(short, long, parse(from_occurrences))]
    pub verbose: i32,
    #[clap(subcommand)]
    pub subcmd: SubCmd,
}

#[derive(Debug, Parser)]
pub enum SubCmd {
    Retrieval(Retrieval),
    Proxy(Proxy),
    Client(Client),
    GenerateTestData,
    Zmtp(Zmtp),
    Logappend(Logappend),
    Test,
}

#[derive(Debug, Parser)]
pub struct Retrieval {
    #[clap(long)]
    pub config: String,
}

#[derive(Debug, Parser)]
pub struct Proxy {
    #[clap(long)]
    pub config: String,
}

#[derive(Debug, Parser)]
pub struct Client {
    #[clap(subcommand)]
    pub client_type: ClientType,
}

#[derive(Debug, Parser)]
pub enum ClientType {
    Binned(BinnedClient),
    Status(StatusClient),
}

#[derive(Debug, Parser)]
pub struct StatusClient {
    #[clap(long)]
    pub host: String,
    #[clap(long)]
    pub port: u16,
}

#[derive(Debug, Parser)]
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

#[derive(Debug, Parser)]
pub struct Zmtp {
    #[clap(long)]
    pub addr: String,
}

#[derive(Debug, Parser)]
pub struct Logappend {
    #[clap(long)]
    pub dir: String,
}
