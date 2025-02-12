use quinn;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::Endpoint;
use quinn::EndpointConfig;
use quinn::Incoming;
use rustls::pki_types::pem::PemObject;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use taskrun::tokio;

macro_rules! info { ($($arg:expr),*) => ( if true { netpod::log::info!($($arg),*); } ); }

autoerr::create_error_v1!(
    name(Error, "Http3Support"),
    enum variants {
        NoRuntime,
        IO(#[from] std::io::Error),
    },
);

pub struct Http3Support {
    ep: Option<Endpoint>,
}

impl Http3Support {
    pub async fn new(bind_addr: SocketAddr) -> Result<Self, Error> {
        let key = match PemObject::from_pem_file("key.pem") {
            Ok(x) => x,
            Err(e) => {
                info!("key error {}", e);
                return Ok(Self::dummy());
            }
        };
        let cert = match PemObject::from_pem_file("cert.pem") {
            Ok(x) => x,
            Err(e) => {
                info!("cert error {}", e);
                return Ok(Self::dummy());
            }
        };
        let conf = EndpointConfig::default();
        let mut tls_conf = match rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
        {
            Ok(x) => x,
            Err(e) => {
                info!("tls config error {}", e);
                return Ok(Self::dummy());
            }
        };
        tls_conf.alpn_protocols = vec![b"h3".to_vec()];
        tls_conf.max_early_data_size = u32::MAX;
        let tls_conf = tls_conf;
        let v = match QuicServerConfig::try_from(tls_conf) {
            Ok(x) => x,
            Err(e) => {
                info!("config error {}", e);
                return Ok(Self::dummy());
            }
        };
        let quic_conf = Arc::new(v);
        let conf_srv = quinn::ServerConfig::with_crypto(quic_conf);
        let sock = std::net::UdpSocket::bind(bind_addr)?;
        info!("h3 sock {:?}", sock);
        let rt = quinn::default_runtime().ok_or_else(|| Error::NoRuntime)?;
        let ep = Endpoint::new(conf, Some(conf_srv), sock, rt)?;
        {
            let ep = ep.clone();
            tokio::task::spawn(Self::accept(ep));
        }
        let ret = Self { ep: Some(ep) };
        Ok(ret)
    }

    fn dummy() -> Self {
        Self { ep: None }
    }

    pub async fn wait_idle(self) -> () {
        if let Some(ep) = self.ep.as_ref() {
            ep.close(quinn::VarInt::from_u32(1), b"shutdown");
            ep.wait_idle().await
        }
    }

    async fn accept(ep: Endpoint) {
        info!("accepting h3");
        while let Some(inc) = ep.accept().await {
            tokio::spawn(Self::handle_incoming(inc));
        }
    }

    async fn handle_incoming(inc: Incoming) {
        info!("new incoming {:?}", inc.remote_address());
        let conn = match inc.await {
            Ok(x) => x,
            Err(e) => {
                info!("connection error {}", e);
                return;
            }
        };
        let fut1 = {
            let conn = conn.clone();
            async move {
                let bi = conn.accept_bi().await;
                info!("got bi {:?}", bi);
                match bi {
                    Ok(mut v) => {
                        v.0.write(b"some-data").await;
                    }
                    Err(e) => {}
                }
            }
        };
        let fut2 = {
            let conn = conn.clone();
            async move {
                let uni = conn.accept_uni().await;
                info!("got uni {:?}", uni);
            }
        };
        tokio::spawn(fut1);
        tokio::spawn(fut2);
    }
}

// impl Future for Http3Support {
//     type Output = Result<(), Error>;

//     fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
//         todo!()
//     }
// }
