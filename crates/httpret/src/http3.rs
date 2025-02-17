use bytes::Bytes;
use http::StatusCode;
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

const EARLY_DATA_MAX: u32 = u32::MAX;

macro_rules! info { ($($arg:expr),*) => ( if true { netpod::log::info!($($arg),*); } ); }

autoerr::create_error_v1!(
    name(Error, "Http3Support"),
    enum variants {
        NoRuntime,
        IO(#[from] std::io::Error),
        H3(#[from] h3::Error),
        Http(#[from] http::Error),
        Pem(#[from] rustls::pki_types::pem::Error),
        Rustls(#[from] rustls::Error),
        NoInitialCipherSuite(#[from] quinn::crypto::rustls::NoInitialCipherSuite),
        QuinnConnection(#[from] quinn::ConnectionError),
    },
);

pub struct Http3Support {
    ep: Option<Endpoint>,
}

impl Http3Support {
    pub async fn new_or_dummy(bind_addr: SocketAddr) -> Result<Self, Error> {
        Ok(Self::new(bind_addr).await.unwrap_or_else(|e| {
            info!("error {}", e);
            Self::dummy()
        }))
    }

    fn dummy() -> Self {
        Self { ep: None }
    }

    async fn new(bind_addr: SocketAddr) -> Result<Self, Error> {
        let key = PemObject::from_pem_file("key.pem")?;
        let cert = PemObject::from_pem_file("cert.pem")?;
        let mut tls_conf = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)?;
        tls_conf.alpn_protocols = vec![b"HTTP/3".to_vec(), b"h3".to_vec()];
        tls_conf.max_early_data_size = EARLY_DATA_MAX;
        let tls_conf = tls_conf;
        let v = QuicServerConfig::try_from(tls_conf)?;
        let quic_conf = Arc::new(v);
        let conf_srv = quinn::ServerConfig::with_crypto(quic_conf);
        let ep2 = Endpoint::server(conf_srv, bind_addr)?;
        {
            let ep = ep2.clone();
            tokio::task::spawn(Self::accept(ep));
        }
        let ret = Self { ep: Some(ep2) };
        Ok(ret)
    }

    async fn new_plain_quic(bind_addr: SocketAddr) -> Result<Self, Error> {
        let key = PemObject::from_pem_file("key.pem")?;
        let cert = PemObject::from_pem_file("cert.pem")?;
        let conf = EndpointConfig::default();
        let mut tls_conf = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)?;
        tls_conf.alpn_protocols = vec![b"h3".to_vec()];
        tls_conf.max_early_data_size = EARLY_DATA_MAX;
        let tls_conf = tls_conf;
        let v = QuicServerConfig::try_from(tls_conf)?;
        let quic_conf = Arc::new(v);
        let conf_srv = quinn::ServerConfig::with_crypto(quic_conf);
        let sock = std::net::UdpSocket::bind(bind_addr)?;
        info!("h3 sock {:?}", sock);
        let rt = quinn::default_runtime().ok_or_else(|| Error::NoRuntime)?;
        let ep1 = Endpoint::new(conf, Some(conf_srv.clone()), sock, rt)?;
        {
            let ep = ep1.clone();
            tokio::task::spawn(Self::accept(ep));
        }
        let ret = Self { ep: Some(ep1) };
        Ok(ret)
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

    async fn handle_incoming(inc: Incoming) -> Result<(), Error> {
        match Self::handle_incoming_inner(inc).await {
            Ok(x) => Ok(x),
            Err(e) => {
                info!("error handle_connection {}", e);
                Err(e)
            }
        }
    }

    async fn handle_incoming_inner(inc: Incoming) -> Result<(), Error> {
        let addr_remote = inc.remote_address();
        info!("new incoming {:?}", addr_remote);
        let conn1 = inc.accept()?.await?;
        let conn2 = h3_quinn::Connection::new(conn1);
        let mut conn3 = h3::server::builder().build::<_, Bytes>(conn2).await?;
        while let Some((req, mut stream)) = conn3.accept().await? {
            let (head, _body) = req.into_parts();
            info!(
                "see request  {}  {:?}  {:?}  {:?}",
                addr_remote, head.method, head.uri, head.headers
            );
            let res = http::Response::builder()
                .version(http::Version::HTTP_3)
                .status(StatusCode::OK)
                .header("x-daqbuf-tmp", "8e4b217")
                .body(())?;
            stream.send_response(res).await?;
            stream.send_data(Bytes::from_static(b"2025-02-05T16:37:12Z")).await?;
            stream.finish().await?;
            info!("response sent  {}", addr_remote);
        }
        Ok(())
    }
}

// impl Future for Http3Support {
//     type Output = Result<(), Error>;

//     fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
//         todo!()
//     }
// }
