//! QUIC data transport (quinn) over an already-hole-punched UDP socket.
//!
//! Replaces the hand-rolled per-packet-ACK reliable layer in [`crate::udp_channel`]
//! once a peer endpoint is confirmed. QUIC brings mature congestion control,
//! packet pacing, stream flow control, and GSO/GRO batched syscalls — far past
//! the ~14 MB/s ceiling of the userspace ARQ. This is the same shape iroh uses
//! internally (a UDP socket that hole-punches, then quinn on top).
//!
//! Security: QUIC's TLS uses a self-signed cert with an accept-any verifier —
//! the peer is already authenticated by the hole-punch nonce, and application
//! payloads stay E2E-encrypted with the room [`Cipher`] on top of QUIC.

use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{ClientConfig, Connection, Endpoint, EndpointConfig, RecvStream, SendStream, ServerConfig};

/// ALPN for our QUIC data channel.
const ALPN: &[u8] = b"ra-quic/1";
/// Hard cap on a single framed message read off a stream (defensive; our frames
/// are bounded well below this).
const MAX_FRAME: usize = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum QuicError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls/config: {0}")]
    Config(String),
    #[error("connect: {0}")]
    Connect(#[from] quinn::ConnectError),
    #[error("connection: {0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("write: {0}")]
    Write(#[from] quinn::WriteError),
    #[error("read: {0}")]
    Read(#[from] quinn::ReadExactError),
    #[error("frame too large: {0}")]
    FrameTooLarge(usize),
}

/// Install the ring crypto provider once (idempotent; ignores "already set").
fn ensure_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Accept any server certificate: the peer is authenticated out-of-band (the
/// hole-punch nonce) and the payload is E2E-encrypted, so QUIC's cert chain is
/// not our trust anchor — we only want QUIC's transport.
#[derive(Debug)]
struct AcceptAny;

impl rustls::client::danger::ServerCertVerifier for AcceptAny {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Generous transport tuning for high-BDP WAN links (the whole point): large
/// flow-control windows so a fat pipe isn't throttled by a small default window.
fn transport_config() -> Arc<quinn::TransportConfig> {
    let mut t = quinn::TransportConfig::default();
    // 128 MiB receive windows cover ~1 Gbps × ~1s without stalling on credit.
    t.stream_receive_window((128u32 * 1024 * 1024).into());
    t.receive_window((256u32 * 1024 * 1024).into());
    t.send_window(256 * 1024 * 1024);
    // Keep the NAT mapping warm and detect death.
    t.keep_alive_interval(Some(std::time::Duration::from_secs(10)));
    t.max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into().unwrap()));
    Arc::new(t)
}

fn server_config() -> Result<ServerConfig, QuicError> {
    ensure_provider();
    let cert = rcgen::generate_simple_self_signed(vec!["ra".to_string()])
        .map_err(|e| QuicError::Config(e.to_string()))?;
    let cert_der = cert.cert.der().clone();
    let key_der =
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    let mut crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der.into())
        .map_err(|e| QuicError::Config(e.to_string()))?;
    crypto.alpn_protocols = vec![ALPN.to_vec()];
    let mut cfg = ServerConfig::with_crypto(Arc::new(
        QuicServerConfig::try_from(crypto).map_err(|e| QuicError::Config(e.to_string()))?,
    ));
    cfg.transport_config(transport_config());
    Ok(cfg)
}

fn client_config() -> Result<ClientConfig, QuicError> {
    ensure_provider();
    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAny))
        .with_no_client_auth();
    crypto.alpn_protocols = vec![ALPN.to_vec()];
    let mut cfg = ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(crypto).map_err(|e| QuicError::Config(e.to_string()))?,
    ));
    cfg.transport_config(transport_config());
    Ok(cfg)
}

/// A live QUIC connection over the punched socket (one side per channel).
pub struct QuicConn {
    pub conn: Connection,
    // Keep the endpoint alive for the connection's lifetime.
    _endpoint: Endpoint,
}

impl QuicConn {
    /// QUIC server side: take ownership of the punched socket and accept the
    /// peer's connection.
    pub async fn accept(socket: UdpSocket) -> Result<Self, QuicError> {
        let endpoint = Endpoint::new(
            EndpointConfig::default(),
            Some(server_config()?),
            socket,
            Arc::new(quinn::TokioRuntime),
        )?;
        let incoming = endpoint
            .accept()
            .await
            .ok_or_else(|| QuicError::Config("endpoint closed before accept".into()))?;
        let conn = incoming.await?;
        Ok(Self { conn, _endpoint: endpoint })
    }

    /// QUIC client side: take ownership of the punched socket and connect to the
    /// confirmed peer endpoint.
    pub async fn connect(socket: UdpSocket, peer: SocketAddr) -> Result<Self, QuicError> {
        let mut endpoint = Endpoint::new(
            EndpointConfig::default(),
            None,
            socket,
            Arc::new(quinn::TokioRuntime),
        )?;
        endpoint.set_default_client_config(client_config()?);
        let conn = endpoint
            .connect(peer, "ra")
            .map_err(QuicError::Connect)?
            .await?;
        Ok(Self { conn, _endpoint: endpoint })
    }
}

/// Write a length-delimited message (`u32` BE length + body) to a stream.
pub async fn write_msg(send: &mut SendStream, data: &[u8]) -> Result<(), QuicError> {
    if data.len() > MAX_FRAME {
        return Err(QuicError::FrameTooLarge(data.len()));
    }
    let len = (data.len() as u32).to_be_bytes();
    send.write_all(&len).await?;
    send.write_all(data).await?;
    Ok(())
}

/// Read one length-delimited message. `Ok(None)` on a clean stream end.
pub async fn read_msg(recv: &mut RecvStream) -> Result<Option<Vec<u8>>, QuicError> {
    let mut len_buf = [0u8; 4];
    match recv.read_exact(&mut len_buf).await {
        Ok(()) => {}
        // Clean end of stream before a new frame began.
        Err(quinn::ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(e) => return Err(QuicError::Read(e)),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(QuicError::FrameTooLarge(len));
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    fn loopback_socket() -> (UdpSocket, SocketAddr) {
        let s = UdpSocket::bind("127.0.0.1:0").unwrap();
        let a = s.local_addr().unwrap();
        (s, a)
    }

    // Establish a QUIC connection over two loopback sockets and stream 100 MB
    // through one bidi stream; assert it completes and report throughput.
    #[tokio::test]
    async fn quic_loopback_throughput() {
        let (ssock, saddr) = loopback_socket();
        let (csock, _caddr) = loopback_socket();

        let server = tokio::spawn(async move {
            let q = QuicConn::accept(ssock).await.unwrap();
            let (mut _send, mut recv) = q.conn.accept_bi().await.unwrap();
            let mut total = 0usize;
            // Drain framed messages until the peer finishes the stream.
            while let Some(msg) = read_msg(&mut recv).await.unwrap() {
                total += msg.len();
            }
            total
        });

        // Give the server a beat to bind its endpoint.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = QuicConn::connect(csock, saddr).await.unwrap();
        let (mut send, mut _recv) = client.conn.open_bi().await.unwrap();

        let total: usize = 100 * 1024 * 1024;
        let chunk = vec![0xABu8; 256 * 1024];
        let t0 = Instant::now();
        let mut sent = 0;
        while sent < total {
            write_msg(&mut send, &chunk).await.unwrap();
            sent += chunk.len();
        }
        send.finish().unwrap();
        // Hold the connection open until the server has drained everything.
        let got = server.await.unwrap();
        let el = t0.elapsed().as_secs_f64();
        assert_eq!(got, sent);
        eprintln!(
            "QUIC loopback: {} MiB in {:.2}s = {:.1} MiB/s",
            sent / 1024 / 1024,
            el,
            (sent as f64 / 1048576.0) / el
        );
    }
}
