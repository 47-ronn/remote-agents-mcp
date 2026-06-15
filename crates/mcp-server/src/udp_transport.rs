//! UDP transport layer for direct peer-to-peer communication.
//!
//! Uses WebSocket relay for signaling, then establishes direct UDP connections
//! between peers via hole-punching. Falls back to WS if UDP is not possible.

use anyhow::{Context, Result};
use remote_agents_shared::{
    reflexive_endpoint, ChannelState, Cipher, Endpoint, UdpAnswer, UdpChannel, UdpChannelResult,
    UdpConfig, UdpOffer,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

/// Manages multiple UDP channels to different peers.
pub struct UdpTransport {
    /// Cipher for E2E encryption
    cipher: Cipher,
    /// Active channels by peer session ID
    channels: RwLock<HashMap<String, Arc<UdpChannel>>>,
    /// Our public endpoint (discovered via relay)
    public_endpoint: RwLock<Option<Endpoint>>,
    /// Our session ID
    session_id: String,
    /// Configuration
    config: UdpConfig,
    /// Channel for outgoing signaling messages
    signal_tx: mpsc::Sender<SignalMessage>,
    /// Inbound application data received over any UDP channel, tagged with the
    /// peer session it came from. Drained by the connection loop and processed
    /// as a [`remote_agents_shared::UdpFrame`]. Without this, received UDP data
    /// would be dropped (the channel's recv side was previously discarded).
    inbound_tx: mpsc::Sender<(String, Vec<u8>)>,
}

/// Signaling messages to send via WS relay.
#[derive(Debug)]
pub enum SignalMessage {
    Offer(UdpOffer),
    Answer(UdpAnswer),
    Result(UdpChannelResult),
}

/// Transport mode for sending data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMode {
    /// Use UDP if available, fallback to WS
    PreferUdp,
    /// Always use WS
    ForceWs,
    /// Always use UDP (fail if not available)
    ForceUdp,
}

impl UdpTransport {
    /// Create a new UDP transport manager.
    pub fn new(
        cipher: Cipher,
        session_id: String,
        signal_tx: mpsc::Sender<SignalMessage>,
        inbound_tx: mpsc::Sender<(String, Vec<u8>)>,
    ) -> Self {
        Self {
            cipher,
            channels: RwLock::new(HashMap::new()),
            public_endpoint: RwLock::new(None),
            session_id,
            config: UdpConfig::default(),
            signal_tx,
            inbound_tx,
        }
    }

    /// Forward a channel's received application data into the shared inbound
    /// queue, tagged with the peer session, until the channel closes.
    fn spawn_inbound_forwarder(&self, peer_session: String, mut recv_rx: mpsc::Receiver<Vec<u8>>) {
        let inbound_tx = self.inbound_tx.clone();
        tokio::spawn(async move {
            while let Some(data) = recv_rx.recv().await {
                if inbound_tx.send((peer_session.clone(), data)).await.is_err() {
                    break;
                }
            }
        });
    }

    /// Set our public endpoint (discovered from relay's YourEndpoint message).
    pub async fn set_public_endpoint(&self, endpoint: Endpoint) {
        info!("UDP public endpoint: {}", endpoint);
        *self.public_endpoint.write().await = Some(endpoint);
    }

    /// Get our public endpoint if known.
    pub async fn public_endpoint(&self) -> Option<Endpoint> {
        *self.public_endpoint.read().await
    }

    /// Initiate a UDP channel to a peer.
    pub async fn offer_channel(&self, peer_session: String) -> Result<String> {
        let channel_id = uuid::Uuid::new_v4().to_string();

        let (channel, recv_rx) = UdpChannel::new(
            channel_id.clone(),
            self.cipher.clone(),
            self.config.clone(),
        )
        .await
        .context("Failed to create UDP channel")?;
        self.spawn_inbound_forwarder(peer_session.clone(), recv_rx);

        let local_endpoint = channel.local_endpoint()?;
        // Reflected IP (from YourEndpoint) + this channel's bound UDP port.
        let public_endpoint = reflexive_endpoint(*self.public_endpoint.read().await, local_endpoint);

        let offer = UdpOffer {
            channel_id: channel_id.clone(),
            from_session: self.session_id.clone(),
            to_session: peer_session.clone(),
            local_endpoint,
            public_endpoint,
            nonce: channel.local_nonce(),
        };

        // Store channel
        {
            let mut channels = self.channels.write().await;
            channels.insert(peer_session.clone(), Arc::new(channel));
        }

        // Send offer via signaling
        self.signal_tx
            .send(SignalMessage::Offer(offer))
            .await
            .context("Failed to send UDP offer")?;

        info!("Sent UDP offer to {} (channel {})", peer_session, channel_id);
        Ok(channel_id)
    }

    /// Handle incoming UDP offer.
    pub async fn handle_offer(&self, offer: UdpOffer) -> Result<()> {
        info!(
            "Received UDP offer from {} (channel {})",
            offer.from_session, offer.channel_id
        );

        let (channel, recv_rx) = UdpChannel::new(
            offer.channel_id.clone(),
            self.cipher.clone(),
            self.config.clone(),
        )
        .await
        .context("Failed to create UDP channel for answer")?;
        self.spawn_inbound_forwarder(offer.from_session.clone(), recv_rx);

        let local_endpoint = channel.local_endpoint()?;
        let public_endpoint = reflexive_endpoint(*self.public_endpoint.read().await, local_endpoint);

        // Set peer endpoint - prefer public if available
        let peer_endpoint = offer
            .public_endpoint
            .unwrap_or(offer.local_endpoint)
            .to_socket_addr();
        channel.set_peer(peer_endpoint, offer.nonce).await;

        let answer = UdpAnswer {
            channel_id: offer.channel_id.clone(),
            from_session: self.session_id.clone(),
            local_endpoint,
            public_endpoint,
            nonce: channel.local_nonce(),
            accepted: true,
        };

        let channel = Arc::new(channel);

        // Store channel
        {
            let mut channels = self.channels.write().await;
            channels.insert(offer.from_session.clone(), channel.clone());
        }

        // Send answer
        self.signal_tx
            .send(SignalMessage::Answer(answer))
            .await
            .context("Failed to send UDP answer")?;

        // Hole-punch, then start the recv/retransmit loops — but only AFTER the
        // punch succeeds, so they don't race punch_hole for probe packets on the
        // shared socket (recv_loop would otherwise swallow ProbeAcks).
        spawn_punch_then_loops(channel, offer.channel_id.clone(), self.signal_tx.clone());

        Ok(())
    }

    /// Handle incoming UDP answer.
    pub async fn handle_answer(&self, answer: UdpAnswer) -> Result<()> {
        info!(
            "Received UDP answer from {} (channel {})",
            answer.from_session, answer.channel_id
        );

        if !answer.accepted {
            warn!("Peer rejected UDP channel {}", answer.channel_id);
            return Ok(());
        }

        let channels = self.channels.read().await;
        let channel = channels
            .get(&answer.from_session)
            .ok_or_else(|| anyhow::anyhow!("No channel for peer {}", answer.from_session))?;

        // Set peer endpoint
        let peer_endpoint = answer
            .public_endpoint
            .unwrap_or(answer.local_endpoint)
            .to_socket_addr();
        channel.set_peer(peer_endpoint, answer.nonce).await;

        // Hole-punch, then start recv/retransmit loops only on success (see
        // handle_offer — avoids racing punch_hole for probe packets).
        spawn_punch_then_loops(channel.clone(), answer.channel_id.clone(), self.signal_tx.clone());

        Ok(())
    }

    /// Check if we have an active UDP channel to a peer.
    pub async fn has_udp_channel(&self, peer_session: &str) -> bool {
        let channels = self.channels.read().await;
        if let Some(channel) = channels.get(peer_session) {
            channel.state().await == ChannelState::Connected
        } else {
            false
        }
    }

    /// Send data to a peer via UDP (if available).
    /// Returns true if sent via UDP, false if should use WS fallback.
    pub async fn send_via_udp(&self, peer_session: &str, data: &[u8]) -> Result<bool> {
        let channels = self.channels.read().await;
        if let Some(channel) = channels.get(peer_session) {
            if channel.state().await == ChannelState::Connected {
                channel.send_reliable(data).await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Send unreliable data via UDP.
    pub async fn send_unreliable(&self, peer_session: &str, data: &[u8]) -> Result<bool> {
        let channels = self.channels.read().await;
        if let Some(channel) = channels.get(peer_session) {
            if channel.state().await == ChannelState::Connected {
                channel.send_unreliable(data).await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Close all UDP channels.
    pub async fn close_all(&self) -> Result<()> {
        let channels = self.channels.read().await;
        for channel in channels.values() {
            let _ = channel.close().await;
        }
        Ok(())
    }

    /// Close a specific channel.
    pub async fn close_channel(&self, peer_session: &str) -> Result<()> {
        let mut channels = self.channels.write().await;
        if let Some(channel) = channels.remove(peer_session) {
            channel.close().await?;
        }
        Ok(())
    }
}

/// Hole-punch a channel in the background; start its recv + retransmit loops
/// only after the punch succeeds, then report the result via signaling. Gating
/// the loops on punch success prevents `recv_loop` from stealing probe packets
/// from `punch_hole` on the shared UDP socket.
fn spawn_punch_then_loops(
    channel: Arc<UdpChannel>,
    channel_id: String,
    signal_tx: mpsc::Sender<SignalMessage>,
) {
    tokio::spawn(async move {
        match channel.punch_hole().await {
            Ok(()) => {
                info!("UDP channel {} established", channel_id);
                let recv_ch = channel.clone();
                tokio::spawn(async move {
                    if let Err(e) = recv_ch.recv_loop().await {
                        debug!("UDP recv loop ended: {}", e);
                    }
                });
                let retrans_ch = channel.clone();
                tokio::spawn(async move {
                    if let Err(e) = retrans_ch.retransmit_loop().await {
                        debug!("UDP retransmit loop ended: {}", e);
                    }
                });
                let _ = signal_tx
                    .send(SignalMessage::Result(UdpChannelResult {
                        channel_id,
                        success: true,
                        error: None,
                    }))
                    .await;
            }
            Err(e) => {
                warn!("UDP hole-punch failed for {}: {}", channel_id, e);
                let _ = signal_tx
                    .send(SignalMessage::Result(UdpChannelResult {
                        channel_id,
                        success: false,
                        error: Some(e.to_string()),
                    }))
                    .await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    #[tokio::test]
    async fn test_transport_creation() {
        let cipher = Cipher::from_passphrase("test-key");
        let (tx, _rx) = mpsc::channel(16);
        let (inbound_tx, _inbound_rx) = mpsc::channel(16);
        let transport = UdpTransport::new(cipher, "test-session".to_string(), tx, inbound_tx);
        assert!(transport.public_endpoint().await.is_none());
    }

    /// Full transport flow over real loopback sockets: offer → answer →
    /// hole-punch → send → inbound forwarder. Exercises `reflexive_endpoint`
    /// (iter42) and the inbound recv wiring (iter39) end-to-end. Loopback is
    /// made reachable by simulating YourEndpoint reflecting 127.0.0.1.
    #[tokio::test]
    async fn two_transports_exchange_data_over_udp() {
        let cipher = Cipher::from_passphrase("udp-integ-key");
        let (a_sig, mut a_sig_rx) = mpsc::channel(16);
        let (a_in, _a_in_rx) = mpsc::channel(16);
        let a = UdpTransport::new(cipher.clone(), "A".into(), a_sig, a_in);
        let (b_sig, mut b_sig_rx) = mpsc::channel(16);
        let (b_in, mut b_in_rx) = mpsc::channel(16);
        let b = UdpTransport::new(cipher, "B".into(), b_sig, b_in);

        // Simulate YourEndpoint reflecting loopback so offers carry a reachable
        // address (reflexive_endpoint pairs this IP with each channel's UDP port).
        let lo = Endpoint::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        a.set_public_endpoint(lo).await;
        b.set_public_endpoint(lo).await;

        // Signaling handshake (normally shuttled through the relay).
        a.offer_channel("B".into()).await.unwrap();
        let offer = match a_sig_rx.recv().await.unwrap() {
            SignalMessage::Offer(o) => o,
            other => panic!("expected Offer, got {other:?}"),
        };
        b.handle_offer(offer).await.unwrap();
        let answer = loop {
            match b_sig_rx.recv().await.unwrap() {
                SignalMessage::Answer(a) => break a,
                _ => continue, // skip any Result frames
            }
        };
        a.handle_answer(answer).await.unwrap();

        // Wait for hole-punch to connect both ends.
        let mut connected = false;
        for _ in 0..60 {
            if a.has_udp_channel("B").await && b.has_udp_channel("A").await {
                connected = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(connected, "UDP channels did not connect over loopback");

        // A sends application data → B's inbound queue receives it (tagged "A").
        assert!(a.send_via_udp("B", b"hello-udp").await.unwrap());
        let (peer, data) = tokio::time::timeout(Duration::from_secs(3), b_in_rx.recv())
            .await
            .expect("inbound timed out")
            .expect("inbound channel closed");
        assert_eq!(peer, "A");
        assert_eq!(data, b"hello-udp");
    }
}
