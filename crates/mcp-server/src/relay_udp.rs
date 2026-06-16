//! UDP transport layer for direct peer-to-peer communication with agents.
//!
//! MCP server initiates UDP connections to agents for faster data transfer.
//! Falls back to WebSocket if UDP hole-punching fails.

use anyhow::{Context, Result};
use remote_agents_shared::{
    reflexive_endpoint, ChannelState, Cipher, Endpoint, UdpAnswer, UdpChannel, UdpChannelResult,
    UdpConfig, UdpOffer,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

/// Manages multiple UDP channels to different agents.
pub struct UdpTransport {
    /// Cipher for E2E encryption
    cipher: Cipher,
    /// Active channels by agent session ID
    channels: RwLock<HashMap<String, Arc<UdpChannel>>>,
    /// Our public endpoint (discovered via relay)
    public_endpoint: RwLock<Option<Endpoint>>,
    /// Our session ID
    session_id: RwLock<Option<String>>,
    /// Configuration
    config: UdpConfig,
    /// Channel for outgoing signaling messages
    signal_tx: mpsc::Sender<SignalMessage>,
}

/// Signaling messages to send via WS relay.
#[derive(Debug)]
pub enum SignalMessage {
    Offer(UdpOffer),
    Answer(UdpAnswer),
    Result(UdpChannelResult),
}

impl UdpTransport {
    /// Create a new UDP transport manager.
    pub fn new(cipher: Cipher, signal_tx: mpsc::Sender<SignalMessage>) -> Self {
        Self {
            cipher,
            channels: RwLock::new(HashMap::new()),
            public_endpoint: RwLock::new(None),
            session_id: RwLock::new(None),
            config: UdpConfig::default(),
            signal_tx,
        }
    }

    /// Set our session ID (after authentication).
    pub async fn set_session_id(&self, session_id: String) {
        *self.session_id.write().await = Some(session_id);
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

    /// Initiate a UDP channel to an agent.
    pub async fn offer_channel(&self, agent_session: String) -> Result<String> {
        let session_id = self
            .session_id
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Session ID not set"))?;

        let channel_id = uuid::Uuid::new_v4().to_string();

        let (channel, _recv_rx) = UdpChannel::new(
            channel_id.clone(),
            self.cipher.clone(),
            self.config.clone(),
        )
        .await
        .context("Failed to create UDP channel")?;

        let local_endpoint = channel.local_endpoint()?;
        let public_endpoint = reflexive_endpoint(*self.public_endpoint.read().await, local_endpoint);

        let offer = UdpOffer {
            channel_id: channel_id.clone(),
            from_session: session_id,
            to_session: agent_session.clone(),
            local_endpoint,
            public_endpoint,
            nonce: channel.local_nonce(),
        };

        // Store channel
        {
            let mut channels = self.channels.write().await;
            channels.insert(agent_session.clone(), Arc::new(channel));
        }

        // Send offer via signaling
        self.signal_tx
            .send(SignalMessage::Offer(offer))
            .await
            .context("Failed to send UDP offer")?;

        info!(
            "Sent UDP offer to {} (channel {})",
            agent_session, channel_id
        );
        Ok(channel_id)
    }

    /// Handle incoming UDP offer from an agent.
    pub async fn handle_offer(&self, offer: UdpOffer) -> Result<()> {
        let session_id = self
            .session_id
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Session ID not set"))?;

        info!(
            "Received UDP offer from {} (channel {})",
            offer.from_session, offer.channel_id
        );

        let (channel, _recv_rx) = UdpChannel::new(
            offer.channel_id.clone(),
            self.cipher.clone(),
            self.config.clone(),
        )
        .await
        .context("Failed to create UDP channel for answer")?;

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
            from_session: session_id,
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

        // Hole-punch, then start recv/retransmit only on success (so they don't
        // race punch_hole for probe packets on the shared socket).
        spawn_punch_then_loops(channel, offer.channel_id.clone(), self.signal_tx.clone());

        Ok(())
    }

    /// Handle incoming UDP answer from an agent.
    pub async fn handle_answer(&self, answer: UdpAnswer) -> Result<()> {
        info!(
            "Received UDP answer from {} (channel {})",
            answer.from_session, answer.channel_id
        );

        if !answer.accepted {
            warn!("Agent rejected UDP channel {}", answer.channel_id);
            return Ok(());
        }

        let channels = self.channels.read().await;
        let channel = channels
            .get(&answer.from_session)
            .ok_or_else(|| anyhow::anyhow!("No channel for agent {}", answer.from_session))?;

        // Set peer endpoint
        let peer_endpoint = answer
            .public_endpoint
            .unwrap_or(answer.local_endpoint)
            .to_socket_addr();
        channel.set_peer(peer_endpoint, answer.nonce).await;

        // Hole-punch, then start recv/retransmit only on success (see
        // handle_offer — avoids racing punch_hole for probe packets).
        spawn_punch_then_loops(channel.clone(), answer.channel_id.clone(), self.signal_tx.clone());

        Ok(())
    }

    /// Check if we have an active UDP channel to an agent.
    pub async fn has_udp_channel(&self, agent_session: &str) -> bool {
        let channels = self.channels.read().await;
        if let Some(channel) = channels.get(agent_session) {
            channel.state().await == ChannelState::Connected
        } else {
            false
        }
    }

    /// Whether a channel entry already exists for this session, in ANY state
    /// (connected or mid-handshake). Used to avoid re-offering — and thereby
    /// clobbering an in-flight handshake — when the agent list is refreshed.
    pub async fn has_channel(&self, agent_session: &str) -> bool {
        self.channels.read().await.contains_key(agent_session)
    }

    /// Send data to an agent via UDP (if available).
    /// Returns true if sent via UDP, false if should use WS fallback.
    pub async fn send_via_udp(&self, agent_session: &str, data: &[u8]) -> Result<bool> {
        let channels = self.channels.read().await;
        if let Some(channel) = channels.get(agent_session) {
            if channel.state().await == ChannelState::Connected {
                channel.send_reliable(data).await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Send unreliable data via UDP.
    pub async fn send_unreliable(&self, agent_session: &str, data: &[u8]) -> Result<bool> {
        let channels = self.channels.read().await;
        if let Some(channel) = channels.get(agent_session) {
            if channel.state().await == ChannelState::Connected {
                channel.send_unreliable(data).await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Close a specific channel.
    pub async fn close_channel(&self, agent_session: &str) -> Result<()> {
        let mut channels = self.channels.write().await;
        if let Some(channel) = channels.remove(agent_session) {
            channel.close().await?;
        }
        Ok(())
    }

    /// Close all UDP channels.
    pub async fn close_all(&self) -> Result<()> {
        let channels = self.channels.read().await;
        for channel in channels.values() {
            let _ = channel.close().await;
        }
        Ok(())
    }
}

/// Hole-punch a channel, then start its recv + retransmit loops only after the
/// punch succeeds (so `recv_loop` doesn't steal probe packets from `punch_hole`
/// on the shared UDP socket), reporting the outcome via signaling.
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

    fn transport() -> (UdpTransport, mpsc::Receiver<SignalMessage>) {
        let cipher = Cipher::from_passphrase("test-key");
        let (tx, rx) = mpsc::channel(16);
        (UdpTransport::new(cipher, tx), rx)
    }

    fn endpoint(port: u16) -> Endpoint {
        Endpoint::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[tokio::test]
    async fn test_transport_creation() {
        let (transport, _rx) = transport();
        assert!(transport.public_endpoint().await.is_none());
    }

    #[tokio::test]
    async fn set_and_get_public_endpoint() {
        let (transport, _rx) = transport();
        assert!(transport.public_endpoint().await.is_none());
        transport.set_public_endpoint(endpoint(4500)).await;
        assert_eq!(transport.public_endpoint().await, Some(endpoint(4500)));
    }

    #[tokio::test]
    async fn offer_channel_requires_session_id() {
        let (transport, _rx) = transport();
        let err = transport
            .offer_channel("agent-x".to_string())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("Session ID not set"), "got: {err}");
    }

    #[tokio::test]
    async fn handle_offer_requires_session_id() {
        let (transport, _rx) = transport();
        let offer = UdpOffer {
            channel_id: "c1".to_string(),
            from_session: "peer".to_string(),
            to_session: "me".to_string(),
            local_endpoint: endpoint(1111),
            public_endpoint: None,
            nonce: [0u8; 16],
        };
        let err = transport.handle_offer(offer).await.unwrap_err().to_string();
        assert!(err.contains("Session ID not set"), "got: {err}");
    }

    #[tokio::test]
    async fn handle_answer_rejected_is_noop() {
        let (transport, _rx) = transport();
        let answer = UdpAnswer {
            channel_id: "c1".to_string(),
            from_session: "peer".to_string(),
            local_endpoint: endpoint(1111),
            public_endpoint: None,
            nonce: [0u8; 16],
            accepted: false,
        };
        // A rejected answer is accepted gracefully and creates no channel.
        transport.handle_answer(answer).await.unwrap();
        assert!(!transport.has_channel("peer").await);
    }

    #[tokio::test]
    async fn handle_answer_unknown_channel_errors() {
        let (transport, _rx) = transport();
        let answer = UdpAnswer {
            channel_id: "c1".to_string(),
            from_session: "ghost".to_string(),
            local_endpoint: endpoint(1111),
            public_endpoint: None,
            nonce: [0u8; 16],
            accepted: true,
        };
        let err = transport.handle_answer(answer).await.unwrap_err().to_string();
        assert!(err.contains("No channel for agent"), "got: {err}");
    }

    #[tokio::test]
    async fn missing_channel_falls_back_to_ws() {
        let (transport, _rx) = transport();
        assert!(!transport.has_udp_channel("nobody").await);
        assert!(!transport.has_channel("nobody").await);
        // No channel → send returns false so the caller uses the WS fallback.
        assert!(!transport.send_via_udp("nobody", b"data").await.unwrap());
        assert!(!transport.send_unreliable("nobody", b"data").await.unwrap());
    }

    #[tokio::test]
    async fn offer_channel_emits_offer_and_registers_channel() {
        let (transport, mut rx) = transport();
        transport.set_session_id("me".to_string()).await;

        let channel_id = transport.offer_channel("agent-x".to_string()).await.unwrap();

        // The offer is queued for the relay with the expected routing fields.
        match rx.recv().await.expect("offer signaled") {
            SignalMessage::Offer(o) => {
                assert_eq!(o.channel_id, channel_id);
                assert_eq!(o.from_session, "me");
                assert_eq!(o.to_session, "agent-x");
            }
            other => panic!("expected Offer, got {other:?}"),
        }
        // A channel entry now exists (mid-handshake) so we don't re-offer.
        assert!(transport.has_channel("agent-x").await);
        // ...but it isn't Connected yet (no hole-punch in a unit test).
        assert!(!transport.has_udp_channel("agent-x").await);
    }
}
