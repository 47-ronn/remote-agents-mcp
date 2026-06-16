//! WebSocket connection to relay server

use crate::config::Config;
use crate::executor;
use crate::state::AgentState;
use crate::udp_transport::{SignalMessage, UdpTransport};
use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use remote_agents_shared::{AgentInfo, ClientMessage, Command, ServerMessage, UdpFrame};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::{interval, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const PING_INTERVAL: Duration = Duration::from_secs(30);
const RECONNECT_MIN: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(60);
/// If no message (including pongs) is seen for this long, consider the link
/// dead and reconnect.
const HEALTH_TIMEOUT: Duration = Duration::from_secs(90);
/// A connection that stayed up at least this long resets the backoff.
const STABLE_THRESHOLD: Duration = Duration::from_secs(30);

/// Run the agent connection loop with auto-reconnect and exponential backoff.
pub async fn run(config: &Config) -> Result<()> {
    // Built once so a runtime mode change (SetMode) survives reconnects.
    let state = AgentState::new(config.clone());

    // Run scheduled tasks independently of relay connectivity.
    state.start_scheduler();

    let mut backoff = RECONNECT_MIN;

    loop {
        let started = Instant::now();
        let result = connect_and_run(config, &state).await;
        let uptime = started.elapsed();

        // A connection that lasted a while was healthy; reset backoff.
        if uptime >= STABLE_THRESHOLD {
            backoff = RECONNECT_MIN;
        }

        match result {
            Ok(()) => {
                info!("Connection closed gracefully");
                break;
            }
            Err(e) => {
                error!("Connection error: {}", e);
                let delay = backoff + jitter(backoff);
                info!("Reconnecting in {:?}...", delay);
                tokio::time::sleep(delay).await;
                backoff = (backoff * 2).min(RECONNECT_MAX);
            }
        }
    }
    Ok(())
}

/// Small deterministic jitter (0..backoff/2) derived from the current nanos to
/// avoid a thundering herd of agents reconnecting in lockstep.
fn jitter(backoff: Duration) -> Duration {
    let half = backoff.as_millis() as u64 / 2;
    if half == 0 {
        return Duration::ZERO;
    }
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    Duration::from_millis(n % (half + 1))
}

async fn connect_and_run(config: &Config, state: &AgentState) -> Result<()> {
    // Build WebSocket URL
    let ws_url = format!(
        "{}/ws/room/{}?token={}",
        config.relay_url, config.room, config.token
    );

    info!("Connecting to {}", ws_url);

    // Connect
    let (ws_stream, _response) = connect_async(&ws_url)
        .await
        .context("Failed to connect to relay")?;

    info!("Connected to relay");

    let (mut write, mut read) = ws_stream.split();

    // Create UDP signaling channel
    let (udp_signal_tx, mut udp_signal_rx) = mpsc::channel::<SignalMessage>(32);
    // Inbound application data received over UDP channels (peer_session, bytes).
    let (udp_inbound_tx, mut udp_inbound_rx) = mpsc::channel::<(String, Vec<u8>)>(32);

    // Build agent info (reflecting the current runtime mode)
    let agent_info = build_agent_info(config, state.mode().await);

    // Send auth message
    let auth_msg = ClientMessage::Auth {
        room: config.room.clone(),
        token: config.token.clone(),
        agent_info: Some(Box::new(agent_info.clone())),
    };

    write
        .send(Message::Text(auth_msg.to_json()?))
        .await
        .context("Failed to send auth")?;

    // Wait for auth response
    let auth_response = timeout(Duration::from_secs(10), read.next())
        .await
        .context("Auth timeout")?
        .ok_or_else(|| anyhow::anyhow!("Connection closed during auth"))?
        .context("WebSocket error during auth")?;

    let session_id = if let Message::Text(text) = auth_response {
        let msg: ServerMessage = ServerMessage::from_json(&text)?;
        match msg {
            ServerMessage::AuthOk { session_id } => {
                info!("Authenticated with session ID: {}", session_id);
                session_id
            }
            ServerMessage::AuthFailed { reason } => {
                return Err(anyhow::anyhow!("Auth failed: {}", reason));
            }
            _ => {
                return Err(anyhow::anyhow!("Unexpected auth response"));
            }
        }
    } else {
        return Err(anyhow::anyhow!("Invalid auth response"));
    };

    info!("End-to-end payload encryption active (AES-GCM-256)");

    // Create UDP transport for direct peer connections
    let udp_transport = Arc::new(UdpTransport::new(
        state.cipher().clone(),
        session_id.clone(),
        udp_signal_tx,
        udp_inbound_tx,
    ));
    info!("UDP transport initialized");

    // Create channels for communication
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ClientMessage>(32);

    // Spawn ping task
    let ping_tx = tx.clone();
    tokio::spawn(async move {
        let mut ping_interval = interval(PING_INTERVAL);
        loop {
            ping_interval.tick().await;
            if ping_tx.send(ClientMessage::Ping).await.is_err() {
                break;
            }
        }
    });

    // Health watchdog: if no inbound traffic arrives within HEALTH_TIMEOUT,
    // the link is presumed dead and we bail out to trigger a reconnect.
    let mut health = interval(HEALTH_TIMEOUT);
    health.tick().await; // consume the immediate first tick
    let mut alive = true;
    let mut commands_handled: u64 = 0;

    // Main loop
    loop {
        tokio::select! {
            // Incoming messages from relay
            msg = read.next() => {
                alive = true;
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if matches!(ServerMessage::from_json(&text), Ok(ServerMessage::Command { .. })) {
                            commands_handled += 1;
                        }
                        if let Err(e) = handle_server_message(&text, state, &tx, &udp_transport).await {
                            error!("Error handling message: {}", e);
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        debug!("Received ws pong");
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!("Server closed connection");
                        break;
                    }
                    Some(Err(e)) => {
                        return Err(e.into());
                    }
                    None => {
                        info!("Connection closed");
                        break;
                    }
                    _ => {}
                }
            }

            // Outgoing messages to relay
            msg = rx.recv() => {
                if let Some(msg) = msg {
                    write.send(Message::Text(msg.to_json()?)).await?;
                }
            }

            // UDP signaling messages to relay
            signal = udp_signal_rx.recv() => {
                if let Some(signal) = signal {
                    let msg = match signal {
                        SignalMessage::Offer(offer) => ClientMessage::UdpOffer(offer),
                        SignalMessage::Answer(answer) => ClientMessage::UdpAnswer(answer),
                        SignalMessage::Result(result) => ClientMessage::UdpResult(result),
                    };
                    write.send(Message::Text(msg.to_json()?)).await?;
                }
            }

            // Inbound application data over a UDP channel (e.g. a command whose
            // bulk partition data was sent directly over UDP).
            inbound = udp_inbound_rx.recv() => {
                if let Some((peer_session, data)) = inbound {
                    commands_handled += 1;
                    handle_udp_inbound(&peer_session, &data, state, &tx, &udp_transport).await;
                }
            }

            // Outbound agent events (e.g. autonomous task completion)
            event = state.next_event() => {
                if let Some(event) = event {
                    let msg = ClientMessage::Notify { event };
                    write.send(Message::Text(msg.to_json()?)).await?;
                }
            }

            // Health check tick
            _ = health.tick() => {
                if !alive {
                    warn!("No activity for {:?}; treating connection as dead", HEALTH_TIMEOUT);
                    anyhow::bail!("health timeout");
                }
                debug!("Health OK (commands handled this session: {})", commands_handled);
                alive = false;
            }
        }
    }

    info!("Session ended after handling {} command(s)", commands_handled);
    Ok(())
}

async fn handle_server_message(
    text: &str,
    state: &AgentState,
    tx: &tokio::sync::mpsc::Sender<ClientMessage>,
    udp_transport: &Arc<UdpTransport>,
) -> Result<()> {
    let msg: ServerMessage = ServerMessage::from_json(text)?;

    match msg {
        ServerMessage::Command {
            request_id,
            payload,
            from_session,
        } => {
            let cipher = state.cipher();

            // Decrypt the command envelope; failure means a key mismatch or
            // tampering — report it without executing anything.
            let command = match Command::decrypt(&payload, &cipher) {
                Ok(cmd) => cmd,
                Err(e) => {
                    warn!("Failed to decrypt command {}: {}", request_id, e);
                    tx.send(ClientMessage::CommandError {
                        request_id,
                        error: "payload decryption failed (key mismatch?)".to_string(),
                    })
                    .await?;
                    return Ok(());
                }
            };

            debug!("Received command: {:?}", command);

            // Execute and re-encrypt the result.
            let response = match executor::execute(&command, state).await {
                Ok(result) => match result.encrypt(&cipher) {
                    Ok(envelope) => {
                        // Try to send result via UDP if channel is available
                        let _sent_via_udp = if udp_transport.has_udp_channel(&from_session).await {
                            match udp_transport.send_via_udp(&from_session, envelope.as_bytes()).await {
                                Ok(true) => {
                                    debug!("Sent result via UDP to {}", from_session);
                                    true
                                }
                                Ok(false) | Err(_) => false,
                            }
                        } else {
                            false
                        };

                        // Always send via WS as fallback/confirmation
                        ClientMessage::CommandResult {
                            request_id,
                            result: envelope,
                        }
                    }
                    Err(e) => ClientMessage::CommandError {
                        request_id,
                        error: format!("result encryption failed: {}", e),
                    },
                },
                Err(e) => ClientMessage::CommandError {
                    request_id,
                    error: e.to_string(),
                },
            };

            tx.send(response).await?;
        }

        // UDP Signaling messages
        ServerMessage::YourEndpoint { endpoint } => {
            info!("Relay reports our public endpoint: {}", endpoint);
            udp_transport.set_public_endpoint(endpoint).await;
        }

        ServerMessage::UdpOffer { from_session, offer } => {
            debug!("Received UDP offer from {}", from_session);
            if let Err(e) = udp_transport.handle_offer(offer).await {
                warn!("Failed to handle UDP offer: {}", e);
            }
        }

        ServerMessage::UdpAnswer { from_session, answer } => {
            debug!("Received UDP answer from {}", from_session);
            if let Err(e) = udp_transport.handle_answer(answer).await {
                warn!("Failed to handle UDP answer: {}", e);
            }
        }

        ServerMessage::UdpResult { from_session, result } => {
            if result.success {
                info!("UDP channel {} established with {}", result.channel_id, from_session);
            } else {
                warn!(
                    "UDP channel {} failed with {}: {:?}",
                    result.channel_id, from_session, result.error
                );
            }
        }

        ServerMessage::Pong => {
            debug!("Received pong");
        }

        ServerMessage::Error { message } => {
            warn!("Server error: {}", message);
        }

        // Peer awareness: the relay tells each agent who else is in the room so
        // a host knows its neighbours' OS/platform/tags (see AgentState::peers).
        ServerMessage::AgentList { agents } => {
            debug!("Peer list updated: {} peer(s)", agents.len());
            state.set_peers(agents).await;
        }
        ServerMessage::AgentJoined { agent } => {
            debug!("Peer joined: {} ({})", agent.name, agent.id);
            state.upsert_peer(agent).await;
        }
        ServerMessage::AgentLeft { agent_id } => {
            debug!("Peer left: {}", agent_id);
            state.remove_peer(&agent_id).await;
        }

        _ => {
            debug!("Ignoring message: {:?}", msg);
        }
    }

    Ok(())
}

/// Handle application data received over a UDP channel. A `UdpFrame::Command`
/// carries an E2E-encrypted command (whose bulk partition data travelled
/// directly over UDP); we decrypt, execute, and return the result over
/// WebSocket so the MCP server's pending map resolves it by `request_id`.
/// (Returning results over UDP is a later optimization; the inbound direction —
/// big data to the agent — is the win here.)
async fn handle_udp_inbound(
    peer_session: &str,
    data: &[u8],
    state: &AgentState,
    tx: &tokio::sync::mpsc::Sender<ClientMessage>,
    _udp_transport: &Arc<UdpTransport>,
) {
    let frame = match UdpFrame::from_bytes(data) {
        Some(f) => f,
        None => {
            warn!("Dropping malformed UDP frame from {}", peer_session);
            return;
        }
    };

    let UdpFrame::Command { request_id, payload, .. } = frame else {
        debug!("Ignoring non-command UDP frame from {}", peer_session);
        return;
    };

    let cipher = state.cipher();
    let reply = match Command::decrypt(&payload, &cipher) {
        Ok(command) => match executor::execute(&command, state).await {
            Ok(result) => match result.encrypt(&cipher) {
                Ok(envelope) => ClientMessage::CommandResult { request_id, result: envelope },
                Err(e) => ClientMessage::CommandError {
                    request_id,
                    error: format!("result encryption failed: {e}"),
                },
            },
            Err(e) => ClientMessage::CommandError { request_id, error: e.to_string() },
        },
        Err(e) => {
            warn!("Failed to decrypt UDP command {}: {}", request_id, e);
            ClientMessage::CommandError {
                request_id,
                error: "payload decryption failed (key mismatch?)".to_string(),
            }
        }
    };

    let _ = tx.send(reply).await;
}

pub(crate) fn build_agent_info(config: &Config, mode: remote_agents_shared::AgentMode) -> AgentInfo {
    AgentInfo {
        id: config.id.clone(),
        name: config.name.clone(),
        mode,
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        hostname: hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string()),
        tags: config.tags.clone(),
        platform: remote_agents_shared::PlatformInfo::detect(),
        autonomous: config.autonomous.enabled,
        accepts_commands: config.accepts_commands,
        connected_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        // session_id is set by the relay, not the agent
        session_id: None,
        // Surfaced from the launcher's cached npm-registry check.
        update_available: crate::config::update_available(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use remote_agents_shared::{AgentMode, Cipher, CommandResult};

    // --- jitter -------------------------------------------------------------

    #[test]
    fn jitter_is_zero_when_backoff_too_small() {
        // half = backoff_ms / 2 == 0 -> always Duration::ZERO.
        assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
        assert_eq!(jitter(Duration::from_millis(1)), Duration::ZERO);
    }

    #[test]
    fn jitter_stays_within_half_of_backoff() {
        let backoff = Duration::from_secs(10); // half = 5000ms
        for _ in 0..50 {
            let j = jitter(backoff);
            assert!(
                j <= Duration::from_millis(5000),
                "jitter {j:?} exceeded backoff/2"
            );
        }
    }

    // --- build_agent_info ---------------------------------------------------

    #[test]
    fn build_agent_info_propagates_config_and_leaves_session_unset() {
        let mut config = Config {
            id: "agent-7".into(),
            name: "worker".into(),
            tags: vec!["gpu".into(), "linux".into()],
            ..Default::default()
        };
        config.autonomous.enabled = true;

        let info = build_agent_info(&config, AgentMode::Edit);

        assert_eq!(info.id, "agent-7");
        assert_eq!(info.name, "worker");
        assert_eq!(info.tags, vec!["gpu".to_string(), "linux".to_string()]);
        assert!(info.autonomous);
        assert!(matches!(info.mode, AgentMode::Edit));
        // session_id is assigned by the relay, never by the agent.
        assert_eq!(info.session_id, None);
        // os/arch come from the build target and are always populated.
        assert!(!info.os.is_empty());
        assert!(!info.arch.is_empty());
    }

    // --- handle_server_message ---------------------------------------------

    /// A command whose envelope cannot be decrypted with our cipher must yield
    /// a `CommandError` (never execute) carrying the original request id.
    #[tokio::test]
    async fn handle_command_with_bad_payload_reports_decrypt_error() {
        let state = AgentState::new(Config::default());
        let (tx, mut rx) = mpsc::channel::<ClientMessage>(8);
        let (sig_tx, _sig_rx) = mpsc::channel(8);
        let (in_tx, _in_rx) = mpsc::channel(8);
        let udp = Arc::new(UdpTransport::new(
            state.cipher(),
            "sess-self".to_string(),
            sig_tx,
            in_tx,
        ));

        // Encrypt under a DIFFERENT key so the agent's cipher can't open it.
        let wrong = Cipher::from_passphrase("not-the-room-key");
        let payload = Command::GetInfo.encrypt(&wrong).unwrap();
        let msg = ServerMessage::Command {
            request_id: "req-1".to_string(),
            from_session: "mcp-1".to_string(),
            payload,
        };
        let text = serde_json::to_string(&msg).unwrap();

        handle_server_message(&text, &state, &tx, &udp)
            .await
            .unwrap();

        match rx.try_recv().expect("a reply should be queued") {
            ClientMessage::CommandError { request_id, error } => {
                assert_eq!(request_id, "req-1");
                assert!(error.contains("decryption"), "unexpected error: {error}");
            }
            other => panic!("expected CommandError, got {other:?}"),
        }
    }

    /// A correctly-encrypted command is executed and its result re-encrypted
    /// back to the caller. `GetInfo` is side-effect free and works in any mode.
    #[tokio::test]
    async fn handle_command_executes_and_returns_encrypted_result() {
        let state = AgentState::new(Config::default());
        let cipher = state.cipher();
        let (tx, mut rx) = mpsc::channel::<ClientMessage>(8);
        let (sig_tx, _sig_rx) = mpsc::channel(8);
        let (in_tx, _in_rx) = mpsc::channel(8);
        let udp = Arc::new(UdpTransport::new(cipher.clone(), "sess-self".to_string(), sig_tx, in_tx));

        let payload = Command::GetInfo.encrypt(&cipher).unwrap();
        let msg = ServerMessage::Command {
            request_id: "req-2".to_string(),
            from_session: "mcp-1".to_string(),
            payload,
        };
        let text = serde_json::to_string(&msg).unwrap();

        handle_server_message(&text, &state, &tx, &udp)
            .await
            .unwrap();

        match rx.try_recv().expect("a reply should be queued") {
            ClientMessage::CommandResult { request_id, result } => {
                assert_eq!(request_id, "req-2");
                // The result decrypts back to a CommandResult::Info.
                let decrypted = CommandResult::decrypt(&result, &cipher).unwrap();
                assert!(matches!(decrypted, CommandResult::Info { .. }));
            }
            other => panic!("expected CommandResult, got {other:?}"),
        }
    }

    /// Peer-awareness: AgentList replaces the peer set; Joined upserts; Left
    /// removes. The agent learns "who surrounds it" from these relay messages.
    #[tokio::test]
    async fn peer_messages_maintain_agent_state_peers() {
        let state = AgentState::new(Config::default());
        let (tx, _rx) = mpsc::channel::<ClientMessage>(8);
        let (sig_tx, _sig_rx) = mpsc::channel(8);
        let (in_tx, _in_rx) = mpsc::channel(8);
        let udp = Arc::new(UdpTransport::new(state.cipher(), "self".to_string(), sig_tx, in_tx));

        let mut peer_a = remote_agents_shared::AgentInfo {
            id: "a".into(),
            name: "alpha".into(),
            mode: AgentMode::Plan,
            os: "linux".into(),
            arch: "x86_64".into(),
            hostname: "alpha".into(),
            tags: vec![],
            platform: Default::default(),
            autonomous: false,
            accepts_commands: true,
            connected_at: 0,
            session_id: None,
            update_available: None,
        };
        peer_a.platform.distro = Some("Ubuntu 22.04".into());

        let deliver = |state: &AgentState, msg: &ServerMessage| {
            let text = serde_json::to_string(msg).unwrap();
            let (state, udp, tx) = (state.clone(), udp.clone(), tx.clone());
            async move { handle_server_message(&text, &state, &tx, &udp).await.unwrap() }
        };

        // Initial full list with one peer (carrying platform metadata).
        deliver(&state, &ServerMessage::AgentList { agents: vec![peer_a.clone()] }).await;
        let peers = state.peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].platform.distro.as_deref(), Some("Ubuntu 22.04"));

        // A second peer joins.
        let peer_b = remote_agents_shared::AgentInfo { id: "b".into(), name: "beta".into(), ..peer_a.clone() };
        deliver(&state, &ServerMessage::AgentJoined { agent: peer_b }).await;
        assert_eq!(state.peers().await.len(), 2);

        // First peer leaves.
        deliver(&state, &ServerMessage::AgentLeft { agent_id: "a".into() }).await;
        let peers = state.peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "b");
    }

    /// A command delivered over UDP (its bulk data having travelled directly) is
    /// decrypted, executed, and its result returned over WebSocket.
    #[tokio::test]
    async fn udp_inbound_command_executes_and_replies_over_ws() {
        let state = AgentState::new(Config::default());
        let cipher = state.cipher();
        let (tx, mut rx) = mpsc::channel::<ClientMessage>(8);
        let (sig_tx, _sig_rx) = mpsc::channel(8);
        let (in_tx, _in_rx) = mpsc::channel(8);
        let udp = Arc::new(UdpTransport::new(cipher.clone(), "self".into(), sig_tx, in_tx));

        // MCP would send this frame over the UDP channel.
        let payload = Command::GetInfo.encrypt(&cipher).unwrap();
        let frame = UdpFrame::Command {
            request_id: "u1".into(),
            from_session: "mcp-1".into(),
            payload,
        };

        handle_udp_inbound("mcp-1", &frame.to_bytes(), &state, &tx, &udp).await;

        match rx.try_recv().expect("a WS reply should be queued") {
            ClientMessage::CommandResult { request_id, result } => {
                assert_eq!(request_id, "u1");
                let decrypted = CommandResult::decrypt(&result, &cipher).unwrap();
                assert!(matches!(decrypted, CommandResult::Info { .. }));
            }
            other => panic!("expected CommandResult, got {other:?}"),
        }
    }

    /// A `YourEndpoint` message records the relay-observed public endpoint on the
    /// UDP transport (the reflexive address used when offering channels).
    #[tokio::test]
    async fn your_endpoint_sets_public_endpoint() {
        use std::net::{IpAddr, Ipv4Addr};
        let state = AgentState::new(Config::default());
        let (tx, _rx) = mpsc::channel::<ClientMessage>(8);
        let (sig_tx, _sig_rx) = mpsc::channel(8);
        let (in_tx, _in_rx) = mpsc::channel(8);
        let udp = Arc::new(UdpTransport::new(state.cipher(), "self".into(), sig_tx, in_tx));

        assert!(udp.public_endpoint().await.is_none());

        let endpoint = remote_agents_shared::Endpoint::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4500);
        let msg = ServerMessage::YourEndpoint { endpoint };
        let text = serde_json::to_string(&msg).unwrap();
        handle_server_message(&text, &state, &tx, &udp).await.unwrap();

        assert_eq!(udp.public_endpoint().await, Some(endpoint));
    }

    /// Garbage bytes that don't parse as a `UdpFrame` are dropped silently — no
    /// reply is queued (and nothing executes).
    #[tokio::test]
    async fn udp_inbound_malformed_frame_is_dropped() {
        let state = AgentState::new(Config::default());
        let (tx, mut rx) = mpsc::channel::<ClientMessage>(8);
        let (sig_tx, _sig_rx) = mpsc::channel(8);
        let (in_tx, _in_rx) = mpsc::channel(8);
        let udp = Arc::new(UdpTransport::new(state.cipher(), "self".into(), sig_tx, in_tx));

        handle_udp_inbound("m", b"\x00\x01not-a-frame", &state, &tx, &udp).await;

        assert!(rx.try_recv().is_err(), "malformed frame must not produce a reply");
    }

    /// A well-formed but non-command UDP frame (e.g. a `Result`) is ignored:
    /// `handle_udp_inbound` only acts on `Command` frames.
    #[tokio::test]
    async fn udp_inbound_non_command_frame_ignored() {
        let state = AgentState::new(Config::default());
        let (tx, mut rx) = mpsc::channel::<ClientMessage>(8);
        let (sig_tx, _sig_rx) = mpsc::channel(8);
        let (in_tx, _in_rx) = mpsc::channel(8);
        let udp = Arc::new(UdpTransport::new(state.cipher(), "self".into(), sig_tx, in_tx));

        let frame = UdpFrame::Result { request_id: "r".into(), result: "x".into() };
        handle_udp_inbound("m", &frame.to_bytes(), &state, &tx, &udp).await;

        assert!(rx.try_recv().is_err(), "non-command frame must not produce a reply");
    }

    #[tokio::test]
    async fn udp_inbound_bad_payload_replies_error() {
        let state = AgentState::new(Config::default());
        let (tx, mut rx) = mpsc::channel::<ClientMessage>(8);
        let (sig_tx, _sig_rx) = mpsc::channel(8);
        let (in_tx, _in_rx) = mpsc::channel(8);
        let udp = Arc::new(UdpTransport::new(state.cipher(), "self".into(), sig_tx, in_tx));

        // Encrypted under a different key → decrypt fails.
        let wrong = remote_agents_shared::Cipher::from_passphrase("nope");
        let payload = Command::GetInfo.encrypt(&wrong).unwrap();
        let frame = UdpFrame::Command { request_id: "u2".into(), from_session: "m".into(), payload };

        handle_udp_inbound("m", &frame.to_bytes(), &state, &tx, &udp).await;

        match rx.try_recv().expect("an error reply should be queued") {
            ClientMessage::CommandError { request_id, error } => {
                assert_eq!(request_id, "u2");
                assert!(error.contains("decryption"));
            }
            other => panic!("expected CommandError, got {other:?}"),
        }
    }
}
