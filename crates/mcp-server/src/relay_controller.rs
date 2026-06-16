//! Connection pool management for relay servers

use crate::relay_udp::{SignalMessage, UdpTransport};
use anyhow::{bail, Context, Result};
use futures::{SinkExt, StreamExt};
use remote_agents_shared::{
    AgentEvent, AgentInfo, AutonomousTask, Cipher, ClientMessage, Command,
    CommandResult, ServerMessage, Target, TaskStatus, UdpFrame,
};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Once};
use std::time::Duration;

/// Install the rustls crypto provider once, so outbound `wss://` connections work.
static CRYPTO_INIT: Once = Once::new();
fn ensure_crypto_provider() {
    CRYPTO_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
use futures::stream::{SplitSink, SplitStream};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Notify, RwLock};
use tokio::time::{sleep_until, timeout, timeout_at, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, warn};

/// Overall backstop for collecting replies to a command.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);
/// For broadcasts: once at least one reply has arrived, stop waiting after this
/// much idle time with no new reply (bounds latency if the agent count is stale).
const FLEET_IDLE_GAP: Duration = Duration::from_secs(3);
/// Controller reconnect backoff bounds (mirrors the agent-side connection loop).
const RECONNECT_MIN: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(60);

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWrite = SplitSink<WsStream, Message>;
type WsRead = SplitStream<WsStream>;

/// Dial the relay and authenticate, returning the split halves + session id.
/// Returns `None` on any failure (the supervisor loop retries with backoff).
async fn redial(
    ws_url: &str,
    room: &str,
    token: &str,
    agent_info: Option<Box<AgentInfo>>,
) -> Option<(WsWrite, WsRead, String)> {
    let (ws, _) = connect_async(ws_url).await.ok()?;
    let (mut write, mut read) = ws.split();
    let auth = ClientMessage::Auth {
        room: room.to_string(),
        token: token.to_string(),
        agent_info,
    };
    write.send(Message::Text(auth.to_json().ok()?)).await.ok()?;
    let resp = timeout(Duration::from_secs(10), read.next()).await.ok()??.ok()?;
    if let Message::Text(t) = resp {
        if let Ok(ServerMessage::AuthOk { session_id }) = ServerMessage::from_json(&t) {
            return Some((write, read, session_id));
        }
    }
    None
}

/// One reply from one agent for a request: Ok(result) or Err(error string).
type AgentReply = (String, Result<CommandResult, String>);

/// Map of in-flight request IDs to the collector receiving every agent's reply.
type PendingMap = Arc<RwLock<HashMap<String, mpsc::UnboundedSender<AgentReply>>>>;

/// Latest known status of autonomous tasks, populated by push events.
type EventMap = Arc<RwLock<HashMap<String, TaskStatus>>>;

/// A reminder cron to cancel automatically when its task completes.
#[derive(Clone)]
struct Watch {
    reminder_name: String,
    self_agent_id: String,
}

/// Map of watched task ids to their reminder cron (for auto-cancel on push).
type WatchMap = Arc<RwLock<HashMap<String, Watch>>>;

/// Per-agent outcome for a (possibly fleet-wide) command.
#[derive(Serialize)]
pub struct AgentOutcome {
    pub agent_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<CommandResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A connection to a single room
struct RoomConnection {
    /// Sender for outgoing messages
    tx: mpsc::Sender<ClientMessage>,
    /// Cached agent list
    agents: Arc<RwLock<Vec<AgentInfo>>>,
    /// Pending command responses
    pending: PendingMap,
    /// End-to-end transport cipher for this room.
    cipher: Cipher,
    /// Latest autonomous task statuses from push events.
    events: EventMap,
    /// Wakes `task_wait` callers when a new event arrives.
    events_notify: Arc<Notify>,
    /// Tasks with a reminder cron to auto-cancel on completion.
    watched: WatchMap,
    /// UDP transport for direct peer-to-peer communication.
    // Held to keep the transport alive; the MCP-side UDP data path is still
    // WIP (plan.md Phase 14: "UDP transport в MCP-сервере"), so it is not yet
    // read. Remove the allow once that path lands.
    #[allow(dead_code)]
    udp_transport: Arc<UdpTransport>,
}

/// Pool of connections to relay servers
pub struct ConnectionPool {
    rooms: HashMap<String, RoomConnection>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            rooms: HashMap::new(),
        }
    }

    /// Connect to a room on the relay server. `key` optionally overrides the
    /// token-derived end-to-end encryption key (must match the agents' key).
    pub async fn connect(
        &mut self,
        relay_url: &str,
        room: &str,
        token: &str,
        key: Option<&str>,
        // The node's own peer identity, advertised so this node is visible in
        // the room's `list_agents` (peer model).
        agent_info: Option<Box<AgentInfo>>,
        // Local executor state. When present, incoming commands from other peers
        // are executed here too — making this a full peer (executes + controls)
        // on a single connection, not a send-only controller.
        executor_state: Option<Arc<crate::state::AgentState>>,
    ) -> Result<String> {
        if self.rooms.contains_key(room) {
            return Ok(format!("Already connected to room '{}'", room));
        }

        ensure_crypto_provider();
        let cipher = Cipher::for_transport(token, key);

        let ws_url = format!("{}/ws/room/{}?token={}", relay_url, room, token);
        info!("Connecting to {}", ws_url);

        let (ws_stream, _) = connect_async(&ws_url)
            .await
            .context("Failed to connect to relay")?;

        let (mut write, mut read) = ws_stream.split();

        // Create channels
        let (tx, mut rx) = mpsc::channel::<ClientMessage>(32);
        let agents = Arc::new(RwLock::new(Vec::new()));
        let pending: PendingMap = Arc::new(RwLock::new(HashMap::new()));
        let events: EventMap = Arc::new(RwLock::new(HashMap::new()));
        let events_notify = Arc::new(Notify::new());
        let watched: WatchMap = Arc::new(RwLock::new(HashMap::new()));

        // Create UDP signaling channel
        let (udp_signal_tx, mut udp_signal_rx) = mpsc::channel::<SignalMessage>(32);
        let udp_transport = Arc::new(UdpTransport::new(cipher.clone(), udp_signal_tx));

        // Keep a copy of our identity to re-send on every reconnect.
        let agent_info_redial = agent_info.clone();

        // Send auth — peer model: we join as a peer carrying our agent_info.
        let auth_msg = ClientMessage::Auth {
            room: room.to_string(),
            token: token.to_string(),
            agent_info,
        };

        write
            .send(Message::Text(auth_msg.to_json()?))
            .await
            .context("Failed to send auth")?;

        // Wait for auth response
        let auth_response = timeout(Duration::from_secs(10), read.next())
            .await
            .context("Auth timeout")?
            .ok_or_else(|| anyhow::anyhow!("Connection closed"))?
            .context("WebSocket error")?;

        let session_id = if let Message::Text(text) = auth_response {
            let msg: ServerMessage = ServerMessage::from_json(&text)?;
            match msg {
                ServerMessage::AuthOk { session_id } => session_id,
                ServerMessage::AuthFailed { reason } => {
                    bail!("Auth failed: {}", reason);
                }
                _ => bail!("Unexpected auth response"),
            }
        } else {
            bail!("Unexpected message type");
        };

        info!("Connected to room '{}' with session '{}'", room, session_id);

        // Set session ID on UDP transport
        udp_transport.set_session_id(session_id.clone()).await;
        info!("UDP transport initialized");

        // Spawn message handler
        let agents_clone = agents.clone();
        let pending_clone = pending.clone();
        let cipher_clone = cipher.clone();
        let events_clone = events.clone();
        let notify_clone = events_notify.clone();
        let watched_clone = watched.clone();
        let tx_clone = tx.clone();
        let udp_transport_clone = udp_transport.clone();

        let executor_clone = executor_state.clone();
        let ws_url_task = ws_url.clone();
        let room_task = room.to_string();
        let token_task = token.to_string();
        let udp_for_session = udp_transport.clone();
        tokio::spawn(async move {
            let shared = HandlerShared {
                agents: agents_clone,
                pending: pending_clone,
                cipher: cipher_clone,
                events: events_clone,
                events_notify: notify_clone,
                watched: watched_clone,
                tx: tx_clone,
                udp_transport: udp_transport_clone,
                executor_state: executor_clone,
            };

            // The first connection is handed in; thereafter the supervisor loop
            // re-dials with exponential backoff so the controller survives relay
            // restarts (like the agent-side connection::run). The mpsc channels
            // and shared maps persist across reconnects, so send_command keeps
            // working (messages queue while offline, flush on reconnect).
            let mut current = Some((write, read));
            let mut backoff = RECONNECT_MIN;
            loop {
                let (mut write, mut read) = match current.take() {
                    Some(c) => c,
                    None => {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(RECONNECT_MAX);
                        match redial(
                            &ws_url_task,
                            &room_task,
                            &token_task,
                            agent_info_redial.clone(),
                        )
                        .await
                        {
                            Some((w, r, session)) => {
                                info!("Controller reconnected to room '{}'", room_task);
                                udp_for_session.set_session_id(session).await;
                                let _ = shared.tx.send(ClientMessage::ListAgents).await;
                                backoff = RECONNECT_MIN;
                                (w, r)
                            }
                            None => {
                                warn!("Controller reconnect to '{}' failed; retrying", room_task);
                                continue;
                            }
                        }
                    }
                };

                // Run until the link drops, then fall through to reconnect.
                loop {
                    tokio::select! {
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    if let Err(e) = handle_message(&text, &shared).await {
                                        error!("Error handling message: {}", e);
                                    }
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    let _ = write.send(Message::Pong(data)).await;
                                }
                                Some(Ok(Message::Close(_))) | None => {
                                    info!("Controller connection closed");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!("WebSocket error: {}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                        msg = rx.recv() => {
                            if let Some(msg) = msg {
                                if let Ok(json) = msg.to_json() {
                                    if let Err(e) = write.send(Message::Text(json)).await {
                                        error!("Failed to send message: {}", e);
                                        break;
                                    }
                                }
                            }
                        }
                        signal = udp_signal_rx.recv() => {
                            if let Some(signal) = signal {
                                let msg = match signal {
                                    SignalMessage::Offer(offer) => ClientMessage::UdpOffer(offer),
                                    SignalMessage::Answer(answer) => ClientMessage::UdpAnswer(answer),
                                    SignalMessage::Result(result) => ClientMessage::UdpResult(result),
                                };
                                if let Ok(json) = msg.to_json() {
                                    if let Err(e) = write.send(Message::Text(json)).await {
                                        error!("Failed to send UDP signal: {}", e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        // Request initial agent list
        tx.send(ClientMessage::ListAgents).await?;

        self.rooms.insert(
            room.to_string(),
            RoomConnection {
                tx,
                agents,
                pending,
                cipher,
                events,
                events_notify,
                watched,
                udp_transport,
            },
        );

        Ok(format!("Connected to room '{}' as session '{}'", room, session_id))
    }

    /// Disconnect from a room
    pub async fn disconnect(&mut self, room: &str) -> Result<()> {
        if let Some(conn) = self.rooms.remove(room) {
            conn.tx.send(ClientMessage::Close).await?;
        }
        Ok(())
    }

    /// List agents in a room
    pub async fn list_agents(&self, room: &str) -> Result<Vec<AgentInfo>> {
        let conn = self.rooms.get(room).ok_or_else(|| {
            anyhow::anyhow!("Not connected to room '{}'", room)
        })?;

        // Request fresh list
        conn.tx.send(ClientMessage::ListAgents).await?;

        // Wait a bit for response
        tokio::time::sleep(Duration::from_millis(100)).await;

        let agents = conn.agents.read().await;
        Ok(agents.clone())
    }

    /// Send a command to agent(s) and return only the successful results.
    /// Now correctly aggregates across a broadcast (every matching agent), not
    /// just the first responder. Used by the single-result helpers.
    pub async fn send_command(
        &self,
        room: &str,
        target: Target,
        payload: Command,
    ) -> Result<Vec<(String, CommandResult)>> {
        let (successes, errors) = self.dispatch(room, target, payload).await?;
        if successes.is_empty() {
            if let Some((_, e)) = errors.into_iter().next() {
                bail!("{}", e);
            }
            bail!("Command timeout");
        }
        Ok(successes)
    }

    /// Send a command to agent(s) and return a per-agent outcome (success OR
    /// error for each), so one failing host doesn't sink the whole batch.
    pub async fn send_command_fleet(
        &self,
        room: &str,
        target: Target,
        payload: Command,
    ) -> Result<Vec<AgentOutcome>> {
        let (successes, errors) = self.dispatch(room, target, payload).await?;
        let mut out: Vec<AgentOutcome> = successes
            .into_iter()
            .map(|(agent_id, result)| AgentOutcome {
                agent_id,
                result: Some(result),
                error: None,
            })
            .collect();
        out.extend(errors.into_iter().map(|(agent_id, error)| AgentOutcome {
            agent_id,
            result: None,
            error: Some(error),
        }));
        Ok(out)
    }

    /// Encrypt, broadcast, and collect replies from every targeted agent.
    async fn dispatch(
        &self,
        room: &str,
        target: Target,
        payload: Command,
    ) -> Result<(Vec<(String, CommandResult)>, Vec<(String, String)>)> {
        let conn = self
            .rooms
            .get(room)
            .ok_or_else(|| anyhow::anyhow!("Not connected to room '{}'", room))?;

        let request_id = uuid::Uuid::new_v4().to_string();

        // Encrypt the command into an opaque envelope before it touches the relay.
        let envelope = payload
            .encrypt(&conn.cipher)
            .map_err(|e| anyhow::anyhow!("failed to encrypt command: {}", e))?;

        // How many replies do we expect? Filter the cached agent list with the
        // same rules the relay's resolveTarget uses. This is a hint for early
        // return only — correctness is guaranteed by the deadline backstop.
        let expected = {
            let agents = conn.agents.read().await;
            match &target {
                Target::Agent { id } => agents.iter().filter(|a| &a.id == id).count(),
                Target::All => agents.len(),
                Target::Tagged { tags } => agents
                    .iter()
                    .filter(|a| a.tags.iter().any(|t| tags.contains(t)))
                    .count(),
                Target::Platform { family } => agents
                    .iter()
                    .filter(|a| {
                        family.eq_ignore_ascii_case(&a.platform.family)
                            || family.eq_ignore_ascii_case(&a.os)
                    })
                    .count(),
            }
        };
        let single = matches!(target, Target::Agent { .. });

        // For a single-agent command, prefer the direct UDP channel (bulk data
        // like a MapTask partition then bypasses the relay). The agent replies
        // over WS, correlated by request_id, so the collector below is unchanged.
        let udp_session = {
            let agents = conn.agents.read().await;
            udp_session_for(&agents, &target)
        };

        // Register the collector BEFORE sending so no reply can race ahead of it.
        let (reply_tx, reply_rx) = mpsc::unbounded_channel::<AgentReply>();
        {
            let mut pending = conn.pending.write().await;
            pending.insert(request_id.clone(), reply_tx);
        }

        let mut sent_via_udp = false;
        if let Some(session) = &udp_session {
            if conn.udp_transport.has_udp_channel(session).await {
                let frame = UdpFrame::Command {
                    request_id: request_id.clone(),
                    // Agent replies over WS (correlated by request_id), so the
                    // origin session is not needed for the reply path.
                    from_session: String::new(),
                    payload: envelope.clone(),
                };
                if let Ok(true) = conn.udp_transport.send_via_udp(session, &frame.to_bytes()).await {
                    sent_via_udp = true;
                    debug!("Sent command {} via UDP to {}", request_id, session);
                }
            }
        }

        // WS path (fallback when no UDP channel, or the broadcast/tagged cases).
        if !sent_via_udp {
            if let Err(e) = conn
                .tx
                .send(ClientMessage::Command {
                    request_id: request_id.clone(),
                    target,
                    payload: envelope,
                })
                .await
            {
                conn.pending.write().await.remove(&request_id);
                return Err(e.into());
            }
        }

        let collected = collect_replies(reply_rx, expected, single).await;

        // Single cleanup covering every exit path; late replies are then dropped.
        conn.pending.write().await.remove(&request_id);

        Ok(collected)
    }

    /// Register a reminder cron to auto-cancel when `task_id` completes.
    pub async fn register_watch(
        &self,
        room: &str,
        task_id: &str,
        reminder_name: &str,
        self_agent_id: &str,
    ) -> Result<()> {
        let conn = self
            .rooms
            .get(room)
            .ok_or_else(|| anyhow::anyhow!("Not connected to room '{}'", room))?;
        conn.watched.write().await.insert(
            task_id.to_string(),
            Watch {
                reminder_name: reminder_name.to_string(),
                self_agent_id: self_agent_id.to_string(),
            },
        );
        Ok(())
    }

    /// Block until an autonomous task completes (via push event) or `timeout_ms`
    /// elapses, then fetch and return its full state over the encrypted path.
    pub async fn task_wait(
        &self,
        room: &str,
        agent_id: &str,
        task_id: &str,
        timeout_ms: u64,
    ) -> Result<AutonomousTask> {
        let (events, notify) = {
            let conn = self
                .rooms
                .get(room)
                .ok_or_else(|| anyhow::anyhow!("Not connected to room '{}'", room))?;
            (conn.events.clone(), conn.events_notify.clone())
        };

        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            // Arm the waiter BEFORE checking, so an event between the check and
            // the await is not missed.
            let notified = notify.notified();
            if let Some(status) = events.read().await.get(task_id).copied() {
                if matches!(status, TaskStatus::Done | TaskStatus::Failed) {
                    break;
                }
            }
            tokio::select! {
                _ = notified => {}
                _ = sleep_until(deadline) => break,
            }
        }

        // Fetch the full task (status + result) over the encrypted command path.
        let results = self
            .send_command(
                room,
                Target::Agent {
                    id: agent_id.to_string(),
                },
                Command::TaskGet {
                    id: task_id.to_string(),
                },
            )
            .await?;
        match results.into_iter().next() {
            Some((_, CommandResult::Task { task })) => Ok(task),
            _ => Err(anyhow::anyhow!("unexpected task_wait result")),
        }
    }
}

/// The peer session to try a direct UDP send for, if any. Only single-agent
/// targets are eligible (broadcast/tagged/platform commands go over the WS
/// fan-out); the agent must have a relay-assigned `session_id`.
fn udp_session_for(agents: &[AgentInfo], target: &Target) -> Option<String> {
    match target {
        Target::Agent { id } => agents
            .iter()
            .find(|a| &a.id == id)
            .and_then(|a| a.session_id.clone()),
        _ => None,
    }
}

/// Drain agent replies for one request, returning (successes, errors).
///
/// Returns as soon as all `expected` replies arrive (or the first, for a
/// single-agent target). After at least one reply, a broadcast also stops on an
/// idle gap; an overall deadline backstops everything.
async fn collect_replies(
    mut rx: mpsc::UnboundedReceiver<AgentReply>,
    expected: usize,
    single: bool,
) -> (Vec<(String, CommandResult)>, Vec<(String, String)>) {
    let mut successes: Vec<(String, CommandResult)> = Vec::new();
    let mut errors: Vec<(String, String)> = Vec::new();
    let deadline = Instant::now() + COMMAND_TIMEOUT;

    loop {
        let total = successes.len() + errors.len();
        if single && total >= 1 {
            break;
        }
        if !single && expected > 0 && total >= expected {
            break;
        }

        // Once we have any reply on a broadcast, cap further waiting on an idle gap.
        let next_deadline = if total > 0 && !single {
            (Instant::now() + FLEET_IDLE_GAP).min(deadline)
        } else {
            deadline
        };

        match timeout_at(next_deadline, rx.recv()).await {
            Ok(Some((agent_id, Ok(result)))) => successes.push((agent_id, result)),
            Ok(Some((agent_id, Err(e)))) => errors.push((agent_id, e)),
            Ok(None) => break, // all senders dropped
            Err(_) => break,   // deadline / idle gap elapsed
        }
    }

    (successes, errors)
}

/// Shared per-connection state handed to the message handler.
struct HandlerShared {
    agents: Arc<RwLock<Vec<AgentInfo>>>,
    pending: PendingMap,
    cipher: Cipher,
    events: EventMap,
    events_notify: Arc<Notify>,
    watched: WatchMap,
    tx: mpsc::Sender<ClientMessage>,
    udp_transport: Arc<UdpTransport>,
    /// When set, this node also executes commands received from other peers.
    executor_state: Option<Arc<crate::state::AgentState>>,
}

/// Agents we can open a UDP data channel to: those that the relay has assigned
/// a `session_id`. Returns `(session_id, name)` pairs.
fn dial_targets(agents: &[AgentInfo]) -> Vec<(String, String)> {
    agents
        .iter()
        .filter_map(|a| a.session_id.clone().map(|s| (s, a.name.clone())))
        .collect()
}

/// Offer a UDP channel to `session` in the background, unless one already
/// exists (connected or mid-handshake) so a list refresh never clobbers an
/// in-flight offer. Failure is non-fatal — WebSocket remains the transport.
async fn spawn_udp_dial(udp: &Arc<UdpTransport>, session: String, name: String) {
    if udp.has_channel(&session).await {
        return;
    }
    let udp = udp.clone();
    tokio::spawn(async move {
        match udp.offer_channel(session).await {
            Ok(channel_id) => {
                info!("Initiated UDP channel {} to agent {}", channel_id, name);
            }
            Err(e) => {
                debug!("Failed to initiate UDP channel to {}: {}", name, e);
            }
        }
    });
}

async fn handle_message(text: &str, shared: &HandlerShared) -> Result<()> {
    let msg: ServerMessage = ServerMessage::from_json(text)?;

    match msg {
        ServerMessage::AgentList { agents: new_agents } => {
            // Dial agents that were already present when we connected (or
            // appeared in a refresh). Without this, only agents that join
            // *after* us via AgentJoined ever get a UDP channel.
            for (session, name) in dial_targets(&new_agents) {
                spawn_udp_dial(&shared.udp_transport, session, name).await;
            }
            let mut list = shared.agents.write().await;
            *list = new_agents;
            debug!("Updated agent list: {} agents", list.len());
        }

        ServerMessage::AgentJoined { agent } => {
            let mut list = shared.agents.write().await;
            // Upsert by id: a re-announce (or a second connection reusing the
            // same persistent node id) must not create a duplicate entry.
            if let Some(slot) = list.iter_mut().find(|a| a.id == agent.id) {
                *slot = agent.clone();
            } else {
                list.push(agent.clone());
            }
            info!("Agent joined: {} ({})", agent.name, agent.id);

            // Initiate UDP channel if agent has session_id.
            if let Some(session_id) = &agent.session_id {
                spawn_udp_dial(&shared.udp_transport, session_id.clone(), agent.name.clone())
                    .await;
            }
        }

        ServerMessage::AgentLeft { agent_id } => {
            let mut list = shared.agents.write().await;
            list.retain(|a| a.id != agent_id);
            info!("Agent left: {}", agent_id);
        }

        ServerMessage::CommandResult {
            request_id,
            agent_id,
            result,
        } => {
            // Decrypt, then forward to the collector (do NOT remove the entry —
            // other agents in the same broadcast still need to deliver).
            let reply = match CommandResult::decrypt(&result, &shared.cipher) {
                Ok(r) => Ok(r),
                Err(e) => {
                    error!("Failed to decrypt result for {}: {}", request_id, e);
                    Err(format!("result decryption failed: {}", e))
                }
            };
            let pending_map = shared.pending.read().await;
            if let Some(tx) = pending_map.get(&request_id) {
                let _ = tx.send((agent_id, reply));
            }
        }

        ServerMessage::CommandError {
            request_id,
            agent_id,
            error,
        } => {
            warn!("Command error from {}: {}", agent_id, error);
            // Per-agent error; record it without dropping other agents' replies.
            let pending_map = shared.pending.read().await;
            if let Some(tx) = pending_map.get(&request_id) {
                let _ = tx.send((agent_id, Err(error)));
            }
        }

        ServerMessage::Event { agent_id, event } => {
            handle_event(shared, &agent_id, event).await;
        }

        // Incoming command from another peer — execute it locally if we have an
        // executor (peer model: this node is a full peer, not just a controller).
        ServerMessage::Command { request_id, payload, .. } => {
            let Some(state) = &shared.executor_state else {
                return Ok(()); // no executor: we don't run others' commands
            };
            let reply = match Command::decrypt(&payload, &shared.cipher) {
                Ok(cmd) => match crate::executor::execute(&cmd, state).await {
                    Ok(result) => match result.encrypt(&shared.cipher) {
                        Ok(envelope) => ClientMessage::CommandResult {
                            request_id,
                            result: envelope,
                        },
                        Err(e) => ClientMessage::CommandError {
                            request_id,
                            error: format!("result encryption failed: {e}"),
                        },
                    },
                    Err(e) => ClientMessage::CommandError {
                        request_id,
                        error: e.to_string(),
                    },
                },
                Err(e) => {
                    warn!("Failed to decrypt incoming command {}: {}", request_id, e);
                    ClientMessage::CommandError {
                        request_id,
                        error: "payload decryption failed (key mismatch?)".to_string(),
                    }
                }
            };
            let _ = shared.tx.send(reply).await;
        }

        ServerMessage::Pong => {
            debug!("Received pong");
        }

        // UDP Signaling messages
        ServerMessage::YourEndpoint { endpoint } => {
            info!("Relay reports our public endpoint: {}", endpoint);
            shared.udp_transport.set_public_endpoint(endpoint).await;
        }

        ServerMessage::UdpOffer { from_session, offer } => {
            debug!("Received UDP offer from {}", from_session);
            if let Err(e) = shared.udp_transport.handle_offer(offer).await {
                warn!("Failed to handle UDP offer: {}", e);
            }
        }

        ServerMessage::UdpAnswer { from_session, answer } => {
            debug!("Received UDP answer from {}", from_session);
            if let Err(e) = shared.udp_transport.handle_answer(answer).await {
                warn!("Failed to handle UDP answer: {}", e);
            }
        }

        ServerMessage::UdpResult { from_session, result } => {
            if result.success {
                info!(
                    "UDP channel {} established with {}",
                    result.channel_id, from_session
                );
            } else {
                warn!(
                    "UDP channel {} failed with {}: {:?}",
                    result.channel_id, from_session, result.error
                );
            }
        }

        _ => {
            debug!("Ignoring message: {:?}", msg);
        }
    }

    Ok(())
}

/// Handle an unsolicited agent event: record status, wake `task_wait` callers,
/// and auto-cancel the reminder cron when a watched task completes.
async fn handle_event(shared: &HandlerShared, agent_id: &str, event: AgentEvent) {
    match event {
        AgentEvent::TaskCompleted { task_id, status } => {
            info!("Event: task {} on {} -> {:?}", task_id, agent_id, status);
            shared.events.write().await.insert(task_id.clone(), status);
            shared.events_notify.notify_waiters();

            // If this task had a reminder cron registered, cancel it now
            // (fire-and-forget ScheduleRemove on the initiator's self-agent).
            if matches!(status, TaskStatus::Done | TaskStatus::Failed) {
                let watch = shared.watched.write().await.remove(&task_id);
                if let Some(watch) = watch {
                    let cmd = Command::ScheduleRemove {
                        name: watch.reminder_name,
                    };
                    if let Ok(envelope) = cmd.encrypt(&shared.cipher) {
                        let _ = shared
                            .tx
                            .send(ClientMessage::Command {
                                request_id: uuid::Uuid::new_v4().to_string(),
                                target: Target::Agent {
                                    id: watch.self_agent_id,
                                },
                                payload: envelope,
                            })
                            .await;
                    }
                }
            }
        }
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn collect_aggregates_successes_and_errors() {
        let (tx, rx) = mpsc::unbounded_channel::<AgentReply>();
        tx.send(("a".into(), Ok(CommandResult::Ok))).unwrap();
        tx.send(("b".into(), Err("boom".into()))).unwrap();
        tx.send(("c".into(), Ok(CommandResult::Ok))).unwrap();
        // expected=3 → early return without hitting any timeout.
        let (ok, err) = collect_replies(rx, 3, false).await;
        assert_eq!(ok.len(), 2);
        assert_eq!(err.len(), 1);
        assert_eq!(err[0].0, "b");
    }

    #[tokio::test]
    async fn collect_single_returns_first_reply() {
        let (tx, rx) = mpsc::unbounded_channel::<AgentReply>();
        tx.send(("a".into(), Ok(CommandResult::Ok))).unwrap();
        tx.send(("b".into(), Ok(CommandResult::Ok))).unwrap();
        let (ok, _err) = collect_replies(rx, 1, true).await;
        assert_eq!(ok.len(), 1, "single target must stop at the first reply");
        assert_eq!(ok[0].0, "a");
    }

    #[tokio::test]
    async fn event_registry_records_and_wakes() {
        // Simulate handle_event's effect on the shared registry + notify.
        let events: EventMap = Arc::new(RwLock::new(HashMap::new()));
        let notify = Arc::new(Notify::new());

        // A waiter arms notified() then awaits.
        let (e2, n2) = (events.clone(), notify.clone());
        let waiter = tokio::spawn(async move {
            loop {
                let notified = n2.notified();
                if let Some(s) = e2.read().await.get("t1").copied() {
                    if matches!(s, TaskStatus::Done | TaskStatus::Failed) {
                        return s;
                    }
                }
                notified.await;
            }
        });

        // Producer records completion and wakes waiters.
        tokio::time::sleep(Duration::from_millis(10)).await;
        events.write().await.insert("t1".into(), TaskStatus::Done);
        notify.notify_waiters();

        let status = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("waiter did not wake")
            .unwrap();
        assert_eq!(status, TaskStatus::Done);
    }

    #[tokio::test]
    async fn collect_no_match_error_is_captured() {
        // Relay "No matching agents found" arrives as an error with empty id.
        let (tx, rx) = mpsc::unbounded_channel::<AgentReply>();
        tx.send(("".into(), Err("No matching agents found".into()))).unwrap();
        // Single target → returns on first reply (no 60s wait).
        let (ok, err) = collect_replies(rx, 1, true).await;
        assert!(ok.is_empty());
        assert_eq!(err.len(), 1);
    }

    fn agent(name: &str, session: Option<&str>) -> AgentInfo {
        AgentInfo {
            id: format!("id-{name}"),
            name: name.to_string(),
            mode: remote_agents_shared::AgentMode::Plan,
            os: "linux".into(),
            arch: "x86_64".into(),
            hostname: "h".into(),
            tags: vec![],
            platform: Default::default(),
            autonomous: false,
            accepts_commands: true,
            connected_at: 0,
            session_id: session.map(String::from),
            update_available: None,
        }
    }

    #[test]
    fn dial_targets_skips_agents_without_session() {
        let agents = vec![
            agent("a", Some("sess-a")),
            agent("b", None), // no session yet → not dialable
            agent("c", Some("sess-c")),
        ];
        let targets = dial_targets(&agents);
        assert_eq!(
            targets,
            vec![
                ("sess-a".to_string(), "a".to_string()),
                ("sess-c".to_string(), "c".to_string()),
            ]
        );
    }

    #[test]
    fn dial_targets_empty_when_no_sessions() {
        assert!(dial_targets(&[]).is_empty());
        assert!(dial_targets(&[agent("a", None)]).is_empty());
    }

    #[test]
    fn udp_session_only_for_known_single_agent() {
        let agents = vec![
            agent("a", Some("sess-a")),
            agent("b", None), // no session yet
        ];
        // Single agent with a session → eligible for UDP (helper ids are "id-<name>").
        assert_eq!(
            udp_session_for(&agents, &Target::Agent { id: "id-a".into() }),
            Some("sess-a".to_string())
        );
        // Agent without a session, unknown agent, and broadcasts → WS only.
        assert_eq!(udp_session_for(&agents, &Target::Agent { id: "id-b".into() }), None);
        assert_eq!(udp_session_for(&agents, &Target::Agent { id: "zzz".into() }), None);
        assert_eq!(udp_session_for(&agents, &Target::All), None);
        assert_eq!(
            udp_session_for(&agents, &Target::Tagged { tags: vec!["x".into()] }),
            None
        );
    }

    #[tokio::test]
    async fn has_channel_false_before_any_offer() {
        let (sig_tx, _rx) = mpsc::channel(4);
        let udp = UdpTransport::new(Cipher::from_passphrase("k"), sig_tx);
        assert!(!udp.has_channel("sess-x").await);
    }

    // --- handle_event: push-driven reminder auto-cancel (Phase 11) -----------

    const WATCH_KEY: &str = "k";

    fn handler_shared() -> (HandlerShared, mpsc::Receiver<ClientMessage>, WatchMap, EventMap) {
        let (tx, rx) = mpsc::channel::<ClientMessage>(8);
        let (sig_tx, _sig_rx) = mpsc::channel(4);
        let cipher = Cipher::from_passphrase(WATCH_KEY);
        let watched: WatchMap = Arc::new(RwLock::new(HashMap::new()));
        let events: EventMap = Arc::new(RwLock::new(HashMap::new()));
        let shared = HandlerShared {
            agents: Arc::new(RwLock::new(Vec::new())),
            pending: Arc::new(RwLock::new(HashMap::new())),
            cipher: cipher.clone(),
            events: events.clone(),
            events_notify: Arc::new(Notify::new()),
            watched: watched.clone(),
            tx,
            udp_transport: Arc::new(UdpTransport::new(cipher, sig_tx)),
            executor_state: None,
        };
        (shared, rx, watched, events)
    }

    #[tokio::test]
    async fn completion_event_cancels_registered_watch() {
        let (shared, mut rx, watched, events) = handler_shared();
        watched.write().await.insert(
            "t1".into(),
            Watch {
                reminder_name: "remind-t1".into(),
                self_agent_id: "self-agent".into(),
            },
        );

        handle_event(
            &shared,
            "host-agent",
            AgentEvent::TaskCompleted { task_id: "t1".into(), status: TaskStatus::Done },
        )
        .await;

        // Status recorded; watch consumed.
        assert_eq!(events.read().await.get("t1").copied(), Some(TaskStatus::Done));
        assert!(!watched.read().await.contains_key("t1"));

        // A ScheduleRemove for the reminder is sent to the INITIATOR's self-agent.
        match rx.try_recv().expect("schedule-remove should be queued") {
            ClientMessage::Command { target, payload, .. } => {
                assert!(matches!(target, Target::Agent { id } if id == "self-agent"));
                let cipher = Cipher::from_passphrase(WATCH_KEY);
                match Command::decrypt(&payload, &cipher).unwrap() {
                    Command::ScheduleRemove { name } => assert_eq!(name, "remind-t1"),
                    other => panic!("expected ScheduleRemove, got {other:?}"),
                }
            }
            other => panic!("expected Command, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn completion_without_watch_sends_nothing() {
        let (shared, mut rx, _watched, events) = handler_shared();
        handle_event(
            &shared,
            "host",
            AgentEvent::TaskCompleted { task_id: "t2".into(), status: TaskStatus::Failed },
        )
        .await;
        assert_eq!(events.read().await.get("t2").copied(), Some(TaskStatus::Failed));
        assert!(rx.try_recv().is_err(), "no watch → no schedule-remove emitted");
    }

    #[tokio::test]
    async fn non_terminal_status_keeps_watch() {
        let (shared, mut rx, watched, events) = handler_shared();
        watched.write().await.insert(
            "t3".into(),
            Watch { reminder_name: "remind-t3".into(), self_agent_id: "s".into() },
        );

        // A non-terminal status records progress but must NOT cancel the reminder.
        handle_event(
            &shared,
            "host",
            AgentEvent::TaskCompleted { task_id: "t3".into(), status: TaskStatus::Running },
        )
        .await;

        assert_eq!(events.read().await.get("t3").copied(), Some(TaskStatus::Running));
        assert!(watched.read().await.contains_key("t3"), "watch must survive non-terminal status");
        assert!(rx.try_recv().is_err());
    }

    // --- single-peer execution: a node also runs others' commands (step 2c) ---

    /// A `HandlerShared` with a local executor, so it runs incoming commands.
    fn shared_with_executor() -> (HandlerShared, mpsc::Receiver<ClientMessage>, Cipher) {
        let (tx, rx) = mpsc::channel::<ClientMessage>(8);
        let (sig_tx, _sig_rx) = mpsc::channel(4);
        let cipher = Cipher::from_passphrase(WATCH_KEY);
        let state = Arc::new(crate::state::AgentState::new(crate::config::Config::default()));
        let shared = HandlerShared {
            agents: Arc::new(RwLock::new(Vec::new())),
            pending: Arc::new(RwLock::new(HashMap::new())),
            cipher: cipher.clone(),
            events: Arc::new(RwLock::new(HashMap::new())),
            events_notify: Arc::new(Notify::new()),
            watched: Arc::new(RwLock::new(HashMap::new())),
            tx,
            udp_transport: Arc::new(UdpTransport::new(cipher.clone(), sig_tx)),
            executor_state: Some(state),
        };
        (shared, rx, cipher)
    }

    fn command_msg(cipher: &Cipher, request_id: &str) -> String {
        let payload = Command::GetInfo.encrypt(cipher).unwrap();
        serde_json::to_string(&ServerMessage::Command {
            request_id: request_id.to_string(),
            from_session: "peer".to_string(),
            payload,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn incoming_command_executed_when_executor_present() {
        let (shared, mut rx, cipher) = shared_with_executor();
        handle_message(&command_msg(&cipher, "r1"), &shared).await.unwrap();

        match rx.try_recv().expect("a result should be queued") {
            ClientMessage::CommandResult { request_id, result } => {
                assert_eq!(request_id, "r1");
                let decrypted = CommandResult::decrypt(&result, &cipher).unwrap();
                assert!(matches!(decrypted, CommandResult::Info { .. }));
            }
            other => panic!("expected CommandResult, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn agent_joined_upserts_by_id() {
        let (shared, _rx, _w, _e) = handler_shared();

        let a = agent("x", Some("s1")); // id = "id-x"
        let text = serde_json::to_string(&ServerMessage::AgentJoined { agent: a }).unwrap();
        handle_message(&text, &shared).await.unwrap();

        // A re-announce / second connection with the SAME id must not duplicate.
        let mut a2 = agent("x", Some("s2"));
        a2.name = "x-renamed".into();
        let text2 = serde_json::to_string(&ServerMessage::AgentJoined { agent: a2 }).unwrap();
        handle_message(&text2, &shared).await.unwrap();

        let list = shared.agents.read().await;
        assert_eq!(list.len(), 1, "same id must not appear twice");
        assert_eq!(list[0].name, "x-renamed", "upsert replaces in place");
    }

    #[tokio::test]
    async fn incoming_command_ignored_without_executor() {
        // A send-only node (no executor) silently drops incoming commands.
        let (shared, mut rx, _watched, _events) = handler_shared();
        let cipher = Cipher::from_passphrase(WATCH_KEY);
        handle_message(&command_msg(&cipher, "r1"), &shared).await.unwrap();
        assert!(rx.try_recv().is_err(), "no executor → no reply");
    }
}
