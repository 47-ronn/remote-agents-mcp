//! Per-connection WebSocket handling and message routing.
//!
//! Mirrors `worker/src/room.ts`: auth → register → route. Each connection has a
//! bounded outbound channel drained by a dedicated writer task, so routing never
//! blocks on socket I/O and a slow consumer is bounded (and dropped) rather than
//! stalling the room.

use crate::routing::{dedup_agents, resolve_targets};
use crate::state::{AgentSession, McpSession, RelayState, Room, Tx, OUTBOUND_CAP};
use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use remote_agents_shared::{AgentInfo, ClientMessage, Endpoint, ServerMessage};
use std::net::IpAddr;
use std::ops::ControlFlow;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info};
use uuid::Uuid;

/// Drive a single accepted WebSocket connection to completion.
pub async fn handle_socket(
    socket: WebSocket,
    room_name: String,
    query_token: Option<String>,
    client_ip: IpAddr,
    state: Arc<RelayState>,
) {
    let (mut sink, mut stream) = socket.split();

    // --- 1. Authenticate: the first frame must be an Auth message ----------
    let first = match stream.next().await {
        Some(Ok(Message::Text(t))) => t,
        _ => return,
    };
    let agent_info = match ClientMessage::from_json(&first) {
        Ok(ClientMessage::Auth {
            token, agent_info, ..
        }) => {
            if !auth_ok(&state, query_token.as_deref(), &token) {
                let _ = sink
                    .send(Message::Text(
                        ServerMessage::AuthFailed {
                            reason: "invalid token".to_string(),
                        }
                        .to_json()
                        .unwrap_or_default(),
                    ))
                    .await;
                return;
            }
            agent_info
        }
        _ => {
            let _ = sink
                .send(Message::Text(
                    ServerMessage::Error {
                        message: "expected auth".to_string(),
                    }
                    .to_json()
                    .unwrap_or_default(),
                ))
                .await;
            return;
        }
    };

    let session_id = Uuid::new_v4().to_string();
    let room = state.room(&room_name);

    // Send auth_ok before handing the sink to the writer task.
    if sink
        .send(Message::Text(
            ServerMessage::AuthOk {
                session_id: session_id.clone(),
            }
            .to_json()
            .unwrap_or_default(),
        ))
        .await
        .is_err()
    {
        state.gc_room(&room_name);
        return;
    }

    // --- 2. Outbound channel + writer task ---------------------------------
    let (tx, mut rx) = mpsc::channel::<String>(OUTBOUND_CAP);
    let mut writer = tokio::spawn(async move {
        while let Some(json) = rx.recv().await {
            if sink.send(Message::Text(json)).await.is_err() {
                break;
            }
        }
    });

    // Reflect the client's observed public IP so it can build a reachable UDP
    // endpoint for hole-punching (port is the client's own UDP port, so 0 here).
    send_to(&tx, &ServerMessage::YourEndpoint {
        endpoint: Endpoint::new(client_ip, 0),
    });

    // --- 3. Register the session -------------------------------------------
    // Peer model: a node with an identity (agent_info) joins as a visible peer;
    // an anonymous connection (no agent_info — e.g. a browser stats/control
    // client) joins as an unlisted observer that still receives broadcasts.
    match &agent_info {
        Some(info) => {
            // Clone and add session_id for UDP signaling (unbox to AgentInfo).
            let mut info_with_session = (**info).clone();
            info_with_session.session_id = Some(session_id.clone());

            // Keyed by session id: one entry per live connection. A machine may
            // hold several connections (many terminals); they collapse to one
            // logical peer on read via dedup_agents.
            room.agents.insert(
                session_id.clone(),
                AgentSession {
                    info: info_with_session.clone(),
                    tx: tx.clone(),
                },
            );

            // Tell the newcomer who its peers are (everyone already here, minus
            // itself), collapsed to one logical host per machine.
            let peers: Vec<AgentInfo> = dedup_agents(&room)
                .into_iter()
                .filter(|a| a.id != info.id)
                .collect();
            send_to(&tx, &ServerMessage::AgentList { agents: peers });

            // Announce the join, carrying the host's MERGED capabilities (this id
            // may already have other connections), so caches aren't downgraded by
            // a less-capable terminal.
            let merged = dedup_agents(&room)
                .into_iter()
                .find(|a| a.id == info.id)
                .unwrap_or_else(|| info_with_session.clone());
            let joined = ServerMessage::AgentJoined { agent: Box::new(merged) };
            broadcast_mcp(&room, &joined);
            broadcast_agents(&room, &joined, Some(&info.id));
            info!("peer joined: {} ({})", info.name, info.id);
        }
        None => {
            room.mcp.insert(
                session_id.clone(),
                McpSession {
                    id: session_id.clone(),
                    tx: tx.clone(),
                },
            );
            debug!("anonymous observer joined: {}", session_id);
        }
    }

    // --- 4. Reader loop -----------------------------------------------------
    // A frame must arrive within `idle_timeout` (agents ping every 30s, the
    // panel polls every ~5s). If none does, the TCP died silently (no close
    // frame) — reap the connection so it doesn't linger as a phantom session.
    let idle_timeout = state.idle_timeout;
    loop {
        tokio::select! {
            incoming = tokio::time::timeout(idle_timeout, stream.next()) => {
                match incoming {
                    Err(_elapsed) => {
                        debug!("idle timeout, reaping session {}", session_id);
                        break;
                    }
                    Ok(Some(Ok(Message::Text(text)))) => {
                        if handle_client_msg(&text, &room, &session_id, &agent_info, &tx)
                            .is_break()
                        {
                            break;
                        }
                    }
                    Ok(Some(Ok(Message::Close(_)))) | Ok(None) => break,
                    Ok(Some(Err(_))) => break,
                    // Control frames (ping/pong/binary) are unused by our protocol.
                    Ok(_) => {}
                }
            }
            _ = &mut writer => break, // peer's read half gone
        }
    }

    // --- 5. Cleanup ---------------------------------------------------------
    writer.abort();
    match &agent_info {
        Some(info) => {
            // Drop just THIS connection (keyed by session id). Announce the peer
            // as gone only if no other connection of the same machine remains —
            // a host with several open terminals stays present until its last
            // one leaves.
            room.agents.remove(&session_id);
            let still_present = room.agents.iter().any(|e| e.value().info.id == info.id);
            if !still_present {
                let left = ServerMessage::AgentLeft {
                    agent_id: info.id.clone(),
                };
                broadcast_mcp(&room, &left);
                broadcast_agents(&room, &left, None);
                info!("peer left: {}", info.id);
            }
        }
        None => {
            room.mcp.remove(&session_id);
        }
    }
    // Drop any in-flight requests this session initiated; their results would
    // have nowhere to go now.
    room.pending.retain(|_, origin| origin != &session_id);
    state.gc_room(&room_name);
}

/// Auth parity with the Cloudflare worker, plus optional server-enforced token:
/// - if a server token is configured, the auth token must equal it;
/// - otherwise, the auth token must equal the connection's query token. Our
///   clients always send the same value in both, so legit connections are
///   unaffected; an empty/absent query token now only admits an empty auth token
///   (previously it admitted ANY auth token — an open-access hole).
fn auth_ok(state: &RelayState, query_token: Option<&str>, auth_token: &str) -> bool {
    if let Some(server) = &state.token {
        return auth_token == server;
    }
    query_token.unwrap_or("") == auth_token
}

/// Route one client message. Returns `Break` to close the connection.
fn handle_client_msg(
    text: &str,
    room: &Room,
    session_id: &str,
    agent_info: &Option<Box<AgentInfo>>,
    self_tx: &Tx,
) -> ControlFlow<()> {
    debug!("Received raw message from {}: {} bytes", session_id, text.len());
    let msg = match ClientMessage::from_json(text) {
        Ok(m) => {
            debug!("Parsed message type: {:?}", std::mem::discriminant(&m));
            m
        }
        Err(e) => {
            tracing::warn!("ignoring unparseable message from {}: {} - first 200 chars: {}", session_id, e, &text[..text.len().min(200)]);
            return ControlFlow::Continue(());
        }
    };

    // Peer model: no network roles. Any peer may list the room, send commands,
    // and return results/events. Whether a node *executes* a received command is
    // its own choice (AgentInfo.accepts_commands / --no-agent), enforced agent-side.
    match msg {
        ClientMessage::ListAgents => {
            // One logical host per machine (collapse its several connections).
            let agents = dedup_agents(room);
            send_to(self_tx, &ServerMessage::AgentList { agents });
        }

        ClientMessage::Command {
            request_id,
            target,
            payload,
        } => {
            info!("Received command {} from {} targeting {:?}", request_id, session_id, target);
            let targets = resolve_targets(room, &target);
            info!("Resolved {} targets for command {}", targets.len(), request_id);
            if targets.is_empty() {
                info!("No targets found for command {}, sending error", request_id);
                send_to(
                    self_tx,
                    &ServerMessage::CommandError {
                        request_id,
                        agent_id: String::new(),
                        error: "No matching agents found".to_string(),
                    },
                );
            } else {
                // Remember who asked, so the result(s) route back to them only.
                room.pending
                    .insert(request_id.clone(), session_id.to_string());
                for (agent_name, tx) in &targets {
                    info!("Sending command {} to agent {}", request_id, agent_name);
                    send_raw(
                        tx,
                        &ServerMessage::Command {
                            request_id: request_id.clone(),
                            from_session: session_id.to_string(),
                            payload: payload.clone(),
                        },
                    );
                }
            }
        }

        ClientMessage::CommandResult { request_id, result } => {
            let agent_id = agent_id_of(agent_info);
            let msg = ServerMessage::CommandResult {
                request_id: request_id.clone(),
                agent_id,
                result,
            };
            route_to_origin(room, &request_id, &msg);
        }

        ClientMessage::CommandError { request_id, error } => {
            let agent_id = agent_id_of(agent_info);
            let msg = ServerMessage::CommandError {
                request_id: request_id.clone(),
                agent_id,
                error,
            };
            route_to_origin(room, &request_id, &msg);
        }

        ClientMessage::Notify { event } => {
            let agent_id = agent_id_of(agent_info);
            broadcast_mcp(room, &ServerMessage::Event { agent_id, event });
        }

        // UDP Signaling: forward offer to target session (agent or MCP),
        // addressed by session id (mirrors the worker's findSocketBySession).
        ClientMessage::UdpOffer(offer) => {
            let to_session = offer.to_session.clone();
            if let Some(target_tx) = crate::routing::find_session_tx(room, &to_session) {
                send_raw(&target_tx, &ServerMessage::UdpOffer {
                    from_session: session_id.to_string(),
                    offer,
                });
            } else {
                debug!("UDP offer target {} not found", to_session);
            }
        }

        // UDP Signaling: forward answer back to offering session
        ClientMessage::UdpAnswer(answer) => {
            // The answer goes back to the session that made the offer
            // which is stored in answer.channel_id's origin, but we don't track that
            // Instead, forward to MCP clients which track channels
            broadcast_mcp(room, &ServerMessage::UdpAnswer {
                from_session: session_id.to_string(),
                answer,
            });
        }

        // UDP Signaling: forward channel result
        ClientMessage::UdpResult(result) => {
            broadcast_mcp(room, &ServerMessage::UdpResult {
                from_session: session_id.to_string(),
                result,
            });
        }

        ClientMessage::Ping => send_to(self_tx, &ServerMessage::Pong),

        // Already authenticated; a second auth is ignored.
        ClientMessage::Auth { .. } => {}

        ClientMessage::Close => return ControlFlow::Break(()),

        // Agent is updating its info (e.g. after mode change)
        ClientMessage::UpdateAgent { agent_info: new_info } => {
            // Update the stored agent info for this session, then DROP the write
            // guard BEFORE re-reading the map. `get_mut` holds a write lock on the
            // session's DashMap shard; `dedup_agents`/`broadcast_agents` below call
            // `room.agents.iter()`, which tries to lock every shard — including the
            // one still held here — and would self-deadlock the task (parked at 0%
            // CPU while holding the lock), cascading into a full relay wedge.
            let updated = {
                let Some(mut entry) = room.agents.get_mut(session_id) else {
                    return ControlFlow::Continue(());
                };
                // Preserve session_id from the existing entry.
                let mut info = (*new_info).clone();
                info.session_id = entry.info.session_id.clone();
                entry.info = info.clone();
                info
            }; // RefMut released here, before any re-lock of room.agents.

            // Broadcast the update to all peers (like AgentJoined but for updates).
            let merged = dedup_agents(room)
                .into_iter()
                .find(|a| a.id == new_info.id)
                .unwrap_or(updated);
            let update_msg = ServerMessage::AgentJoined { agent: Box::new(merged) };
            broadcast_mcp(room, &update_msg);
            broadcast_agents(room, &update_msg, Some(&new_info.id));
            debug!("agent updated: {} ({})", new_info.name, new_info.id);
        }
    }

    ControlFlow::Continue(())
}

fn agent_id_of(info: &Option<Box<AgentInfo>>) -> String {
    info.as_ref().map(|a| a.id.clone()).unwrap_or_default()
}

/// Route a command result/error back to the session that issued the request.
/// Falls back to broadcasting to controllers if the origin is unknown (e.g. a
/// late/duplicate reply) or has since disconnected. The pending entry is kept
/// (not removed) so broadcasts — one request_id, many agent replies — all route.
fn route_to_origin(room: &Room, request_id: &str, msg: &ServerMessage) {
    if let Some(origin) = room.pending.get(request_id).map(|e| e.value().clone()) {
        if let Some(tx) = crate::routing::find_session_tx(room, &origin) {
            send_raw(&tx, msg);
            return;
        }
    }
    broadcast_mcp(room, msg);
}

/// Non-blocking send to one connection. A full queue means a slow/stuck
/// consumer; the message is dropped (the writer task / disconnect handles the
/// dead connection).
fn send_raw(tx: &Tx, msg: &ServerMessage) {
    if let Ok(json) = msg.to_json() {
        if tx.try_send(json).is_err() {
            tracing::warn!("Dropping message to slow/closed consumer (channel full)");
        }
    }
}

fn send_to(tx: &Tx, msg: &ServerMessage) {
    send_raw(tx, msg);
}

fn broadcast_mcp(room: &Room, msg: &ServerMessage) {
    if let Ok(json) = msg.to_json() {
        for entry in room.mcp.iter() {
            let _ = entry.tx.try_send(json.clone());
        }
    }
}

/// Broadcast to all agents in the room, optionally skipping one id (e.g. the
/// agent that triggered the event). Lets each host learn about its neighbours.
fn broadcast_agents(room: &Room, msg: &ServerMessage, except_id: Option<&str>) {
    if let Ok(json) = msg.to_json() {
        for entry in room.agents.iter() {
            // Skip every connection of the excepted machine (agents are keyed by
            // session id now, so compare the agent id, not the map key).
            if Some(entry.value().info.id.as_str()) == except_id {
                continue;
            }
            let _ = entry.tx.try_send(json.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_ok_server_token_takes_precedence_and_must_match() {
        let s = RelayState::new(Some("srv".into()));
        // Server token is the only thing that matters when configured.
        assert!(auth_ok(&s, Some("anything"), "srv"));
        assert!(!auth_ok(&s, Some("srv"), "wrong")); // mismatch rejected even if query matches
        assert!(!auth_ok(&s, None, "wrong"));
    }

    #[test]
    fn auth_ok_query_token_when_no_server_token() {
        let s = RelayState::new(None);
        assert!(auth_ok(&s, Some("q"), "q")); // query must equal auth
        assert!(!auth_ok(&s, Some("q"), "x")); // mismatch rejected
        // Empty/absent query no longer admits an arbitrary auth token (the
        // closed open-access hole): it must also be empty.
        assert!(!auth_ok(&s, Some(""), "whatever"));
        assert!(!auth_ok(&s, None, "whatever"));
        assert!(auth_ok(&s, Some(""), "")); // empty == empty (token-less dev)
        assert!(auth_ok(&s, None, "")); // absent query == empty auth
    }
}

/// Routing tests for `handle_client_msg`: drive the dispatch path directly with
/// in-memory `Room`s and mpsc channels (no real WebSocket), asserting which
/// connection each message lands on. This is the relay's core fan-out/route-back
/// logic, security-relevant and previously only covered indirectly via routing.rs.
#[cfg(test)]
mod routing_tests {
    use super::*;
    use crate::state::{AgentSession, McpSession, Room};
    use remote_agents_shared::{AgentEvent, AgentInfo, AgentMode, Target, TaskStatus};
    use tokio::sync::mpsc::{self, Receiver};

    fn info(id: &str, session: &str) -> AgentInfo {
        AgentInfo {
            id: id.into(),
            name: "n".into(),
            mode: AgentMode::Edit,
            os: "linux".into(),
            arch: "x86_64".into(),
            hostname: "h".into(),
            tags: vec![],
            platform: Default::default(),
            autonomous: false,
            accepts_commands: true,
            connected_at: 0,
            session_id: Some(session.into()),
            version: String::new(),
            update_available: None,
            connections: None,
        }
    }

    fn chan() -> (Tx, Receiver<String>) {
        mpsc::channel(OUTBOUND_CAP)
    }

    fn add_agent(room: &Room, session_id: &str, id: &str) -> Receiver<String> {
        let (tx, rx) = chan();
        room.agents
            .insert(session_id.into(), AgentSession { info: info(id, session_id), tx });
        rx
    }

    fn add_mcp(room: &Room, session_id: &str) -> Receiver<String> {
        let (tx, rx) = chan();
        room.mcp
            .insert(session_id.into(), McpSession { id: session_id.into(), tx });
        rx
    }

    /// Pop the next frame off a connection's outbound queue, parsed.
    fn next(rx: &mut Receiver<String>) -> Option<ServerMessage> {
        rx.try_recv().ok().and_then(|j| ServerMessage::from_json(&j).ok())
    }

    fn dispatch(
        room: &Room,
        session_id: &str,
        agent: &Option<Box<AgentInfo>>,
        self_tx: &Tx,
        msg: ClientMessage,
    ) -> ControlFlow<()> {
        handle_client_msg(&msg.to_json().unwrap(), room, session_id, agent, self_tx)
    }

    #[test]
    fn command_to_all_fans_out_to_agents_and_records_pending() {
        let room = Room::default();
        let mut a1 = add_agent(&room, "s-a1", "agentA");
        let mut a2 = add_agent(&room, "s-a2", "agentB");
        let (self_tx, _self_rx) = chan(); // an MCP/panel origin (anonymous)

        let flow = dispatch(
            &room,
            "origin-sess",
            &None,
            &self_tx,
            ClientMessage::Command {
                request_id: "req1".into(),
                target: Target::All,
                payload: "ENC".into(),
            },
        );
        assert!(flow.is_continue());

        for rx in [&mut a1, &mut a2] {
            match next(rx) {
                Some(ServerMessage::Command { request_id, payload, from_session }) => {
                    assert_eq!(request_id, "req1");
                    assert_eq!(payload, "ENC");
                    assert_eq!(from_session, "origin-sess");
                }
                other => panic!("each agent should receive the Command, got {other:?}"),
            }
        }
        // Origin recorded so replies route back to it specifically.
        assert_eq!(
            room.pending.get("req1").map(|e| e.value().clone()),
            Some("origin-sess".to_string())
        );
    }

    #[test]
    fn command_with_no_matching_target_errors_back_to_sender_only() {
        let room = Room::default(); // no agents at all
        let (self_tx, mut self_rx) = chan();

        let _ = dispatch(
            &room,
            "origin",
            &None,
            &self_tx,
            ClientMessage::Command {
                request_id: "req2".into(),
                target: Target::Agent { id: "ghost".into() },
                payload: "X".into(),
            },
        );

        match next(&mut self_rx) {
            Some(ServerMessage::CommandError { request_id, error, .. }) => {
                assert_eq!(request_id, "req2");
                assert!(error.contains("No matching"), "got: {error}");
            }
            other => panic!("sender should get CommandError, got {other:?}"),
        }
        // Nothing was dispatched, so nothing is pending.
        assert!(room.pending.is_empty());
    }

    #[test]
    fn command_result_routes_to_origin_only_not_other_observers() {
        let room = Room::default();
        let mut origin = add_mcp(&room, "origin-sess");
        let mut observer = add_mcp(&room, "observer-sess");
        room.pending.insert("req3".into(), "origin-sess".into());

        let agent = Some(Box::new(info("agentA", "s-a1")));
        let (atx, _arx) = chan();
        let _ = dispatch(
            &room,
            "s-a1",
            &agent,
            &atx,
            ClientMessage::CommandResult { request_id: "req3".into(), result: "RES".into() },
        );

        match next(&mut origin) {
            Some(ServerMessage::CommandResult { request_id, agent_id, result }) => {
                assert_eq!(request_id, "req3");
                assert_eq!(agent_id, "agentA");
                assert_eq!(result, "RES");
            }
            other => panic!("origin should receive its result, got {other:?}"),
        }
        assert!(
            next(&mut observer).is_none(),
            "a targeted result must NOT reach unrelated observers"
        );
        // pending is intentionally kept so further (broadcast) replies still route.
        assert!(room.pending.contains_key("req3"));
    }

    #[test]
    fn command_result_with_unknown_request_falls_back_to_mcp_broadcast() {
        let room = Room::default();
        let mut o1 = add_mcp(&room, "m1");
        let mut o2 = add_mcp(&room, "m2");
        // No pending entry for req4: origin unknown (late/duplicate) → broadcast.
        let agent = Some(Box::new(info("agentA", "s-a1")));
        let (atx, _arx) = chan();
        let _ = dispatch(
            &room,
            "s-a1",
            &agent,
            &atx,
            ClientMessage::CommandError { request_id: "req4".into(), error: "boom".into() },
        );

        for rx in [&mut o1, &mut o2] {
            match next(rx) {
                Some(ServerMessage::CommandError { request_id, agent_id, error }) => {
                    assert_eq!(request_id, "req4");
                    assert_eq!(agent_id, "agentA");
                    assert_eq!(error, "boom");
                }
                other => panic!("unknown-origin reply should broadcast to MCP, got {other:?}"),
            }
        }
    }

    #[test]
    fn notify_broadcasts_event_to_mcp_observers_with_agent_id() {
        let room = Room::default();
        let mut o1 = add_mcp(&room, "m1");
        let agent = Some(Box::new(info("agentA", "s-a1")));
        let (atx, _arx) = chan();
        let _ = dispatch(
            &room,
            "s-a1",
            &agent,
            &atx,
            ClientMessage::Notify {
                event: AgentEvent::TaskCompleted {
                    task_id: "t1".into(),
                    status: TaskStatus::Done,
                },
            },
        );
        match next(&mut o1) {
            Some(ServerMessage::Event { agent_id, .. }) => assert_eq!(agent_id, "agentA"),
            other => panic!("expected Event broadcast, got {other:?}"),
        }
    }

    #[test]
    fn ping_replies_pong_to_sender() {
        let room = Room::default();
        let (self_tx, mut self_rx) = chan();
        let flow = dispatch(&room, "s", &None, &self_tx, ClientMessage::Ping);
        assert!(flow.is_continue());
        assert!(matches!(next(&mut self_rx), Some(ServerMessage::Pong)));
    }

    #[test]
    fn close_message_breaks_the_loop() {
        let room = Room::default();
        let (self_tx, _rx) = chan();
        assert!(dispatch(&room, "s", &None, &self_tx, ClientMessage::Close).is_break());
    }

    #[test]
    fn unparseable_message_is_ignored_without_reply() {
        let room = Room::default();
        let (self_tx, mut rx) = chan();
        let flow = handle_client_msg("{ not valid json", &room, "s", &None, &self_tx);
        assert!(flow.is_continue());
        assert!(next(&mut rx).is_none());
    }
}
