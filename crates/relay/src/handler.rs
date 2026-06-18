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
    let msg = match ClientMessage::from_json(text) {
        Ok(m) => m,
        Err(e) => {
            debug!("ignoring unparseable message: {}", e);
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
            let targets = resolve_targets(room, &target);
            if targets.is_empty() {
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
                for (_, tx) in targets {
                    send_raw(
                        &tx,
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
            debug!("dropping message to slow/closed consumer");
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
