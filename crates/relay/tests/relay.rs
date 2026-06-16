//! End-to-end test: spin the relay on an ephemeral port and drive a fake agent
//! and MCP client through auth, agent listing, command routing, and a push
//! event — all over the real WebSocket protocol.

use futures::{SinkExt, StreamExt};
use remote_agents_relay::{router, state::RelayState};
use remote_agents_shared::{
    AgentEvent, AgentInfo, AgentMode, ClientMessage, ClientRole, Endpoint, ServerMessage, Target,
    TaskStatus, UdpOffer,
};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn start_relay() -> u16 {
    let state = Arc::new(RelayState::new(None));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(
            listener,
            router(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    // Give the server a moment to start accepting.
    tokio::time::sleep(Duration::from_millis(50)).await;
    port
}

async fn connect(port: u16, room: &str) -> Ws {
    let url = format!("ws://127.0.0.1:{}/ws/room/{}?token=secret", port, room);
    let (ws, _) = connect_async(url).await.unwrap();
    ws
}

async fn send(ws: &mut Ws, msg: &ClientMessage) {
    ws.send(Message::Text(msg.to_json().unwrap())).await.unwrap();
}

async fn recv(ws: &mut Ws) -> ServerMessage {
    loop {
        match tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("recv timeout")
        {
            Some(Ok(Message::Text(t))) => {
                let msg = ServerMessage::from_json(&t).unwrap();
                // YourEndpoint is connection-setup noise sent to every peer;
                // skip it so tests can assert on the messages they care about.
                if matches!(msg, ServerMessage::YourEndpoint { .. }) {
                    continue;
                }
                return msg;
            }
            Some(Ok(_)) => continue,
            other => panic!("unexpected: {:?}", other),
        }
    }
}

/// Like `recv` but returns `None` if no (non-noise) message arrives within `ms`.
/// Used to assert a peer does NOT receive something.
async fn try_recv(ws: &mut Ws, ms: u64) -> Option<ServerMessage> {
    loop {
        match tokio::time::timeout(Duration::from_millis(ms), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let msg = ServerMessage::from_json(&t).unwrap();
                if matches!(msg, ServerMessage::YourEndpoint { .. }) {
                    continue;
                }
                return Some(msg);
            }
            Ok(Some(Ok(_))) => continue,
            Ok(_) => return None,  // connection closed
            Err(_) => return None, // timeout → nothing arrived
        }
    }
}

fn agent_info(id: &str, tags: &[&str]) -> AgentInfo {
    AgentInfo {
        id: id.to_string(),
        name: id.to_string(),
        mode: AgentMode::Bypass,
        os: "linux".into(),
        arch: "x86_64".into(),
        hostname: id.into(),
        tags: tags.iter().map(|s| s.to_string()).collect(),
        platform: Default::default(),
        autonomous: true,
        accepts_commands: true,
        connected_at: 0,
        session_id: None,
        update_available: None,
    }
}

async fn auth(ws: &mut Ws, role: ClientRole, info: Option<AgentInfo>) -> String {
    send(
        ws,
        &ClientMessage::Auth {
            room: "dev".into(),
            token: "secret".into(),
            role,
            agent_info: info.map(Box::new),
        },
    )
    .await;
    match recv(ws).await {
        ServerMessage::AuthOk { session_id } => session_id,
        other => panic!("expected auth_ok, got {:?}", other),
    }
}

#[tokio::test]
async fn full_round_trip() {
    let port = start_relay().await;

    // Agent connects first.
    let mut agent = connect(port, "dev").await;
    auth(&mut agent, ClientRole::Agent, Some(agent_info("a1", &["backend"]))).await;
    // On joining the (empty) room the agent receives its initial peer list.
    match recv(&mut agent).await {
        ServerMessage::AgentList { agents } => assert!(agents.is_empty()),
        other => panic!("expected initial agent_list, got {:?}", other),
    }

    // MCP connects.
    let mut mcp = connect(port, "dev").await;
    auth(&mut mcp, ClientRole::Mcp, None).await;

    // MCP lists agents and sees the one agent.
    send(&mut mcp, &ClientMessage::ListAgents).await;
    match recv(&mut mcp).await {
        ServerMessage::AgentList { agents } => {
            assert_eq!(agents.len(), 1);
            assert_eq!(agents[0].id, "a1");
            assert!(agents[0].autonomous);
        }
        other => panic!("expected agent_list, got {:?}", other),
    }

    // MCP sends a command targeting all; the agent receives it.
    send(
        &mut mcp,
        &ClientMessage::Command {
            request_id: "req-1".into(),
            target: Target::All,
            payload: "ENCRYPTED_PAYLOAD".into(),
        },
    )
    .await;
    match recv(&mut agent).await {
        ServerMessage::Command {
            request_id, payload, ..
        } => {
            assert_eq!(request_id, "req-1");
            assert_eq!(payload, "ENCRYPTED_PAYLOAD"); // forwarded opaquely
        }
        other => panic!("expected command, got {:?}", other),
    }

    // Agent replies; MCP receives the result tagged with the agent id.
    send(
        &mut agent,
        &ClientMessage::CommandResult {
            request_id: "req-1".into(),
            result: "ENCRYPTED_RESULT".into(),
        },
    )
    .await;
    match recv(&mut mcp).await {
        ServerMessage::CommandResult {
            request_id,
            agent_id,
            result,
        } => {
            assert_eq!(request_id, "req-1");
            assert_eq!(agent_id, "a1");
            assert_eq!(result, "ENCRYPTED_RESULT");
        }
        other => panic!("expected command_result, got {:?}", other),
    }

    // Agent pushes an unsolicited completion event; MCP receives it.
    send(
        &mut agent,
        &ClientMessage::Notify {
            event: AgentEvent::TaskCompleted {
                task_id: "t1".into(),
                status: TaskStatus::Done,
            },
        },
    )
    .await;
    match recv(&mut mcp).await {
        ServerMessage::Event { agent_id, event } => {
            assert_eq!(agent_id, "a1");
            matches!(event, AgentEvent::TaskCompleted { .. });
        }
        other => panic!("expected event, got {:?}", other),
    }

    // Tagged targeting that matches nothing yields a no-match error to the MCP.
    send(
        &mut mcp,
        &ClientMessage::Command {
            request_id: "req-2".into(),
            target: Target::Tagged {
                tags: vec!["frontend".into()],
            },
            payload: "X".into(),
        },
    )
    .await;
    match recv(&mut mcp).await {
        ServerMessage::CommandError { request_id, .. } => assert_eq!(request_id, "req-2"),
        other => panic!("expected command_error, got {:?}", other),
    }

    // When the agent disconnects, MCP is notified.
    drop(agent);
    match recv(&mut mcp).await {
        ServerMessage::AgentLeft { agent_id } => assert_eq!(agent_id, "a1"),
        other => panic!("expected agent_left, got {:?}", other),
    }
}

/// Peer awareness: agents — not just MCP clients — are told who shares the room,
/// so each host knows its surroundings (their OS/platform/tags).
#[tokio::test]
async fn agents_learn_about_each_other() {
    let port = start_relay().await;

    // First agent joins an empty room → empty initial peer list.
    let mut a1 = connect(port, "dev").await;
    auth(&mut a1, ClientRole::Agent, Some(agent_info("a1", &["backend"]))).await;
    match recv(&mut a1).await {
        ServerMessage::AgentList { agents } => assert!(agents.is_empty()),
        other => panic!("expected empty agent_list, got {:?}", other),
    }

    // Second agent joins → its initial list already contains a1.
    let mut a2 = connect(port, "dev").await;
    auth(&mut a2, ClientRole::Agent, Some(agent_info("a2", &["frontend"]))).await;
    match recv(&mut a2).await {
        ServerMessage::AgentList { agents } => {
            assert_eq!(agents.len(), 1);
            assert_eq!(agents[0].id, "a1");
        }
        other => panic!("expected agent_list with a1, got {:?}", other),
    }

    // a1 is told that a2 joined (peer event, not only to MCP).
    match recv(&mut a1).await {
        ServerMessage::AgentJoined { agent } => assert_eq!(agent.id, "a2"),
        other => panic!("expected agent_joined a2, got {:?}", other),
    }

    // When a2 leaves, a1 is notified.
    drop(a2);
    match recv(&mut a1).await {
        ServerMessage::AgentLeft { agent_id } => assert_eq!(agent_id, "a2"),
        other => panic!("expected agent_left a2, got {:?}", other),
    }
}

/// Minimal dependency-free HTTP/1.1 GET; returns (status_code, body).
async fn http_get(port: u16, path: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

#[tokio::test]
async fn health_endpoint_returns_ok() {
    let port = start_relay().await;
    let (status, body) = http_get(port, "/health").await;
    assert_eq!(status, 200);
    assert!(body.contains("\"status\":\"ok\""), "body: {body}");
    assert!(body.contains("remote-agents-relay"));
}

#[tokio::test]
async fn room_info_reports_connected_agents() {
    let port = start_relay().await;

    // Unknown/empty room → zero agents, zero mcp clients.
    let (status, body) = http_get(port, "/api/room/dev").await;
    assert_eq!(status, 200);
    assert!(body.contains("\"agents\":[]"), "body: {body}");
    assert!(body.contains("\"mcp_clients\":0"));

    // After an agent joins it shows up in the room info.
    let mut agent = connect(port, "dev").await;
    auth(&mut agent, ClientRole::Agent, Some(agent_info("a1", &["backend"]))).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (_status, body) = http_get(port, "/api/room/dev").await;
    assert!(body.contains("\"id\":\"a1\""), "body: {body}");
}

/// The relay reflects the client's observed IP via `YourEndpoint` (so peers can
/// build a reachable UDP endpoint for hole-punching).
#[tokio::test]
async fn relay_reflects_your_endpoint() {
    let port = start_relay().await;
    let mut agent = connect(port, "dev").await;
    auth(&mut agent, ClientRole::Agent, Some(agent_info("a1", &[]))).await;

    // Read raw frames (don't use recv(), which filters YourEndpoint out).
    let mut saw = None;
    for _ in 0..5 {
        if let Some(Ok(Message::Text(t))) = tokio::time::timeout(Duration::from_secs(2), agent.next())
            .await
            .expect("timeout")
        {
            if let ServerMessage::YourEndpoint { endpoint } = ServerMessage::from_json(&t).unwrap() {
                saw = Some(endpoint);
                break;
            }
        }
    }
    let ep = saw.expect("expected a YourEndpoint frame");
    assert_eq!(ep.addr, IpAddr::V4(Ipv4Addr::LOCALHOST)); // loopback client
}

fn make_offer(from: &str, to: &str) -> UdpOffer {
    UdpOffer {
        channel_id: "ch1".into(),
        from_session: from.to_string(),
        to_session: to.to_string(),
        local_endpoint: Endpoint::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5000),
        public_endpoint: None,
        nonce: [0u8; 16],
    }
}

/// Peer model (iter75): the relay no longer enforces network roles. An agent
/// issuing a fleet command is PROCESSED (not rejected with "Not authorized") —
/// here it targets a non-matching tag, so it gets a normal no-match error.
#[tokio::test]
async fn relay_has_no_role_authorization() {
    let port = start_relay().await;

    let mut agent = connect(port, "dev").await;
    auth(&mut agent, ClientRole::Agent, Some(agent_info("a", &[]))).await;
    assert!(matches!(recv(&mut agent).await, ServerMessage::AgentList { .. })); // initial peers

    send(
        &mut agent,
        &ClientMessage::Command {
            request_id: "x".into(),
            target: Target::Tagged { tags: vec!["nobody".into()] },
            payload: "P".into(),
        },
    )
    .await;
    // The command is accepted and resolved (no match) — NOT a role rejection.
    match recv(&mut agent).await {
        ServerMessage::CommandError { request_id, error, .. } => {
            assert_eq!(request_id, "x");
            assert!(error.contains("No matching"), "got: {error}");
        }
        other => panic!("expected no-match CommandError, got {:?}", other),
    }
}

/// UDP signaling must route an offer to the target by **session id** (not agent
/// id). Regression guard for the iter25 relay bug.
#[tokio::test]
async fn udp_offer_routes_to_target_agent_by_session() {
    let port = start_relay().await;

    let mut a = connect(port, "dev").await;
    let a_sess = auth(&mut a, ClientRole::Agent, Some(agent_info("a", &[]))).await;
    assert!(matches!(recv(&mut a).await, ServerMessage::AgentList { .. }));

    let mut b = connect(port, "dev").await;
    let b_sess = auth(&mut b, ClientRole::Agent, Some(agent_info("b", &[]))).await;
    assert!(matches!(recv(&mut b).await, ServerMessage::AgentList { .. }));
    // a is told b joined.
    assert!(matches!(recv(&mut a).await, ServerMessage::AgentJoined { .. }));

    // a offers a UDP channel addressed to b's SESSION id.
    send(&mut a, &ClientMessage::UdpOffer(make_offer(&a_sess, &b_sess))).await;

    match recv(&mut b).await {
        ServerMessage::UdpOffer { from_session, offer } => {
            assert_eq!(from_session, a_sess);
            assert_eq!(offer.to_session, b_sess);
            assert_eq!(offer.channel_id, "ch1");
        }
        other => panic!("expected udp_offer at b, got {:?}", other),
    }
}

/// Peer-model routing: a command result returns ONLY to the peer that issued the
/// command, not to every controller in the room (iter74).
#[tokio::test]
async fn result_routes_only_to_requesting_mcp() {
    let port = start_relay().await;

    // An executing agent.
    let mut agent = connect(port, "dev").await;
    auth(&mut agent, ClientRole::Agent, Some(agent_info("a1", &[]))).await;
    match recv(&mut agent).await {
        ServerMessage::AgentList { .. } => {}
        other => panic!("expected AgentList, got {:?}", other),
    }

    // Two controllers in the same room.
    let mut mcp1 = connect(port, "dev").await;
    auth(&mut mcp1, ClientRole::Mcp, None).await;
    let mut mcp2 = connect(port, "dev").await;
    auth(&mut mcp2, ClientRole::Mcp, None).await;

    // mcp1 issues the command.
    send(
        &mut mcp1,
        &ClientMessage::Command {
            request_id: "req-x".into(),
            target: Target::Agent { id: "a1".into() },
            payload: "P".into(),
        },
    )
    .await;
    match recv(&mut agent).await {
        ServerMessage::Command { request_id, .. } => assert_eq!(request_id, "req-x"),
        other => panic!("expected Command at agent, got {:?}", other),
    }
    send(
        &mut agent,
        &ClientMessage::CommandResult {
            request_id: "req-x".into(),
            result: "R".into(),
        },
    )
    .await;

    // The initiator (mcp1) receives the result...
    match recv(&mut mcp1).await {
        ServerMessage::CommandResult { request_id, result, .. } => {
            assert_eq!(request_id, "req-x");
            assert_eq!(result, "R");
        }
        other => panic!("expected CommandResult at mcp1, got {:?}", other),
    }
    // ...the bystander controller (mcp2) does NOT.
    assert!(
        try_recv(&mut mcp2, 400).await.is_none(),
        "result must not leak to other controllers"
    );
}

/// Peer model (iter75): roles are gone — an *agent* peer can also initiate a
/// command and receive the result routed back to it, just like a controller.
#[tokio::test]
async fn agent_peer_can_initiate_command() {
    let port = start_relay().await;

    // Executor agent.
    let mut a1 = connect(port, "dev").await;
    auth(&mut a1, ClientRole::Agent, Some(agent_info("a1", &[]))).await;
    match recv(&mut a1).await {
        ServerMessage::AgentList { .. } => {}
        other => panic!("expected AgentList, got {:?}", other),
    }

    // Initiator agent (a second peer — NOT a controller).
    let mut a2 = connect(port, "dev").await;
    auth(&mut a2, ClientRole::Agent, Some(agent_info("a2", &[]))).await;
    match recv(&mut a2).await {
        ServerMessage::AgentList { agents } => assert_eq!(agents.len(), 1), // sees a1
        other => panic!("expected AgentList, got {:?}", other),
    }
    // a1 is told a2 joined; drain it.
    match recv(&mut a1).await {
        ServerMessage::AgentJoined { .. } => {}
        other => panic!("expected AgentJoined at a1, got {:?}", other),
    }

    // a2 commands a1 (an agent issuing a command — previously forbidden).
    send(
        &mut a2,
        &ClientMessage::Command {
            request_id: "p2p".into(),
            target: Target::Agent { id: "a1".into() },
            payload: "P".into(),
        },
    )
    .await;
    match recv(&mut a1).await {
        ServerMessage::Command { request_id, .. } => assert_eq!(request_id, "p2p"),
        other => panic!("expected Command at a1, got {:?}", other),
    }
    send(
        &mut a1,
        &ClientMessage::CommandResult {
            request_id: "p2p".into(),
            result: "R".into(),
        },
    )
    .await;

    // The result routes back to the initiating agent a2.
    match recv(&mut a2).await {
        ServerMessage::CommandResult { request_id, result, .. } => {
            assert_eq!(request_id, "p2p");
            assert_eq!(result, "R");
        }
        other => panic!("expected CommandResult at a2, got {:?}", other),
    }
}
