//! End-to-end test: spin the relay on an ephemeral port and drive a fake agent
//! and MCP client through auth, agent listing, command routing, and a push
//! event — all over the real WebSocket protocol.

use futures::{SinkExt, StreamExt};
use remote_agents_relay::{router, state::RelayState};
use remote_agents_shared::{
    AgentEvent, AgentInfo, AgentMode, ClientMessage, Endpoint, ServerMessage, Target,
    TaskStatus, UdpOffer,
};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn start_relay() -> u16 {
    start_relay_with(Arc::new(RelayState::new(None))).await
}

async fn start_relay_with(state: Arc<RelayState>) -> u16 {
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
    ws.send(Message::Binary(msg.to_proto_bytes().unwrap())).await.unwrap();
}

async fn recv(ws: &mut Ws) -> ServerMessage {
    loop {
        match tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("recv timeout")
        {
            Some(Ok(Message::Binary(b))) => {
                let msg = ServerMessage::from_proto_bytes(&b).unwrap();
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
            Ok(Some(Ok(Message::Binary(b)))) => {
                let msg = ServerMessage::from_proto_bytes(&b).unwrap();
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
        version: String::new(), update_available: None, connections: None,
    }
}

async fn auth(ws: &mut Ws, info: Option<AgentInfo>) -> String {
    send(
        ws,
        &ClientMessage::Auth {
            room: "dev".into(),
            token: "secret".into(),
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
    auth(&mut agent, Some(agent_info("a1", &["backend"]))).await;
    // On joining the (empty) room the agent receives its initial peer list.
    match recv(&mut agent).await {
        ServerMessage::AgentList { agents } => assert!(agents.is_empty()),
        other => panic!("expected initial agent_list, got {:?}", other),
    }

    // MCP connects.
    let mut mcp = connect(port, "dev").await;
    auth(&mut mcp, None).await;

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
            payload: "QUJDREVG".into(),
        },
    )
    .await;
    match recv(&mut agent).await {
        ServerMessage::Command {
            request_id, payload, ..
        } => {
            assert_eq!(request_id, "req-1");
            assert_eq!(payload, "QUJDREVG"); // forwarded opaquely
        }
        other => panic!("expected command, got {:?}", other),
    }

    // Agent replies; MCP receives the result tagged with the agent id.
    send(
        &mut agent,
        &ClientMessage::CommandResult {
            request_id: "req-1".into(),
            result: "UkVTVUxU".into(),
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
            assert_eq!(result, "UkVTVUxU");
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
            payload: "WA==".into(),
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
    auth(&mut a1, Some(agent_info("a1", &["backend"]))).await;
    match recv(&mut a1).await {
        ServerMessage::AgentList { agents } => assert!(agents.is_empty()),
        other => panic!("expected empty agent_list, got {:?}", other),
    }

    // Second agent joins → its initial list already contains a1.
    let mut a2 = connect(port, "dev").await;
    auth(&mut a2, Some(agent_info("a2", &["frontend"]))).await;
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

    // After an agent joins it shows up in the room info, with its connection count.
    let mut agent = connect(port, "dev").await;
    auth(&mut agent, Some(agent_info("a1", &["backend"]))).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (_status, body) = http_get(port, "/api/room/dev").await;
    assert!(body.contains("\"id\":\"a1\""), "body: {body}");
    assert!(body.contains("\"connections\":1"), "body: {body}");

    // A second socket of the SAME machine collapses to one entry (matching the
    // worker's deduped /info) and the count rises to 2.
    let mut agent_dup = connect(port, "dev").await;
    auth(&mut agent_dup, Some(agent_info("a1", &["backend"]))).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (_status, body) = http_get(port, "/api/room/dev").await;
    assert_eq!(body.matches("\"id\":\"a1\"").count(), 1, "deduped to one entry: {body}");
    assert!(body.contains("\"connections\":2"), "body: {body}");
}

/// The relay reflects the client's observed IP via `YourEndpoint` (so peers can
/// build a reachable UDP endpoint for hole-punching).
#[tokio::test]
async fn relay_reflects_your_endpoint() {
    let port = start_relay().await;
    let mut agent = connect(port, "dev").await;
    auth(&mut agent, Some(agent_info("a1", &[]))).await;

    // Read raw frames (don't use recv(), which filters YourEndpoint out).
    let mut saw = None;
    for _ in 0..5 {
        if let Some(Ok(Message::Binary(b))) = tokio::time::timeout(Duration::from_secs(2), agent.next())
            .await
            .expect("timeout")
        {
            if let ServerMessage::YourEndpoint { endpoint } = ServerMessage::from_proto_bytes(&b).unwrap() {
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
        local_candidates: Vec::new(),
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
    auth(&mut agent, Some(agent_info("a", &[]))).await;
    assert!(matches!(recv(&mut agent).await, ServerMessage::AgentList { .. })); // initial peers

    send(
        &mut agent,
        &ClientMessage::Command {
            request_id: "x".into(),
            target: Target::Tagged { tags: vec!["nobody".into()] },
            payload: "UA==".into(),
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
    let a_sess = auth(&mut a, Some(agent_info("a", &[]))).await;
    assert!(matches!(recv(&mut a).await, ServerMessage::AgentList { .. }));

    let mut b = connect(port, "dev").await;
    let b_sess = auth(&mut b, Some(agent_info("b", &[]))).await;
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
    auth(&mut agent, Some(agent_info("a1", &[]))).await;
    match recv(&mut agent).await {
        ServerMessage::AgentList { .. } => {}
        other => panic!("expected AgentList, got {:?}", other),
    }

    // Two controllers in the same room.
    let mut mcp1 = connect(port, "dev").await;
    auth(&mut mcp1, None).await;
    let mut mcp2 = connect(port, "dev").await;
    auth(&mut mcp2, None).await;

    // mcp1 issues the command.
    send(
        &mut mcp1,
        &ClientMessage::Command {
            request_id: "req-x".into(),
            target: Target::Agent { id: "a1".into() },
            payload: "UA==".into(),
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
            result: "Ug==".into(),
        },
    )
    .await;

    // The initiator (mcp1) receives the result...
    match recv(&mut mcp1).await {
        ServerMessage::CommandResult { request_id, result, .. } => {
            assert_eq!(request_id, "req-x");
            assert_eq!(result, "Ug==");
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
    auth(&mut a1, Some(agent_info("a1", &[]))).await;
    match recv(&mut a1).await {
        ServerMessage::AgentList { .. } => {}
        other => panic!("expected AgentList, got {:?}", other),
    }

    // Initiator agent (a second peer — NOT a controller).
    let mut a2 = connect(port, "dev").await;
    auth(&mut a2, Some(agent_info("a2", &[]))).await;
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
            payload: "UA==".into(),
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
            result: "Ug==".into(),
        },
    )
    .await;

    // The result routes back to the initiating agent a2.
    match recv(&mut a2).await {
        ServerMessage::CommandResult { request_id, result, .. } => {
            assert_eq!(request_id, "p2p");
            assert_eq!(result, "Ug==");
        }
        other => panic!("expected CommandResult at a2, got {:?}", other),
    }
}

/// Peer-model registration (iter80): an identified peer — even a send-only one —
/// is visible in list_agents; an anonymous connection (no agent_info, e.g. a
/// browser stats client) is NOT listed but can still observe and command. A
/// broadcast skips the send-only peer.
#[tokio::test]
async fn peer_visibility_and_anonymous_observer() {
    let port = start_relay().await;

    // Full peer A (executes).
    let mut a = connect(port, "dev").await;
    auth(&mut a, Some(agent_info("a", &[]))).await;
    assert!(matches!(recv(&mut a).await, ServerMessage::AgentList { .. }));

    // Send-only peer B (--no-agent: visible, but never executes).
    let mut b_info = agent_info("b", &[]);
    b_info.accepts_commands = false;
    let mut b = connect(port, "dev").await;
    auth(&mut b, Some(b_info)).await;
    assert!(matches!(recv(&mut b).await, ServerMessage::AgentList { .. })); // sees A
    assert!(matches!(recv(&mut a).await, ServerMessage::AgentJoined { .. })); // A learns of B

    // Anonymous observer C (no agent_info).
    let mut c = connect(port, "dev").await;
    auth(&mut c, None).await;

    // C lists the room and sees BOTH peers (send-only B is still visible)...
    send(&mut c, &ClientMessage::ListAgents).await;
    match recv(&mut c).await {
        ServerMessage::AgentList { agents } => {
            let mut ids: Vec<String> = agents.iter().map(|a| a.id.clone()).collect();
            ids.sort();
            assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
        }
        other => panic!("expected AgentList, got {:?}", other),
    }

    // ...and a broadcast from C reaches the executing peer A but NOT send-only B.
    send(
        &mut c,
        &ClientMessage::Command {
            request_id: "bc".into(),
            target: Target::All,
            payload: "UA==".into(),
        },
    )
    .await;
    match recv(&mut a).await {
        ServerMessage::Command { request_id, .. } => assert_eq!(request_id, "bc"),
        other => panic!("expected Command at A, got {:?}", other),
    }
    assert!(
        try_recv(&mut b, 400).await.is_none(),
        "send-only peer must be skipped by broadcasts"
    );
}

/// A machine may hold several connections (many terminals on the same box) under
/// one agent-id; they collapse to ONE logical peer. When one of them closes, the
/// host stays present (and emits no false `AgentLeft`) as long as another
/// connection of the same id remains, and Agent-targeted commands keep reaching a
/// live connection. Regression for duplicate `ojo` sockets in the room.
#[tokio::test]
async fn duplicate_connection_disconnect_keeps_host_present() {
    let port = start_relay().await;

    // An observer watching join/leave traffic, connected first so it sees both
    // joins.
    let mut mcp = connect(port, "dev").await;
    auth(&mut mcp, None).await;

    // First connection for id "dup".
    let mut a1 = connect(port, "dev").await;
    auth(&mut a1, Some(agent_info("dup", &[]))).await;
    let _ = recv(&mut a1).await; // initial (empty) AgentList
    match recv(&mut mcp).await {
        ServerMessage::AgentJoined { agent } => assert_eq!(agent.id, "dup"),
        other => panic!("expected AgentJoined(dup), got {:?}", other),
    }

    // Second connection for the SAME id replaces the first in the room map.
    let mut a2 = connect(port, "dev").await;
    auth(&mut a2, Some(agent_info("dup", &[]))).await;
    let _ = recv(&mut a2).await; // its initial AgentList
    match recv(&mut mcp).await {
        ServerMessage::AgentJoined { agent } => assert_eq!(agent.id, "dup"),
        other => panic!("expected re-join AgentJoined(dup), got {:?}", other),
    }

    // The stale first socket goes away.
    a1.close(None).await.unwrap();
    drop(a1);

    // The observer must NOT receive an AgentLeft for "dup": the live socket a2
    // still owns the id.
    assert!(
        try_recv(&mut mcp, 400).await.is_none(),
        "stale disconnect must not announce a false agent_left"
    );

    // The peer is still listed, and a command targeting it reaches the live a2.
    send(&mut mcp, &ClientMessage::ListAgents).await;
    match recv(&mut mcp).await {
        ServerMessage::AgentList { agents } => {
            assert_eq!(agents.len(), 1, "exactly one live peer for the id");
            assert_eq!(agents[0].id, "dup");
        }
        other => panic!("expected AgentList, got {:?}", other),
    }
    send(
        &mut mcp,
        &ClientMessage::Command {
            request_id: "to-live".into(),
            target: Target::Agent { id: "dup".into() },
            payload: "UA==".into(),
        },
    )
    .await;
    match recv(&mut a2).await {
        ServerMessage::Command { request_id, .. } => assert_eq!(request_id, "to-live"),
        other => panic!("expected Command at live replacement, got {:?}", other),
    }
}

/// Two connections from the same machine with DIFFERENT capabilities (one
/// autonomous terminal, one not) collapse to a single host that is autonomous
/// (a capability is present if ANY connection has it), and an Agent-targeted
/// command is delivered to the AUTONOMOUS connection — not the plain one. This
/// is the exact `ojo` bug: a non-autonomous terminal answered "autonomous mode
/// is not enabled" because the relay fanned the command out to every duplicate.
#[tokio::test]
async fn duplicate_connections_merge_capabilities_and_route_to_capable() {
    let port = start_relay().await;

    let mut mcp = connect(port, "dev").await;
    auth(&mut mcp, None).await;

    // First connection: NOT autonomous (a terminal where the AI CLI isn't found).
    let mut plain = connect(port, "dev").await;
    let mut info_plain = agent_info("ojo", &[]);
    info_plain.autonomous = false;
    auth(&mut plain, Some(info_plain)).await;
    let _ = recv(&mut plain).await; // initial (empty) AgentList
    match recv(&mut mcp).await {
        ServerMessage::AgentJoined { agent } => assert_eq!(agent.id, "ojo"),
        other => panic!("expected AgentJoined(ojo), got {:?}", other),
    }

    // Second connection from the SAME machine: autonomous.
    let mut auto = connect(port, "dev").await;
    let mut info_auto = agent_info("ojo", &[]);
    info_auto.autonomous = true;
    auth(&mut auto, Some(info_auto)).await;
    let _ = recv(&mut auto).await; // its initial AgentList
    match recv(&mut mcp).await {
        ServerMessage::AgentJoined { agent } => {
            assert_eq!(agent.id, "ojo");
            assert!(agent.autonomous, "host is autonomous if any connection is");
        }
        other => panic!("expected merged AgentJoined, got {:?}", other),
    }

    // list_agents shows ONE host, autonomous.
    send(&mut mcp, &ClientMessage::ListAgents).await;
    match recv(&mut mcp).await {
        ServerMessage::AgentList { agents } => {
            assert_eq!(agents.len(), 1, "one logical host for the machine");
            assert_eq!(agents[0].id, "ojo");
            assert!(agents[0].autonomous);
        }
        other => panic!("expected AgentList, got {:?}", other),
    }

    // The Agent-targeted command must land on the AUTONOMOUS connection only.
    send(
        &mut mcp,
        &ClientMessage::Command {
            request_id: "to-auto".into(),
            target: Target::Agent { id: "ojo".into() },
            payload: "UA==".into(),
        },
    )
    .await;
    match recv(&mut auto).await {
        ServerMessage::Command { request_id, .. } => assert_eq!(request_id, "to-auto"),
        other => panic!("expected Command at autonomous connection, got {:?}", other),
    }
    assert!(
        try_recv(&mut plain, 400).await.is_none(),
        "non-autonomous connection must not receive the Agent-targeted command"
    );
}

// A connection that sends nothing past the idle window is reaped (its socket is
// closed by the server), so a silently-dead TCP can't linger as a phantom.
#[tokio::test]
async fn idle_connection_is_reaped() {
    let state = Arc::new(RelayState::new(None).with_idle_timeout(Duration::from_millis(300)));
    let port = start_relay_with(state).await;

    let mut agent = connect(port, "dev").await;
    auth(&mut agent, Some(agent_info("idle1", &[]))).await;

    // Send nothing. Within ~idle_timeout the server closes the socket; the
    // client stream then ends (None) or yields a Close frame.
    let reaped = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match agent.next().await {
                None | Some(Ok(Message::Close(_))) | Some(Err(_)) => break true,
                Some(Ok(_)) => continue, // ignore agent_list / your_endpoint etc.
            }
        }
    })
    .await
    .expect("connection was not reaped within 2s");
    assert!(reaped);
}

// A connection that keeps sending stays alive well past the idle window.
#[tokio::test]
async fn active_connection_survives_idle_window() {
    let state = Arc::new(RelayState::new(None).with_idle_timeout(Duration::from_millis(300)));
    let port = start_relay_with(state).await;

    let mut agent = connect(port, "dev").await;
    auth(&mut agent, Some(agent_info("live1", &[]))).await;
    // Consume the initial peer list the agent gets on joining the room.
    match recv(&mut agent).await {
        ServerMessage::AgentList { .. } => {}
        other => panic!("expected initial agent_list, got {:?}", other),
    }

    // Ping every 100ms across ~5 idle windows; the server must answer each pong
    // and never close us.
    for _ in 0..5 {
        send(&mut agent, &ClientMessage::Ping).await;
        match recv(&mut agent).await {
            ServerMessage::Pong => {}
            other => panic!("expected pong, got {:?}", other),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// `/api/rooms` enumerates every active room with its counts (the self-hosted
// relay can do this even though the Cloudflare worker stubs it out).
#[tokio::test]
async fn rooms_list_enumerates_active_rooms() {
    let port = start_relay().await;

    // No rooms yet.
    let (status, body) = http_get(port, "/api/rooms").await;
    assert_eq!(status, 200);
    assert!(body.contains("\"rooms\":[]"), "body: {body}");

    // Agents join two different rooms (room comes from the URL path).
    let mut a = connect(port, "dev").await;
    auth(&mut a, Some(agent_info("a1", &[]))).await;
    let mut b = connect(port, "prod").await;
    auth(&mut b, Some(agent_info("b1", &[]))).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (status, body) = http_get(port, "/api/rooms").await;
    assert_eq!(status, 200);
    assert!(body.contains("\"room\":\"dev\""), "body: {body}");
    assert!(body.contains("\"room\":\"prod\""), "body: {body}");
    assert!(body.contains("\"connections\":1"), "body: {body}");
    assert!(body.contains("\"agents\":1"), "body: {body}");

    // When the last connection of a room leaves, the room is GC'd and drops out.
    drop(a);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (_s, body) = http_get(port, "/api/rooms").await;
    assert!(!body.contains("\"room\":\"dev\""), "dev should be gone: {body}");
    assert!(body.contains("\"room\":\"prod\""), "prod should remain: {body}");
}

// An untrusted client cannot force the relay to buffer an oversized frame: a
// message past the relay's max WS message size (1 MiB) is refused and the
// connection is dropped, rather than being read into memory and parsed. Guards
// the per-connection memory-amplification surface (axum's 64 MiB default).
#[tokio::test]
async fn oversized_message_is_rejected_and_connection_dropped() {
    let port = start_relay().await;
    let mut ws = connect(port, "dev").await;

    // Authenticate normally first, so what follows is exercised on an
    // established session — isolating the SIZE check from the "first frame must
    // be auth" rule. (A small Ping here would be answered with a Pong.)
    auth(&mut ws, Some(agent_info("big", &[]))).await;

    // A ~2 MiB frame that is still a valid Ping: an appended unknown protobuf
    // field (#15, length-delimited) is skipped on decode, so absent the size cap
    // the server would parse it and reply Pong. With the cap, the frame is over
    // the 1 MiB limit and the connection is dropped instead. (The client's own
    // frame limit is larger, so it happily sends it.)
    let mut oversized = ClientMessage::Ping.to_proto_bytes().unwrap();
    oversized.push((15 << 3) | 2); // tag: field 15, wire type 2 (length-delimited)
    let pad = 2 * 1024 * 1024usize;
    let mut n = pad; // length as a protobuf varint
    loop {
        let mut byte = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            byte |= 0x80;
        }
        oversized.push(byte);
        if n == 0 {
            break;
        }
    }
    oversized.extend(std::iter::repeat_n(b'x', pad));
    ws.send(Message::Binary(oversized)).await.unwrap();

    // The server drops the connection: the client stream ends (None), yields a
    // Close, or errors. Crucially it must NOT answer the oversized Ping (which
    // is what an uncapped server would do) — a Pong here means the cap is gone.
    let dropped = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match ws.next().await {
                None | Some(Ok(Message::Close(_))) | Some(Err(_)) => break true,
                Some(Ok(Message::Binary(b))) => {
                    assert!(
                        !matches!(ServerMessage::from_proto_bytes(&b), Ok(ServerMessage::Pong)),
                        "server answered the oversized Ping — the size cap is not enforced"
                    );
                    continue; // ignore setup noise (agent_list / your_endpoint)
                }
                Some(Ok(_)) => continue,
            }
        }
    })
    .await
    .expect("connection was not dropped within 2s after an oversized frame");
    assert!(dropped, "oversized frame must drop the connection");
}

/// `UpdateAgent` (sent by an agent after a mode change, see the set_mode fix)
/// must NOT deadlock the relay. The handler held a `get_mut` write guard on the
/// session's DashMap shard and then called `dedup_agents`/`broadcast_agents`,
/// which iterate `room.agents` and re-lock every shard — including the held one,
/// self-deadlocking the task while it owned the lock and wedging the whole relay
/// (every later peer join / HTTP request parked at 0% CPU). Regression guard: an
/// observer must receive the broadcast that follows the update, and the relay
/// must keep accepting new connections afterwards.
#[tokio::test]
async fn update_agent_does_not_deadlock_relay() {
    let port = start_relay().await;

    // Observer connected first so it sees join + update traffic.
    let mut mcp = connect(port, "dev").await;
    auth(&mut mcp, None).await;

    // Agent joins; observer is told it joined.
    let mut agent = connect(port, "dev").await;
    auth(&mut agent, Some(agent_info("a1", &["backend"]))).await;
    let _ = recv(&mut agent).await; // initial (empty) AgentList
    match recv(&mut mcp).await {
        ServerMessage::AgentJoined { agent } => assert_eq!(agent.id, "a1"),
        other => panic!("expected AgentJoined(a1), got {:?}", other),
    }

    // The agent updates its info (the path that triggers `get_mut` + re-lock).
    let mut updated = agent_info("a1", &["frontend"]);
    updated.mode = AgentMode::Edit;
    send(
        &mut agent,
        &ClientMessage::UpdateAgent { agent_info: Box::new(updated) },
    )
    .await;

    // Under the bug this broadcast never fires (the handler deadlocks at
    // `dedup_agents` before reaching it) and this recv times out.
    match recv(&mut mcp).await {
        ServerMessage::AgentJoined { agent } => {
            assert_eq!(agent.id, "a1");
            assert!(agent.tags.contains(&"frontend".to_string()), "update applied");
        }
        other => panic!("expected AgentJoined update, got {:?}", other),
    }

    // And the relay is still alive: a fresh peer can join and the observer sees it.
    let mut agent2 = connect(port, "dev").await;
    auth(&mut agent2, Some(agent_info("a2", &[]))).await;
    let _ = recv(&mut agent2).await; // its initial AgentList
    match recv(&mut mcp).await {
        ServerMessage::AgentJoined { agent } => assert_eq!(agent.id, "a2"),
        other => panic!("relay wedged after UpdateAgent: {:?}", other),
    }
}

// Two sockets sharing one agent-id collapse to one host whose `connections`
// count is surfaced (so the panel can warn about duplicate/possibly-mis-keyed
// sockets). Mirrors the worker's dedupAgents behaviour.
#[tokio::test]
async fn list_agents_reports_connection_count_for_duplicate_sockets() {
    let port = start_relay().await;

    // Two live connections advertise the SAME agent id "dup".
    let mut dup1 = connect(port, "dev").await;
    auth(&mut dup1, Some(agent_info("dup", &[]))).await;
    let mut dup2 = connect(port, "dev").await;
    auth(&mut dup2, Some(agent_info("dup", &[]))).await;

    // An observer lists the room.
    let mut mcp = connect(port, "dev").await;
    auth(&mut mcp, None).await;
    send(&mut mcp, &ClientMessage::ListAgents).await;

    match recv(&mut mcp).await {
        ServerMessage::AgentList { agents } => {
            assert_eq!(agents.len(), 1, "two sockets of one id collapse to one host");
            assert_eq!(agents[0].id, "dup");
            assert_eq!(agents[0].connections, Some(2), "both sockets counted");
        }
        other => panic!("expected agent_list, got {:?}", other),
    }
}
