//! Regression: the MCP controller's FIRST dial to the relay used to be
//! one-shot. A relay that was down (or network-blocked, e.g. by a firewall
//! rule) at startup permanently disabled the remote tools for the whole
//! session — "local-only forever", which from the AI host's seat looks exactly
//! like a token/room mismatch. `join_room_with_retry` must keep re-dialing in
//! the background and recover as soon as the relay becomes reachable.
//!
//! Mirrors `transfer_e2e.rs`: a real in-process relay on a loopback port, so
//! the recovery path (auth frame → roster → connection pool) is exercised
//! end-to-end, not just the retry loop's bookkeeping.

use remote_agent::relay_api::McpServer;
use remote_agents_relay::{router, state::RelayState};
use remote_agents_shared::{AgentInfo, AgentMode};
use std::sync::Arc;
use std::time::Duration;

/// Start the real relay on a SPECIFIC port (transfer_e2e picks a free one;
/// here the port must be dead first, then come alive).
async fn start_relay_on(port: u16) {
    let state = Arc::new(RelayState::new(None));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("relay rebinds the reserved port");
    tokio::spawn(async move {
        axum::serve(
            listener,
            router(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
}

fn peer_info(id: &str) -> AgentInfo {
    AgentInfo {
        id: id.to_string(),
        name: id.to_string(),
        mode: AgentMode::Bypass,
        os: "linux".into(),
        arch: "x86_64".into(),
        hostname: id.into(),
        tags: vec![],
        platform: Default::default(),
        autonomous: false,
        accepts_commands: true,
        connected_at: 0,
        session_id: None,
        version: String::new(),
        update_available: None,
        connections: None,
    }
}

#[tokio::test]
async fn first_dial_failure_recovers_once_relay_appears() {
    // Reserve a port, then free it so the first dial hits connection-refused.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let relay_url = format!("ws://127.0.0.1:{port}");

    let ctrl = Arc::new(McpServer::new());

    // Startup dial against a dead port fails…
    let first = ctrl
        .join_room_with_retry(
            &relay_url,
            "dev",
            "secret",
            None,
            Some(Box::new(peer_info("ctrl"))),
            None,
        )
        .await;
    assert!(first.is_err(), "dial against a dead port must fail");

    // …but remote control is not dead: the tools report "not connected"
    // instead of the room having been abandoned entirely.
    assert!(
        ctrl.list_agents("dev").await.is_err(),
        "pre-recovery list_agents must surface a clean not-connected error"
    );

    // The relay comes up on that port…
    start_relay_on(port).await;

    // …and the background retry lands: the roster endpoint starts answering
    // (the controller itself is a peer, so the roster contains it).
    let mut recovered = None;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(agents) = ctrl.list_agents("dev").await {
            if agents.iter().any(|a| a.id == "ctrl") {
                recovered = Some(agents.len());
                break;
            }
        }
    }
    let n = recovered.expect("background retry must connect once the relay is reachable");
    assert!(n >= 1, "the controller peer must see itself in the roster");
}

#[tokio::test]
async fn first_dial_success_needs_no_retry() {
    // Happy path: a reachable relay connects on the inline dial, and the
    // method returns the pool's connect message (no background task spawned).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    start_relay_on(port).await;

    let ctrl = Arc::new(McpServer::new());
    let msg = ctrl
        .join_room_with_retry(
            &format!("ws://127.0.0.1:{port}"),
            "dev",
            "secret",
            None,
            Some(Box::new(peer_info("ctrl"))),
            None,
        )
        .await
        .expect("inline dial against a live relay connects");
    assert!(msg.contains("Connected"), "unexpected connect message: {msg}");
    assert!(
        ctrl.list_agents("dev")
            .await
            .is_ok_and(|agents| agents.iter().any(|a| a.id == "ctrl")),
        "roster must be immediately available"
    );
}
