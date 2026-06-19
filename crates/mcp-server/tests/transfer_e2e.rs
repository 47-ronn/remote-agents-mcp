//! End-to-end host↔host file transfer over a real in-process relay plus two
//! agent loops. Exercises the transport plumbing that unit tests can't reach:
//! the run-mode `SendFileTo` intercept, `send_peer_command`'s pending-map and
//! reply routing (WS/UDP), `stream_file`, and the receiver's `FileRecv` write —
//! end-to-end, asserting the destination file matches the source byte-for-byte.

use remote_agent::config::Config;
use remote_agent::state::AgentState;
use remote_agent::{connection, relay_api::McpServer};
use remote_agents_relay::{router, state::RelayState};
use remote_agents_shared::{AgentInfo, AgentMode, Command, CommandResult, Target, TransferState};
use std::sync::Arc;
use std::time::Duration;

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
    tokio::time::sleep(Duration::from_millis(50)).await;
    port
}

fn agent_config(id: &str, port: u16) -> Config {
    Config {
        id: id.to_string(),
        name: id.to_string(),
        room: "dev".to_string(),
        token: "secret".to_string(),
        relay_url: format!("ws://127.0.0.1:{port}"),
        ..Default::default()
    }
}

fn agent_info(id: &str) -> AgentInfo {
    AgentInfo {
        id: id.to_string(),
        name: id.to_string(),
        mode: AgentMode::Bypass,
        os: "linux".into(),
        arch: "x86_64".into(),
        hostname: id.to_string(),
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
async fn host_to_host_transfer_round_trips_over_relay() {
    let port = start_relay().await;
    let relay_url = format!("ws://127.0.0.1:{port}");

    // Source agent uses a small chunk so the streaming loop runs several slices.
    let mut src_cfg = agent_config("src-agent", port);
    src_cfg.security.transfer_chunk_size = 1000;
    let dst_cfg = agent_config("dst-agent", port);

    // Two real agent loops (run mode).
    tokio::spawn(async move {
        let _ = connection::run(&src_cfg).await;
    });
    tokio::spawn(async move {
        let _ = connection::run(&dst_cfg).await;
    });

    // Anonymous controller drives the transfer.
    let ctrl = McpServer::new();
    ctrl.join_room(&relay_url, "dev", "secret", None, None, None)
        .await
        .expect("controller joins room");

    // Wait until both agents have registered.
    let mut ready = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(agents) = ctrl.list_agents("dev").await {
            let has = |id: &str| agents.iter().any(|a| a.id == id);
            if has("src-agent") && has("dst-agent") {
                ready = true;
                break;
            }
        }
    }
    assert!(ready, "both agents should register with the relay");

    // The destination must allow writes to receive the file.
    ctrl.set_mode("dev", "dst-agent", AgentMode::Bypass)
        .await
        .expect("set dst to bypass");

    // Real source file on disk (the src agent reads its own filesystem).
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("payload.bin");
    let dst_path = dir.path().join("received.bin");
    // ~4 KiB of non-UTF8 data across several 1000-byte chunks.
    let data: Vec<u8> = (0u8..=255).cycle().take(4000).collect();
    std::fs::write(&src_path, &data).unwrap();

    // Kick off the transfer; the source returns a transfer id immediately.
    let res = ctrl
        .send_command(
            "dev",
            Target::Agent { id: "src-agent".into() },
            Command::SendFileTo {
                src_path: src_path.to_string_lossy().to_string(),
                dest_id: "dst-agent".to_string(),
                dest_path: dst_path.to_string_lossy().to_string(),
            },
        )
        .await
        .expect("dispatch SendFileTo");
    let id = match res.into_iter().next() {
        Some((_, CommandResult::TransferQueued { id })) => id,
        other => panic!("expected TransferQueued, got {other:?}"),
    };

    // Poll the source's transfer registry until it reports completion.
    let mut done = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let r = ctrl
            .send_command(
                "dev",
                Target::Agent { id: "src-agent".into() },
                Command::TransferGet { id: id.clone() },
            )
            .await;
        if let Ok(v) = r {
            if let Some((_, CommandResult::Transfer { status })) = v.into_iter().next() {
                match status.state {
                    TransferState::Done => {
                        done = true;
                        break;
                    }
                    TransferState::Failed => panic!("transfer failed: {:?}", status.error),
                    _ => {}
                }
            }
        }
    }
    assert!(done, "transfer should reach Done state");

    // The received file must be byte-identical to the source.
    assert_eq!(
        std::fs::read(&dst_path).unwrap(),
        data,
        "destination file must match the source"
    );
}

/// iter141: `send_file` with no `agent_id` routes to the node's OWN id, so the
/// local host streams its own filesystem to a remote agent. This exercises the
/// loopback path the fix produces: a full peer node (executor attached, exactly
/// like `run_mcp_server` builds it) issues `SendFileTo` to its own id; the relay
/// delivers `Target::Agent{self}` back to the same socket without excluding the
/// sender, and the peer's `begin_send_file` handler streams to the destination.
#[tokio::test]
async fn local_host_to_agent_loopback_round_trips_over_relay() {
    let port = start_relay().await;
    let relay_url = format!("ws://127.0.0.1:{port}");

    // Destination is an ordinary agent loop.
    let dst_cfg = agent_config("dst-agent", port);
    tokio::spawn(async move {
        let _ = connection::run(&dst_cfg).await;
    });

    // The "local host": a full peer with an attached executor, mirroring
    // run_mcp_server (config.accepts_commands → Some(state.clone())). It holds
    // the source file and will send to its OWN id. A small chunk forces the
    // streaming loop to run several slices. The relay and the local TransferGet
    // share one Arc<TransferStore>, so progress is visible either way.
    let mut host_cfg = agent_config("host-agent", port);
    host_cfg.security.transfer_chunk_size = 1000;
    let host_state = Arc::new(AgentState::new(host_cfg));
    let host = McpServer::new();
    host.join_room(
        &relay_url,
        "dev",
        "secret",
        None,
        Some(Box::new(agent_info("host-agent"))),
        Some(host_state),
    )
    .await
    .expect("host joins room as a full peer");

    // Wait until both the host peer and the destination have registered.
    let mut ready = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(agents) = host.list_agents("dev").await {
            let has = |id: &str| agents.iter().any(|a| a.id == id);
            if has("host-agent") && has("dst-agent") {
                ready = true;
                break;
            }
        }
    }
    assert!(ready, "host peer and destination should register");

    // The destination must allow writes to receive the file.
    host.set_mode("dev", "dst-agent", AgentMode::Bypass)
        .await
        .expect("set dst to bypass");

    // Source file on the host's own filesystem.
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("payload.bin");
    let dst_path = dir.path().join("received.bin");
    let data: Vec<u8> = (0u8..=255).cycle().take(4000).collect();
    std::fs::write(&src_path, &data).unwrap();

    // Send to our OWN id — the loopback the iter141 fix produces.
    let res = host
        .send_command(
            "dev",
            Target::Agent { id: "host-agent".into() },
            Command::SendFileTo {
                src_path: src_path.to_string_lossy().to_string(),
                dest_id: "dst-agent".to_string(),
                dest_path: dst_path.to_string_lossy().to_string(),
            },
        )
        .await
        .expect("dispatch SendFileTo to own id");
    let id = match res.into_iter().next() {
        Some((_, CommandResult::TransferQueued { id })) => id,
        other => panic!("expected TransferQueued, got {other:?}"),
    };

    // Poll the host's own transfer registry until it reports completion.
    let mut done = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let r = host
            .send_command(
                "dev",
                Target::Agent { id: "host-agent".into() },
                Command::TransferGet { id: id.clone() },
            )
            .await;
        if let Ok(v) = r {
            if let Some((_, CommandResult::Transfer { status })) = v.into_iter().next() {
                match status.state {
                    TransferState::Done => {
                        done = true;
                        break;
                    }
                    TransferState::Failed => panic!("transfer failed: {:?}", status.error),
                    _ => {}
                }
            }
        }
    }
    assert!(done, "loopback transfer should reach Done state");

    // The received file must be byte-identical to the source.
    assert_eq!(
        std::fs::read(&dst_path).unwrap(),
        data,
        "destination file must match the source"
    );
}
