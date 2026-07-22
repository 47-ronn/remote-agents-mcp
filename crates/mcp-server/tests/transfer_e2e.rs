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
    // Hermetic: never hit external STUN servers (their latency under CI made this
    // test flaky/time out). Endpoints fall back to the relay-reflected loopback.
    std::env::set_var("REMOTE_AGENTS_NO_STUN", "1");
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

/// A `run`-agent SOURCES a folder sync: `SyncDirTo` dispatched to a run-mode
/// agent must walk its own tree, query the destination manifest, and stream only
/// the changed files — the agent-side `begin_sync_dir`/`run_sync_agent` path
/// (previously only the controller could source a sync). Asserts every file
/// lands at the destination byte-for-byte, across a nested subdirectory.
#[tokio::test]
async fn host_to_host_sync_dir_round_trips_over_relay() {
    std::env::set_var("REMOTE_AGENTS_NO_STUN", "1"); // hermetic: no live STUN
    let port = start_relay().await;
    let relay_url = format!("ws://127.0.0.1:{port}");

    let mut src_cfg = agent_config("src-agent", port);
    src_cfg.security.transfer_chunk_size = 1000; // several slices per file
    let dst_cfg = agent_config("dst-agent", port);

    tokio::spawn(async move {
        let _ = connection::run(&src_cfg).await;
    });
    tokio::spawn(async move {
        let _ = connection::run(&dst_cfg).await;
    });

    let ctrl = McpServer::new();
    ctrl.join_room(&relay_url, "dev", "secret", None, None, None)
        .await
        .expect("controller joins room");

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

    ctrl.set_mode("dev", "dst-agent", AgentMode::Bypass)
        .await
        .expect("set dst to bypass");

    // Source tree on the src agent's filesystem: a file at the root and one in a
    // nested subdirectory, with non-UTF8 bytes spanning several chunks.
    let dir = tempfile::tempdir().unwrap();
    let src_root = dir.path().join("src");
    let dst_root = dir.path().join("dst");
    std::fs::create_dir_all(src_root.join("sub")).unwrap();
    let a: Vec<u8> = (0u8..=255).cycle().take(3500).collect();
    let b: Vec<u8> = (0u8..=255).rev().cycle().take(2200).collect();
    std::fs::write(src_root.join("a.bin"), &a).unwrap();
    std::fs::write(src_root.join("sub").join("b.bin"), &b).unwrap();

    let res = ctrl
        .send_command(
            "dev",
            Target::Agent { id: "src-agent".into() },
            Command::SyncDirTo {
                src_path: src_root.to_string_lossy().to_string(),
                dest_id: "dst-agent".to_string(),
                dest_path: dst_root.to_string_lossy().to_string(),
                delete: false,
                checksum: false,
                dry_run: false,
                exclude: vec![],
            },
        )
        .await
        .expect("dispatch SyncDirTo");
    let id = match res.into_iter().next() {
        Some((_, CommandResult::TransferQueued { id })) => id,
        other => panic!("expected TransferQueued, got {other:?}"),
    };

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
                    TransferState::Failed => panic!("sync failed: {:?}", status.error),
                    _ => {}
                }
            }
        }
    }
    assert!(done, "folder sync should reach Done state");

    assert_eq!(std::fs::read(dst_root.join("a.bin")).unwrap(), a, "root file must match");
    assert_eq!(
        std::fs::read(dst_root.join("sub").join("b.bin")).unwrap(),
        b,
        "nested file must match"
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
    std::env::set_var("REMOTE_AGENTS_NO_STUN", "1"); // hermetic: no live STUN
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

/// A machine with several MCP clients open joins the relay N times under ONE
/// agent id (`list_agents` reports `connections: N`). A locally-sourced transfer
/// must start in the process that asked for it.
///
/// Regression: the local `send_file`/`sync_dir` path used to dispatch to our own
/// agent id over the relay, and `Target::Agent` delivers to exactly one socket
/// chosen by capability score — so the transfer could start in a *sibling*
/// process, registered in that process's `TransferStore`. The id came back to a
/// caller that had never heard of it: `transfer_get` answered "no such transfer"
/// while the sync ran, or failed, somewhere unobservable. `source_transfer` keeps
/// the transfer and its id in the same process.
#[tokio::test]
async fn local_source_registers_the_transfer_in_this_process() {
    std::env::set_var("REMOTE_AGENTS_NO_STUN", "1"); // hermetic: no live STUN
    let port = start_relay().await;
    let relay_url = format!("ws://127.0.0.1:{port}");

    let dst_cfg = agent_config("dst-agent", port);
    tokio::spawn(async move {
        let _ = connection::run(&dst_cfg).await;
    });

    // Two peers of the SAME machine: same agent id, separate AgentStates —
    // exactly what two Claude Code sessions on one box look like to the relay.
    let mut cfg = agent_config("host-agent", port);
    cfg.security.transfer_chunk_size = 1000;
    let state_a = Arc::new(AgentState::new(cfg.clone()));
    let state_b = Arc::new(AgentState::new(cfg));
    let peer_a = McpServer::new();
    let peer_b = McpServer::new();
    for (peer, state) in [(&peer_a, &state_a), (&peer_b, &state_b)] {
        peer.join_room(
            &relay_url,
            "dev",
            "secret",
            None,
            Some(Box::new(agent_info("host-agent"))),
            Some(state.clone()),
        )
        .await
        .expect("peer joins room");
    }

    let mut ready = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(agents) = peer_a.list_agents("dev").await {
            let has = |id: &str| agents.iter().any(|a| a.id == id);
            if has("host-agent") && has("dst-agent") {
                ready = true;
                break;
            }
        }
    }
    assert!(ready, "both peers and the destination should register");

    peer_a
        .set_mode("dev", "dst-agent", AgentMode::Bypass)
        .await
        .expect("set dst to bypass");

    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("payload.bin");
    let dst_path = dir.path().join("received.bin");
    let data: Vec<u8> = (0u8..=255).cycle().take(4000).collect();
    std::fs::write(&src_path, &data).unwrap();

    // Source it on peer A — the local `send_file` path with no agent_id.
    let id = match peer_a
        .source_transfer(
            "dev",
            Command::SendFileTo {
                src_path: src_path.to_string_lossy().to_string(),
                dest_id: "dst-agent".to_string(),
                dest_path: dst_path.to_string_lossy().to_string(),
            },
        )
        .await
        .expect("source the transfer locally")
    {
        CommandResult::TransferQueued { id } => id,
        other => panic!("expected TransferQueued, got {other:?}"),
    };

    // The returned id must be poll-able right here — this is what broke.
    assert!(
        state_a.transfers().get(&id).is_some(),
        "the caller's own store must know the id it was handed"
    );
    assert!(
        state_b.transfers().get(&id).is_none(),
        "the sibling process must not have been handed the transfer"
    );

    let mut done = false;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        match state_a.transfers().get(&id).expect("transfer stays in this store").state {
            TransferState::Done => {
                done = true;
                break;
            }
            TransferState::Failed => {
                panic!("transfer failed: {:?}", state_a.transfers().get(&id).unwrap().error)
            }
            _ => {}
        }
    }
    assert!(done, "locally-sourced transfer should reach Done state");
    assert_eq!(
        std::fs::read(&dst_path).unwrap(),
        data,
        "destination file must match the source"
    );
}

/// A peer that holds the relay connection open but runs no executor is exactly
/// what a wedged agent looks like from the outside: present in the roster,
/// `accepts_commands: true`, and silent. It stands in for the real failure here —
/// an agent whose command loop hangs while its WebSocket stays up.
async fn join_zombie(relay_url: &str, id: &str) {
    let peer = McpServer::new();
    peer.join_room(relay_url, "dev", "secret", None, Some(Box::new(agent_info(id))), None)
        .await
        .expect("zombie peer joins room");
    // Keep it in the room for the life of the test.
    Box::leak(Box::new(peer));
}

async fn wait_for_agents(peer: &McpServer, ids: &[&str]) {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(agents) = peer.list_agents("dev").await {
            if ids.iter().all(|id| agents.iter().any(|a| a.id == *id)) {
                return;
            }
        }
    }
    panic!("agents {ids:?} should register");
}

/// Regression: syncing to an agent that is connected but not answering must fail
/// synchronously with a diagnosable error, not return a transfer id that then
/// sits in `Queued` for a full COMMAND_TIMEOUT. The silent-queue behaviour read
/// as "the sync finished instantly" to anyone who didn't poll for a minute.
#[tokio::test]
async fn sync_to_unresponsive_dest_fails_fast_instead_of_queueing() {
    std::env::set_var("REMOTE_AGENTS_NO_STUN", "1");
    let port = start_relay().await;
    let relay_url = format!("ws://127.0.0.1:{port}");

    join_zombie(&relay_url, "zombie-agent").await;

    let cfg = agent_config("host-agent", port);
    let state = Arc::new(AgentState::new(cfg));
    let peer = McpServer::new();
    peer.join_room(
        &relay_url,
        "dev",
        "secret",
        None,
        Some(Box::new(agent_info("host-agent"))),
        Some(state.clone()),
    )
    .await
    .expect("host peer joins room");

    wait_for_agents(&peer, &["host-agent", "zombie-agent"]).await;

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), b"payload").unwrap();

    let started = std::time::Instant::now();
    let result = peer
        .source_transfer(
            "dev",
            Command::SyncDirTo {
                src_path: dir.path().to_string_lossy().to_string(),
                dest_id: "zombie-agent".to_string(),
                dest_path: "/tmp/never-written".to_string(),
                delete: false,
                checksum: false,
                dry_run: false,
                exclude: vec![],
            },
        )
        .await;
    let elapsed = started.elapsed();

    let err = match result {
        Err(e) => e.to_string(),
        Ok(other) => panic!("expected a synchronous error, got {other:?}"),
    };
    assert!(
        err.contains("not responding"),
        "error should name the unresponsive destination, got: {err}"
    );
    assert!(
        elapsed < Duration::from_secs(30),
        "must fail on the probe budget, not the 60s command timeout (took {elapsed:?})"
    );
    assert!(
        !std::path::Path::new("/tmp/never-written").exists(),
        "nothing should have been written to the destination path"
    );
}

/// `list_agents` presence is not health: the probe must separate a live peer from
/// one that only holds its socket open.
#[tokio::test]
async fn probe_liveness_separates_live_peers_from_wedged_ones() {
    std::env::set_var("REMOTE_AGENTS_NO_STUN", "1");
    let port = start_relay().await;
    let relay_url = format!("ws://127.0.0.1:{port}");

    let dst_cfg = agent_config("live-agent", port);
    tokio::spawn(async move {
        let _ = connection::run(&dst_cfg).await;
    });
    join_zombie(&relay_url, "zombie-agent").await;

    let cfg = agent_config("host-agent", port);
    let state = Arc::new(AgentState::new(cfg));
    let peer = McpServer::new();
    peer.join_room(
        &relay_url,
        "dev",
        "secret",
        None,
        Some(Box::new(agent_info("host-agent"))),
        Some(state),
    )
    .await
    .expect("host peer joins room");

    wait_for_agents(&peer, &["live-agent", "zombie-agent"]).await;

    let alive = peer
        .probe_liveness("dev", &["live-agent".to_string(), "zombie-agent".to_string()])
        .await;

    assert_eq!(alive.get("live-agent"), Some(&true), "a running agent must answer the probe");
    assert_eq!(alive.get("zombie-agent"), Some(&false), "a wedged agent must not be reported alive");
}
