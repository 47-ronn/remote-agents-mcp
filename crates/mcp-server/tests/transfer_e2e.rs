//! End-to-end host↔host file transfer over a real in-process relay plus two
//! agent loops. Exercises the transport plumbing that unit tests can't reach:
//! the run-mode `SendFileTo` intercept, `send_peer_command`'s pending-map and
//! reply routing (WS/UDP), `stream_file`, and the receiver's `FileRecv` write —
//! end-to-end, asserting the destination file matches the source byte-for-byte.

use remote_agent::config::Config;
use remote_agent::{connection, relay_api::McpServer};
use remote_agents_relay::{router, state::RelayState};
use remote_agents_shared::{AgentMode, Command, CommandResult, Target, TransferState};
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
