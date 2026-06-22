//! Throwaway driver to exercise `sync_dir` end-to-end against a live relay,
//! without needing an MCP client that already exposes the new tool.
//!
//! It joins a room as an anonymous send-only controller, issues a `SyncDirTo`
//! to a SOURCE agent (which runs the sync to the destination), then polls
//! `TransferGet` until the folder transfer finishes.
//!
//! Usage:
//!   sync_probe <relay> <room> <token> <src_id> <dest_id> <src_path> <dest_path> \
//!              [delete] [checksum] [dry_run]

use std::time::Duration;

use anyhow::{bail, Result};
use remote_agent::relay_api::McpServer;
use remote_agents_shared::{Command, CommandResult, Target, TransferState};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 8 {
        bail!("usage: sync_probe <relay> <room> <token> <src_id> <dest_id> <src_path> <dest_path> [delete] [checksum] [dry_run]");
    }
    let (relay, room, token) = (&a[1], &a[2], &a[3]);
    let (src_id, dest_id) = (a[4].clone(), a[5].clone());
    let (src_path, dest_path) = (a[6].clone(), a[7].clone());
    let flag = |i: usize| a.get(i).map(|s| s == "true").unwrap_or(false);
    let (delete, checksum, dry_run) = (flag(8), flag(9), flag(10));

    let api = McpServer::new();
    api.join_room(relay, room, token, None, None, None).await?;
    eprintln!("connected to {room} @ {relay}");
    for ag in api.list_agents(room).await? {
        eprintln!("  agent {} {} v{} mode={:?}", ag.id, ag.name, ag.version, ag.mode);
    }

    let res = api
        .send_command(
            room,
            Target::Agent { id: src_id.clone() },
            Command::SyncDirTo {
                src_path,
                dest_id,
                dest_path,
                delete,
                checksum,
                dry_run,
            },
        )
        .await?;
    let transfer_id = match res.into_iter().next() {
        Some((_, CommandResult::TransferQueued { id })) => id,
        Some((_, other)) => bail!("unexpected reply: {other:?}"),
        None => bail!("no reply from source agent"),
    };
    eprintln!("sync queued: transfer {transfer_id}");

    for _ in 0..120 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let res = api
            .send_command(
                room,
                Target::Agent { id: src_id.clone() },
                Command::TransferGet { id: transfer_id.clone() },
            )
            .await?;
        if let Some((_, CommandResult::Transfer { status })) = res.into_iter().next() {
            eprintln!(
                "  {:?}  files {}/{}  {} bytes{}",
                status.state,
                status.files_done,
                status.files_total,
                status.bytes,
                status.error.map(|e| format!("  ERROR: {e}")).unwrap_or_default(),
            );
            match status.state {
                TransferState::Done => {
                    eprintln!("DONE");
                    return Ok(());
                }
                TransferState::Failed => bail!("sync failed"),
                _ => {}
            }
        }
    }
    bail!("timed out waiting for sync")
}
