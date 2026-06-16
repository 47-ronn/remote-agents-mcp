//! Live round-trip e2e: join a relay room as an MCP controller, find a connected
//! agent, switch it to Bypass, run a shell command, and print the decrypted
//! result. Exercises the full real path including AES-GCM-256 E2E encryption —
//! something the unit/integration tests only cover in pieces.
//!
//! Usage:
//!   cargo run --example live_roundtrip -- [relay_url] [room] [token] [command]
//! Defaults target the test worker / e2e-live room. Requires at least one agent
//! already connected to that room.

use anyhow::{bail, Result};
use remote_agent::relay_api::McpServer;
use remote_agents_shared::{AgentMode, CommandResult, Target};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let relay = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "ws://127.0.0.1:8080".into());
    let room = args.get(2).cloned().unwrap_or_else(|| "e2e-live".into());
    let token = args.get(3).cloned().unwrap_or_else(|| "e2etok".into());
    let command = args
        .get(4)
        .cloned()
        .unwrap_or_else(|| "echo roundtrip-ok from $(hostname)".into());

    let mcp = McpServer::new();
    let session = mcp.join_room(&relay, &room, &token, None, None).await?;
    println!("joined room '{room}' as session {session}");

    // Give the relay a moment to deliver the agent list.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let agents = mcp.list_agents(&room).await?;
    if agents.is_empty() {
        bail!("no agents connected to room '{room}'");
    }
    let agent = &agents[0];
    println!(
        "targeting agent {} ({}) os={} distro={}",
        agent.name,
        agent.id,
        agent.os,
        agent.platform.distro.as_deref().unwrap_or("(unknown)")
    );

    // Bypass so an arbitrary command is allowed, then run it.
    mcp.set_mode(&room, &agent.id, AgentMode::Bypass).await?;
    let results = mcp
        .exec(
            &room,
            Target::Agent { id: agent.id.clone() },
            &command,
            Some(10_000),
        )
        .await?;

    for (id, res) in results {
        match res {
            CommandResult::Exec {
                stdout,
                stderr,
                exit_code,
            } => {
                print!("[{id}] exit={exit_code}\n--- stdout ---\n{stdout}");
                if !stderr.is_empty() {
                    print!("--- stderr ---\n{stderr}");
                }
            }
            other => println!("[{id}] {other:?}"),
        }
    }
    Ok(())
}
