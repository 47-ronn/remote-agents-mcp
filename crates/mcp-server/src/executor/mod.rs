//! Command execution module.

mod filesystem;
mod git;
pub(crate) mod shell;

use crate::safety;
use crate::state::AgentState;
use anyhow::{bail, Result};
use remote_agents_shared::{AgentInfo, AgentMode, Command, CommandResult};
use std::path::Path;
use tracing::info;

/// Run a shell command with the default timeout, returning (stdout, stderr,
/// exit_code). Used by the scheduler for already-vetted commands.
pub async fn run_shell(command: &str) -> Result<(String, String, i32)> {
    let r = shell::exec(command, None, 300_000).await?;
    Ok((r.stdout, r.stderr, r.exit_code))
}

/// Execute a command against the current agent state and return the result.
pub async fn execute(cmd: &Command, state: &AgentState) -> Result<CommandResult> {
    let mode = state.mode().await;
    let sec = &state.config.security;

    // Send-only nodes (--no-agent: prod controllers, browser dashboards) are
    // visible peers that dispatch work but never run others' commands.
    if !state.config.accepts_commands {
        bail!("This node does not accept remote commands (--no-agent)");
    }

    if mode == AgentMode::Disabled {
        bail!("Agent is disabled");
    }

    match cmd {
        Command::Exec {
            command,
            timeout_ms,
            cwd,
        } => {
            info!("Executing: {}", command);
            safety::check_command_allowed(command, mode, sec)?;

            let timeout = timeout_ms.unwrap_or(sec.command_timeout * 1000);
            let result = shell::exec(command, cwd.as_deref(), timeout).await?;

            Ok(CommandResult::Exec {
                stdout: result.stdout,
                stderr: result.stderr,
                exit_code: result.exit_code,
                duration_ms: Some(result.duration_ms),
                timed_out: Some(result.timed_out),
            })
        }

        Command::ReadFile { path } => {
            info!("Reading file: {}", path);
            safety::check_path_allowed(path, sec)?;

            let metadata = tokio::fs::metadata(path).await?;
            safety::check_size(metadata.len(), sec)?;

            let content = tokio::fs::read_to_string(path).await?;
            let size = content.len() as u64;

            Ok(CommandResult::File { content, size })
        }

        Command::WriteFile {
            path,
            content,
            create_backup,
        } => {
            info!("Writing file: {}", path);

            if !mode.allows_write() {
                bail!("Write operations not allowed in {:?} mode", mode);
            }
            safety::check_path_allowed(path, sec)?;
            safety::check_size(content.len() as u64, sec)?;

            if *create_backup && sec.backup_enabled && Path::new(path).exists() {
                filesystem::create_backup(path, &sec.backup_dir, sec.max_backup_versions).await?;
            }

            tokio::fs::write(path, content).await?;

            Ok(CommandResult::Ok)
        }

        Command::ListDir { path, pattern } => {
            info!("Listing directory: {}", path);
            safety::check_path_allowed(path, sec)?;

            let entries = filesystem::list_directory(path, pattern.as_deref()).await?;

            Ok(CommandResult::Dir { entries })
        }

        Command::SetMode { mode: new_mode } => {
            info!("Switching mode {:?} -> {:?}", mode, new_mode);
            state.set_mode(*new_mode).await;
            Ok(CommandResult::Mode { mode: *new_mode })
        }

        Command::GetInfo => {
            let info = AgentInfo {
                id: state.config.id.clone(),
                name: state.config.name.clone(),
                mode,
                os: std::env::consts::OS.to_string(),
                arch: std::env::consts::ARCH.to_string(),
                hostname: hostname::get()
                    .map(|h| h.to_string_lossy().to_string())
                    .unwrap_or_else(|_| "unknown".to_string()),
                tags: state.config.tags.clone(),
                platform: remote_agents_shared::PlatformInfo::detect(),
                autonomous: state.autonomous().enabled(),
                accepts_commands: state.config.accepts_commands,
                connected_at: 0,
                version: env!("CARGO_PKG_VERSION").to_string(),
                session_id: None,
                update_available: crate::config::update_available(),
                connections: None,
            };

            Ok(CommandResult::Info { info })
        }

        Command::GitStatus { repo } => {
            info!("Git status: {}", repo);
            safety::check_path_allowed(repo, sec)?;
            let status = git::status(repo).await?;
            Ok(CommandResult::GitStatus { status })
        }

        Command::GitPull {
            repo,
            remote,
            branch,
        } => {
            info!("Git pull: {} {}", repo, remote);
            if !mode.allows_write() {
                bail!("Git pull not allowed in {:?} mode", mode);
            }
            safety::check_path_allowed(repo, sec)?;
            let (output, success) = git::pull(repo, remote, branch.as_deref()).await?;
            Ok(CommandResult::Git { output, success })
        }

        Command::GitCommit {
            repo,
            message,
            files,
        } => {
            info!("Git commit: {}", repo);
            if !mode.allows_write() {
                bail!("Git commit not allowed in {:?} mode", mode);
            }
            safety::check_path_allowed(repo, sec)?;
            let (output, success) = git::commit(repo, message, files).await?;
            Ok(CommandResult::Git { output, success })
        }

        Command::GitPush {
            repo,
            remote,
            branch,
        } => {
            info!("Git push: {} {}", repo, remote);
            if !mode.allows_write() {
                bail!("Git push not allowed in {:?} mode", mode);
            }
            safety::check_path_allowed(repo, sec)?;
            let (output, success) = git::push(repo, remote, branch.as_deref()).await?;
            Ok(CommandResult::Git { output, success })
        }

        Command::ScheduleAdd {
            name,
            cron,
            command,
        } => {
            info!("Schedule add: {} ({})", name, cron);
            // Scheduled commands run with the agent's current privileges; gate
            // the command under the active mode before accepting it.
            safety::check_command_allowed(command, mode, sec)?;
            state.scheduler().add(name, cron, command).await?;
            Ok(CommandResult::Ok)
        }

        Command::ScheduleRemove { name } => {
            info!("Schedule remove: {}", name);
            state.scheduler().remove(name).await?;
            Ok(CommandResult::Ok)
        }

        Command::ScheduleList => {
            let tasks = state.scheduler().list().await;
            Ok(CommandResult::Schedule { tasks })
        }

        Command::TaskDispatch { prompt, initiator } => {
            info!("Autonomous task dispatch ({} chars)", prompt.len());
            let store = state.autonomous();
            if !store.enabled() {
                bail!("autonomous mode is not enabled on this host");
            }
            let id = store.dispatch(prompt, initiator.clone())?;
            Ok(CommandResult::TaskQueued { id })
        }

        Command::TaskGet { id } => {
            let store = state.autonomous();
            match store.get(id)? {
                Some(task) => Ok(CommandResult::Task { task }),
                None => bail!("no autonomous task '{}'", id),
            }
        }

        Command::TaskList => {
            let tasks = state.autonomous().list()?;
            Ok(CommandResult::TaskList { tasks })
        }

        // === AI-provider sessions (claude / opencode history) ===
        Command::SessionList => {
            // Filesystem/CLI scans are blocking — keep them off the async runtime.
            let (sessions, active) = tokio::task::spawn_blocking(|| {
                (crate::sessions::list_sessions(), crate::sessions::active_sessions())
            })
            .await
            .map_err(|e| anyhow::anyhow!("session scan failed: {e}"))?;
            Ok(CommandResult::SessionList { sessions, active })
        }

        Command::SessionGet { provider, id } => {
            let (provider, id) = (provider.clone(), id.clone());
            let messages = tokio::task::spawn_blocking(move || {
                crate::sessions::get_transcript(&provider, &id)
            })
            .await
            .map_err(|e| anyhow::anyhow!("transcript fetch failed: {e}"))??;
            Ok(CommandResult::SessionTranscript { messages })
        }

        Command::SessionResume { provider, id, prompt } => {
            let store = state.autonomous();
            if !store.enabled() {
                bail!("autonomous mode is not enabled on this host");
            }
            let runner = crate::sessions::resume_runner(provider, id)?;
            // `claude --resume <id>` only finds the session within the project
            // that maps to its cwd, so resume from the session's recorded dir.
            let cwd = if provider == "claude" {
                crate::sessions::claude_session_cwd(id)
            } else {
                None
            };
            let new_id = store.dispatch_with_runner(prompt, None, Some(runner), cwd)?;
            Ok(CommandResult::TaskQueued { id: new_id })
        }

        Command::SessionTerminate { id } => {
            let id = id.clone();
            tokio::task::spawn_blocking(move || crate::sessions::terminate(&id))
                .await
                .map_err(|e| anyhow::anyhow!("terminate failed: {e}"))??;
            Ok(CommandResult::Ok)
        }

        // === File transfer: metadata / chunked read / thumbnail / search ===
        // All are blocking (fs / image decode / spawning find|grep) → off-runtime.
        Command::FileStat { path } => {
            let path = path.clone();
            let sec = state.config.security.clone();
            let meta = tokio::task::spawn_blocking(move || crate::files::stat(&path, &sec))
                .await
                .map_err(|e| anyhow::anyhow!("file stat failed: {e}"))??;
            Ok(CommandResult::FileMeta { meta })
        }

        Command::FileChunk { path, offset, len } => {
            let (path, offset, len) = (path.clone(), *offset, *len);
            let sec = state.config.security.clone();
            let (data, eof) =
                tokio::task::spawn_blocking(move || crate::files::read_chunk(&path, offset, len, &sec))
                    .await
                    .map_err(|e| anyhow::anyhow!("file chunk failed: {e}"))??;
            Ok(CommandResult::FileChunk { data, eof })
        }

        Command::FileThumb { path, max_px } => {
            let (path, max_px) = (path.clone(), *max_px);
            let sec = state.config.security.clone();
            let (data, w, h) =
                tokio::task::spawn_blocking(move || crate::files::thumbnail(&path, max_px, &sec))
                    .await
                    .map_err(|e| anyhow::anyhow!("thumbnail failed: {e}"))??;
            Ok(CommandResult::FileThumb { data, w, h })
        }

        Command::FileSearch { roots, query, kind } => {
            let (roots, query, kind) = (roots.clone(), query.clone(), *kind);
            let sec = state.config.security.clone();
            let hits =
                tokio::task::spawn_blocking(move || crate::files::search(&roots, &query, kind, &sec))
                    .await
                    .map_err(|e| anyhow::anyhow!("search failed: {e}"))??;
            Ok(CommandResult::FileSearch { hits })
        }

        // Receiver side of a host↔host transfer: write one slice to disk.
        Command::FileRecv {
            transfer_id: _,
            dest_path,
            offset,
            bytes,
            eof,
            sha256,
        } => {
            if !mode.allows_write() {
                bail!("Receiving a file requires Edit/Bypass mode (got {:?})", mode);
            }
            let (dest_path, bytes, sha) = (dest_path.clone(), bytes.clone(), sha256.clone());
            let (offset, eof) = (*offset, *eof);
            let sec = state.config.security.clone();
            tokio::task::spawn_blocking(move || {
                crate::transfer::receive_chunk(&dest_path, offset, &bytes, eof, sha.as_deref(), &sec)
            })
            .await
            .map_err(|e| anyhow::anyhow!("receive chunk failed: {e}"))??;
            Ok(CommandResult::Ok)
        }

        // Status of a host↔host transfer this node initiated.
        Command::TransferGet { id } => match state.transfers().get(id) {
            Some(status) => Ok(CommandResult::Transfer { status }),
            None => bail!("no such transfer: {id}"),
        },

        // SendFileTo is intercepted by the relay handler (it needs the
        // connection's peer-send primitives); reaching here means it was issued
        // on a path without an outbound connection (e.g. the local MCP tool).
        Command::SendFileTo { .. } => {
            bail!("send_file must be issued to a connected peer node")
        }

        // === Cloudflare quick tunnels (dev: expose a local port publicly) ===
        // Starting/stopping exposes a local service to the internet, so require
        // a write-capable mode. Listing is read-only. The work (download +
        // spawn + wait for URL) is blocking → run it off the async runtime.
        Command::TunnelStart { target } => {
            if !mode.allows_write() {
                bail!("Starting a tunnel requires Edit/Bypass mode (got {:?})", mode);
            }
            let (tunnels, target, data_dir) =
                (state.tunnels(), target.clone(), dirs::data_dir());
            let info = tokio::task::spawn_blocking(move || tunnels.start(&target, data_dir))
                .await
                .map_err(|e| anyhow::anyhow!("tunnel task failed: {e}"))??;
            Ok(CommandResult::TunnelStarted { tunnel: info })
        }
        Command::TunnelList => {
            let tunnels = state.tunnels();
            let list = tokio::task::spawn_blocking(move || tunnels.list())
                .await
                .map_err(|e| anyhow::anyhow!("tunnel task failed: {e}"))?;
            Ok(CommandResult::TunnelList { tunnels: list })
        }
        Command::TunnelStop { id } => {
            if !mode.allows_write() {
                bail!("Stopping a tunnel requires Edit/Bypass mode (got {:?})", mode);
            }
            let (tunnels, id) = (state.tunnels(), id.clone());
            tokio::task::spawn_blocking(move || tunnels.stop(&id))
                .await
                .map_err(|e| anyhow::anyhow!("tunnel task failed: {e}"))??;
            Ok(CommandResult::Ok)
        }

        // MapReduce (Phase 13): map_fn/reduce_fn are shell commands; the
        // partition data (or collected map outputs) is fed on stdin. This
        // reuses the existing shell executor and safety policy — no separate
        // scripting runtime needed.
        Command::MapTask {
            job_id,
            partition_id,
            map_fn,
            data,
        } => {
            info!("MapTask {}#{}: {}", job_id, partition_id, map_fn);
            let (output, success, error) =
                run_compute_fn(map_fn, data, mode, sec).await;
            Ok(CommandResult::MapResult {
                job_id: job_id.clone(),
                partition_id: *partition_id,
                output,
                success,
                error,
            })
        }
        Command::ReduceTask {
            job_id,
            reduce_fn,
            inputs,
        } => {
            info!("ReduceTask {}: {}", job_id, reduce_fn);
            // Map outputs are joined by newline so line-oriented tools
            // (awk/sort/paste/…) can fold them naturally.
            let stdin = inputs.join("\n");
            let (output, success, error) =
                run_compute_fn(reduce_fn, &stdin, mode, sec).await;
            Ok(CommandResult::ReduceResult {
                job_id: job_id.clone(),
                output,
                success,
                error,
            })
        }
    }
}

/// Receive one host↔host transfer chunk, transport-aware. The size cap
/// (`max_transfer_size`) exists to protect the shared RELAY from huge transfers;
/// a direct UDP (p2p) channel doesn't touch the relay, so the cap is lifted when
/// `over_udp` is set. The connection loops call this (they know the transport);
/// the generic [`execute`] keeps the cap for the relay/WS path. Returns
/// `CommandResult::Ok` once the slice is written (and verified on `eof`).
pub async fn recv_file_chunk(
    state: &AgentState,
    dest_path: &str,
    offset: u64,
    bytes_b64: &str,
    eof: bool,
    sha256: Option<&str>,
    over_udp: bool,
) -> Result<CommandResult> {
    if !state.config.accepts_commands {
        bail!("This node does not accept remote commands (--no-agent)");
    }
    let mode = state.mode().await;
    if !mode.allows_write() {
        bail!("Receiving a file requires Edit/Bypass mode (got {:?})", mode);
    }
    let mut sec = state.config.security.clone();
    if over_udp {
        sec.max_transfer_size = 0; // direct p2p: no relay to protect → no cap
    }
    let (dest_path, bytes, sha) = (dest_path.to_string(), bytes_b64.to_string(), sha256.map(str::to_string));
    tokio::task::spawn_blocking(move || {
        crate::transfer::receive_chunk(&dest_path, offset, &bytes, eof, sha.as_deref(), &sec)
    })
    .await
    .map_err(|e| anyhow::anyhow!("receive chunk failed: {e}"))??;
    Ok(CommandResult::Ok)
}

/// Write a raw (already-decrypted, un-base64'd) file slice at `offset`. The
/// binary counterpart to [`recv_file_chunk`] used by the direct-UDP file path,
/// where slices arrive as raw bytes in a `UdpFrame::FileData`.
pub async fn recv_file_chunk_raw(
    state: &AgentState,
    dest_path: &str,
    offset: u64,
    raw: Vec<u8>,
    eof: bool,
    sha256: Option<&str>,
    over_udp: bool,
) -> Result<CommandResult> {
    if !state.config.accepts_commands {
        bail!("This node does not accept remote commands (--no-agent)");
    }
    let mode = state.mode().await;
    if !mode.allows_write() {
        bail!("Receiving a file requires Edit/Bypass mode (got {:?})", mode);
    }
    let mut sec = state.config.security.clone();
    if over_udp {
        sec.max_transfer_size = 0; // direct p2p: no relay to protect → no cap
    }
    let (dest_path, sha) = (dest_path.to_string(), sha256.map(str::to_string));
    tokio::task::spawn_blocking(move || {
        crate::transfer::receive_chunk_raw(&dest_path, offset, &raw, eof, sha.as_deref(), &sec)
    })
    .await
    .map_err(|e| anyhow::anyhow!("receive chunk failed: {e}"))??;
    Ok(CommandResult::Ok)
}

/// Run a MapReduce compute function: a shell command `func` fed `stdin_data` on
/// standard input. Returns `(output, success, error)` so a single failing
/// partition surfaces as a failed result rather than aborting the whole job.
/// The same safety policy that gates `Exec` applies here.
async fn run_compute_fn(
    func: &str,
    stdin_data: &str,
    mode: AgentMode,
    sec: &crate::config::SecurityConfig,
) -> (String, bool, Option<String>) {
    if let Err(e) = safety::check_command_allowed(func, mode, sec) {
        return (String::new(), false, Some(format!("blocked by safety policy: {e}")));
    }
    let timeout = sec.command_timeout * 1000;
    match shell::exec_with_stdin(func, stdin_data, timeout).await {
        Ok(r) if r.exit_code == 0 => (r.stdout, true, None),
        Ok(r) => (
            r.stdout,
            false,
            Some(format!("exit {}: {}", r.exit_code, r.stderr.trim())),
        ),
        Err(e) => (String::new(), false, Some(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    async fn state_in(mode: AgentMode) -> AgentState {
        let s = AgentState::new(Config::default());
        s.set_mode(mode).await;
        s
    }

    async fn bypass_state() -> AgentState {
        state_in(AgentMode::Bypass).await
    }

    fn tmp_file(tag: &str) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir()
            .join(format!(
                "exec-test-{}-{}-{}.txt",
                tag,
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed),
            ))
            .to_string_lossy()
            .to_string()
    }

    #[tokio::test]
    async fn no_agent_node_rejects_remote_commands() {
        // A send-only peer (--no-agent) never executes others' commands, even in
        // bypass mode and even for a side-effect-free command like GetInfo.
        let config = Config {
            accepts_commands: false,
            ..Config::default()
        };
        let state = AgentState::new(config);
        state.set_mode(AgentMode::Bypass).await;

        let err = execute(&Command::GetInfo, &state).await.unwrap_err().to_string();
        assert!(err.contains("--no-agent"), "got: {err}");
    }

    #[tokio::test]
    async fn disabled_mode_rejects_commands() {
        let state = state_in(AgentMode::Disabled).await;
        let err = execute(&Command::GetInfo, &state).await.unwrap_err().to_string();
        assert!(err.contains("disabled"), "got: {err}");
    }

    #[tokio::test]
    async fn set_mode_updates_state() {
        let state = bypass_state().await;
        match execute(&Command::SetMode { mode: AgentMode::Edit }, &state).await.unwrap() {
            CommandResult::Mode { mode } => assert_eq!(mode, AgentMode::Edit),
            other => panic!("expected Mode, got {other:?}"),
        }
        assert_eq!(state.mode().await, AgentMode::Edit);
    }

    #[tokio::test]
    async fn get_info_reports_platform() {
        let state = state_in(AgentMode::Plan).await;
        match execute(&Command::GetInfo, &state).await.unwrap() {
            CommandResult::Info { info } => {
                assert_eq!(info.os, std::env::consts::OS);
                assert_eq!(info.arch, std::env::consts::ARCH);
                assert_eq!(info.mode, AgentMode::Plan);
            }
            other => panic!("expected Info, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn write_blocked_in_plan_mode() {
        let state = state_in(AgentMode::Plan).await;
        let cmd = Command::WriteFile {
            path: tmp_file("plan"),
            content: "nope".into(),
            create_backup: false,
        };
        let err = execute(&cmd, &state).await.unwrap_err().to_string();
        assert!(err.contains("Write operations not allowed"), "got: {err}");
    }

    #[tokio::test]
    async fn read_write_roundtrip_in_bypass() {
        let state = bypass_state().await;
        let path = tmp_file("rw");

        let write = Command::WriteFile {
            path: path.clone(),
            content: "round-trip".into(),
            create_backup: false,
        };
        assert!(matches!(
            execute(&write, &state).await.unwrap(),
            CommandResult::Ok
        ));

        match execute(&Command::ReadFile { path: path.clone() }, &state).await.unwrap() {
            CommandResult::File { content, size } => {
                assert_eq!(content, "round-trip");
                assert_eq!(size, "round-trip".len() as u64);
            }
            other => panic!("expected File, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn git_pull_blocked_in_readonly_mode() {
        let state = state_in(AgentMode::Plan).await;
        let cmd = Command::GitPull {
            repo: "/tmp/whatever".into(),
            remote: "origin".into(),
            branch: None,
        };
        let err = execute(&cmd, &state).await.unwrap_err().to_string();
        assert!(err.contains("not allowed"), "got: {err}");
    }

    #[tokio::test]
    async fn task_dispatch_errors_when_autonomous_disabled() {
        // Force autonomous off (default is auto-detect, which could be enabled on
        // a machine that has the runner CLI on PATH).
        let config = Config {
            autonomous: crate::config::AutonomousConfig {
                enabled: Some(false),
                ..Default::default()
            },
            ..Default::default()
        };
        let state = AgentState::new(config);
        state.set_mode(AgentMode::Bypass).await;

        let cmd = Command::TaskDispatch { prompt: "do a thing".into(), initiator: None };
        let err = execute(&cmd, &state).await.unwrap_err().to_string();
        assert!(err.contains("autonomous mode is not enabled"), "got: {err}");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn map_task_runs_shell_over_stdin() {
        let state = bypass_state().await;
        let cmd = Command::MapTask {
            job_id: "j".into(),
            partition_id: 2,
            map_fn: "tr a-z A-Z".into(),
            data: "hello".into(),
        };
        match execute(&cmd, &state).await.unwrap() {
            CommandResult::MapResult {
                job_id,
                partition_id,
                output,
                success,
                error,
            } => {
                assert_eq!(job_id, "j");
                assert_eq!(partition_id, 2);
                assert!(success);
                assert!(error.is_none());
                assert_eq!(output.trim(), "HELLO");
            }
            other => panic!("expected MapResult, got {other:?}"),
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn reduce_task_folds_inputs_via_stdin() {
        let state = bypass_state().await;
        // Map outputs arrive newline-joined; sum them with awk.
        let cmd = Command::ReduceTask {
            job_id: "j".into(),
            reduce_fn: "awk '{s+=$1} END {print s}'".into(),
            inputs: vec!["1".into(), "2".into(), "3".into()],
        };
        match execute(&cmd, &state).await.unwrap() {
            CommandResult::ReduceResult { output, success, .. } => {
                assert!(success);
                assert_eq!(output.trim(), "6");
            }
            other => panic!("expected ReduceResult, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn map_task_nonzero_exit_is_captured_as_failure() {
        let state = bypass_state().await;
        let cmd = Command::MapTask {
            job_id: "j".into(),
            partition_id: 0,
            map_fn: "exit 3".into(),
            data: String::new(),
        };
        match execute(&cmd, &state).await.unwrap() {
            CommandResult::MapResult { success, error, .. } => {
                assert!(!success);
                assert!(error.unwrap().contains("exit 3"));
            }
            other => panic!("expected MapResult, got {other:?}"),
        }
    }
}
