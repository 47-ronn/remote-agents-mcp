//! Command execution module.

mod filesystem;
mod git;
mod shell;

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
                autonomous: state.config.autonomous.enabled,
                accepts_commands: state.config.accepts_commands,
                connected_at: 0,
                session_id: None,
                update_available: crate::config::update_available(),
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
        // Config::default() leaves autonomous mode disabled.
        let state = bypass_state().await;
        let cmd = Command::TaskDispatch { prompt: "do a thing".into(), initiator: None };
        let err = execute(&cmd, &state).await.unwrap_err().to_string();
        assert!(err.contains("autonomous mode is not enabled"), "got: {err}");
    }

    #[tokio::test]
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
