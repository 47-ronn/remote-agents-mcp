//! Relay API for controlling remote agents
//!
//! Provides high-level API for controlling remote agents via relay server.

use crate::relay_controller::{AgentOutcome, ConnectionPool};
use anyhow::Result;
use remote_agents_shared::{Command, CommandResult};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Build the wire `Command` for a fleet git operation. Pure (no I/O) so the
/// op-name → command mapping can be unit-tested without a live relay.
/// `op` is one of `status` | `pull` | `commit` | `push`; a `None` remote
/// defaults to `origin`.
fn build_git_command(
    op: &str,
    repo: &str,
    remote: Option<String>,
    branch: Option<String>,
    message: Option<String>,
    files: Vec<String>,
) -> Result<Command> {
    let remote = remote.unwrap_or_else(|| "origin".to_string());
    Ok(match op {
        "status" => Command::GitStatus {
            repo: repo.to_string(),
        },
        "pull" => Command::GitPull {
            repo: repo.to_string(),
            remote,
            branch,
        },
        "commit" => Command::GitCommit {
            repo: repo.to_string(),
            message: message.unwrap_or_default(),
            files,
        },
        "push" => Command::GitPush {
            repo: repo.to_string(),
            remote,
            branch,
        },
        other => return Err(anyhow::anyhow!("unknown git op '{}'", other)),
    })
}

/// Render a `CommandResult` from a git operation into human-readable text.
/// Pure, so the variant → text mapping is unit-testable.
fn git_result_text(r: CommandResult) -> String {
    match r {
        CommandResult::Git { output, .. } => output,
        CommandResult::Ok => "ok".to_string(),
        other => format!("{:?}", other),
    }
}

/// MCP Server state
pub struct McpServer {
    /// Connection pool to relay servers
    connections: Arc<RwLock<ConnectionPool>>,
}

impl McpServer {
    /// Create a new MCP server instance
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(ConnectionPool::new())),
        }
    }

    /// Join a room on the relay server. `key` optionally overrides the
    /// token-derived end-to-end encryption key (must match the agents).
    pub async fn join_room(
        &self,
        relay_url: &str,
        room: &str,
        token: &str,
        key: Option<&str>,
    ) -> Result<String> {
        let mut pool = self.connections.write().await;
        pool.connect(relay_url, room, token, key).await
    }

    /// Leave a room
    pub async fn leave_room(&self, room: &str) -> Result<()> {
        let mut pool = self.connections.write().await;
        pool.disconnect(room).await
    }

    /// List agents in a room
    pub async fn list_agents(
        &self,
        room: &str,
    ) -> Result<Vec<remote_agents_shared::AgentInfo>> {
        let pool = self.connections.read().await;
        pool.list_agents(room).await
    }

    /// Execute a command on an agent
    pub async fn exec(
        &self,
        room: &str,
        target: remote_agents_shared::Target,
        command: &str,
        timeout_ms: Option<u64>,
    ) -> Result<Vec<(String, remote_agents_shared::CommandResult)>> {
        let pool = self.connections.read().await;
        pool.send_command(
            room,
            target,
            remote_agents_shared::Command::Exec {
                command: command.to_string(),
                timeout_ms,
                cwd: None,
            },
        )
        .await
    }

    /// Execute a command across a fleet (single agent, all, or tagged),
    /// returning a per-agent outcome so one failure doesn't sink the batch.
    pub async fn fleet_exec(
        &self,
        room: &str,
        target: remote_agents_shared::Target,
        command: &str,
        timeout_ms: Option<u64>,
    ) -> Result<Vec<AgentOutcome>> {
        let pool = self.connections.read().await;
        pool.send_command_fleet(
            room,
            target,
            remote_agents_shared::Command::Exec {
                command: command.to_string(),
                timeout_ms,
                cwd: None,
            },
        )
        .await
    }

    /// Read a file across a fleet (single agent, all, or tagged).
    pub async fn fleet_read(
        &self,
        room: &str,
        target: remote_agents_shared::Target,
        path: &str,
    ) -> Result<Vec<AgentOutcome>> {
        let pool = self.connections.read().await;
        pool.send_command_fleet(
            room,
            target,
            remote_agents_shared::Command::ReadFile {
                path: path.to_string(),
            },
        )
        .await
    }

    /// Write a file across a fleet (single agent, all, or tagged).
    pub async fn fleet_write(
        &self,
        room: &str,
        target: remote_agents_shared::Target,
        path: &str,
        content: &str,
    ) -> Result<Vec<AgentOutcome>> {
        let pool = self.connections.read().await;
        pool.send_command_fleet(
            room,
            target,
            remote_agents_shared::Command::WriteFile {
                path: path.to_string(),
                content: content.to_string(),
                create_backup: true,
            },
        )
        .await
    }

    /// Run a git operation across a fleet. `op` is one of
    /// `status` | `pull` | `commit` | `push`.
    #[allow(clippy::too_many_arguments)]
    pub async fn fleet_git(
        &self,
        room: &str,
        target: remote_agents_shared::Target,
        op: &str,
        repo: &str,
        remote: Option<String>,
        branch: Option<String>,
        message: Option<String>,
        files: Vec<String>,
    ) -> Result<Vec<AgentOutcome>> {
        let command = build_git_command(op, repo, remote, branch, message, files)?;
        let pool = self.connections.read().await;
        pool.send_command_fleet(room, target, command).await
    }

    /// Read a file from an agent
    pub async fn read_file(
        &self,
        room: &str,
        agent_id: &str,
        path: &str,
    ) -> Result<String> {
        let pool = self.connections.read().await;
        let results = pool
            .send_command(
                room,
                remote_agents_shared::Target::Agent {
                    id: agent_id.to_string(),
                },
                remote_agents_shared::Command::ReadFile {
                    path: path.to_string(),
                },
            )
            .await?;

        if let Some((_, remote_agents_shared::CommandResult::File { content, .. })) =
            results.into_iter().next()
        {
            Ok(content)
        } else {
            Err(anyhow::anyhow!("Failed to read file"))
        }
    }

    /// Write a file to an agent
    pub async fn write_file(
        &self,
        room: &str,
        agent_id: &str,
        path: &str,
        content: &str,
    ) -> Result<()> {
        let pool = self.connections.read().await;
        pool.send_command(
            room,
            remote_agents_shared::Target::Agent {
                id: agent_id.to_string(),
            },
            remote_agents_shared::Command::WriteFile {
                path: path.to_string(),
                content: content.to_string(),
                create_backup: true,
            },
        )
        .await?;

        Ok(())
    }

    /// Send an arbitrary command to a target (agent, all, or tagged).
    /// Returns a list of (agent_id, result) pairs.
    pub async fn send_command(
        &self,
        room: &str,
        target: remote_agents_shared::Target,
        command: remote_agents_shared::Command,
    ) -> Result<Vec<(String, remote_agents_shared::CommandResult)>> {
        let pool = self.connections.read().await;
        pool.send_command(room, target, command).await
    }

    /// Change an agent's operating mode.
    pub async fn set_mode(
        &self,
        room: &str,
        agent_id: &str,
        mode: remote_agents_shared::AgentMode,
    ) -> Result<()> {
        let pool = self.connections.read().await;
        pool.send_command(
            room,
            remote_agents_shared::Target::Agent {
                id: agent_id.to_string(),
            },
            remote_agents_shared::Command::SetMode { mode },
        )
        .await?;
        Ok(())
    }

    /// Get structured git status for a repo on an agent.
    pub async fn git_status(
        &self,
        room: &str,
        agent_id: &str,
        repo: &str,
    ) -> Result<remote_agents_shared::GitStatus> {
        let pool = self.connections.read().await;
        let results = pool
            .send_command(
                room,
                remote_agents_shared::Target::Agent {
                    id: agent_id.to_string(),
                },
                remote_agents_shared::Command::GitStatus {
                    repo: repo.to_string(),
                },
            )
            .await?;

        match results.into_iter().next() {
            Some((_, remote_agents_shared::CommandResult::GitStatus { status })) => Ok(status),
            _ => Err(anyhow::anyhow!("unexpected git status result")),
        }
    }

    /// Commit staged/all changes on an agent's repo.
    pub async fn git_commit(
        &self,
        room: &str,
        agent_id: &str,
        repo: &str,
        message: &str,
        files: Vec<String>,
    ) -> Result<String> {
        self.git_text(
            room,
            remote_agents_shared::Target::Agent {
                id: agent_id.to_string(),
            },
            remote_agents_shared::Command::GitCommit {
                repo: repo.to_string(),
                message: message.to_string(),
                files,
            },
        )
        .await
        .map(|v| v.into_iter().map(|(_, o)| o).collect::<Vec<_>>().join("\n"))
    }

    /// Pull every agent's repo in one broadcast — the `git_merge_all`
    /// convenience for keeping a fleet in sync.
    pub async fn git_merge_all(
        &self,
        room: &str,
        repo: &str,
        remote: &str,
        branch: Option<String>,
    ) -> Result<Vec<(String, String)>> {
        self.git_text(
            room,
            remote_agents_shared::Target::All,
            remote_agents_shared::Command::GitPull {
                repo: repo.to_string(),
                remote: remote.to_string(),
                branch,
            },
        )
        .await
    }

    async fn git_text(
        &self,
        room: &str,
        target: remote_agents_shared::Target,
        command: remote_agents_shared::Command,
    ) -> Result<Vec<(String, String)>> {
        let pool = self.connections.read().await;
        let results = pool.send_command(room, target, command).await?;
        Ok(results
            .into_iter()
            .map(|(id, r)| (id, git_result_text(r)))
            .collect())
    }

    /// Add a scheduled task on an agent.
    pub async fn schedule_add(
        &self,
        room: &str,
        agent_id: &str,
        name: &str,
        cron: &str,
        command: &str,
    ) -> Result<()> {
        let pool = self.connections.read().await;
        pool.send_command(
            room,
            remote_agents_shared::Target::Agent {
                id: agent_id.to_string(),
            },
            remote_agents_shared::Command::ScheduleAdd {
                name: name.to_string(),
                cron: cron.to_string(),
                command: command.to_string(),
            },
        )
        .await?;
        Ok(())
    }

    /// List scheduled tasks on an agent.
    pub async fn schedule_list(
        &self,
        room: &str,
        agent_id: &str,
    ) -> Result<Vec<remote_agents_shared::ScheduledTask>> {
        let pool = self.connections.read().await;
        let results = pool
            .send_command(
                room,
                remote_agents_shared::Target::Agent {
                    id: agent_id.to_string(),
                },
                remote_agents_shared::Command::ScheduleList,
            )
            .await?;
        match results.into_iter().next() {
            Some((_, remote_agents_shared::CommandResult::Schedule { tasks })) => Ok(tasks),
            _ => Err(anyhow::anyhow!("unexpected schedule list result")),
        }
    }

    /// Dispatch an autonomous AI task to a host (runs with the host's own
    /// credentials). Returns the new task id immediately.
    pub async fn task_dispatch(
        &self,
        room: &str,
        agent_id: &str,
        prompt: &str,
    ) -> Result<String> {
        let pool = self.connections.read().await;
        let results = pool
            .send_command(
                room,
                remote_agents_shared::Target::Agent {
                    id: agent_id.to_string(),
                },
                remote_agents_shared::Command::TaskDispatch {
                    prompt: prompt.to_string(),
                },
            )
            .await?;
        match results.into_iter().next() {
            Some((_, remote_agents_shared::CommandResult::TaskQueued { id })) => Ok(id),
            _ => Err(anyhow::anyhow!("unexpected task dispatch result")),
        }
    }

    /// Dispatch an autonomous task AND register a reminder cron on the
    /// initiator's own agent (`self_agent_id`). The reminder is auto-cancelled
    /// when the host pushes a completion event. Returns the task id.
    pub async fn task_dispatch_watched(
        &self,
        room: &str,
        agent_id: &str,
        prompt: &str,
        self_agent_id: &str,
        cron: &str,
        command: &str,
    ) -> Result<String> {
        let id = self.task_dispatch(room, agent_id, prompt).await?;
        let reminder = format!("remind-{}", id);
        let pool = self.connections.read().await;
        pool.send_command(
            room,
            remote_agents_shared::Target::Agent {
                id: self_agent_id.to_string(),
            },
            remote_agents_shared::Command::ScheduleAdd {
                name: reminder.clone(),
                cron: cron.to_string(),
                command: command.to_string(),
            },
        )
        .await?;
        pool.register_watch(room, &id, &reminder, self_agent_id).await?;
        Ok(id)
    }

    /// Wait for an autonomous task to complete (push-driven) or time out, then
    /// return its full state.
    pub async fn task_wait(
        &self,
        room: &str,
        agent_id: &str,
        task_id: &str,
        timeout_ms: u64,
    ) -> Result<remote_agents_shared::AutonomousTask> {
        let pool = self.connections.read().await;
        pool.task_wait(room, agent_id, task_id, timeout_ms).await
    }

    /// Get a single autonomous task (status + result) from a host.
    pub async fn task_get(
        &self,
        room: &str,
        agent_id: &str,
        id: &str,
    ) -> Result<remote_agents_shared::AutonomousTask> {
        let pool = self.connections.read().await;
        let results = pool
            .send_command(
                room,
                remote_agents_shared::Target::Agent {
                    id: agent_id.to_string(),
                },
                remote_agents_shared::Command::TaskGet { id: id.to_string() },
            )
            .await?;
        match results.into_iter().next() {
            Some((_, remote_agents_shared::CommandResult::Task { task })) => Ok(task),
            _ => Err(anyhow::anyhow!("unexpected task get result")),
        }
    }

    /// List autonomous tasks on a host.
    pub async fn task_list(
        &self,
        room: &str,
        agent_id: &str,
    ) -> Result<Vec<remote_agents_shared::AutonomousTask>> {
        let pool = self.connections.read().await;
        let results = pool
            .send_command(
                room,
                remote_agents_shared::Target::Agent {
                    id: agent_id.to_string(),
                },
                remote_agents_shared::Command::TaskList,
            )
            .await?;
        match results.into_iter().next() {
            Some((_, remote_agents_shared::CommandResult::TaskList { tasks })) => Ok(tasks),
            _ => Err(anyhow::anyhow!("unexpected task list result")),
        }
    }
}

impl Default for McpServer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_git_command_maps_each_op() {
        match build_git_command("status", "/repo", None, None, None, vec![]).unwrap() {
            Command::GitStatus { repo } => assert_eq!(repo, "/repo"),
            other => panic!("expected GitStatus, got {other:?}"),
        }

        // pull: explicit remote + branch are threaded through.
        match build_git_command(
            "pull",
            "/repo",
            Some("upstream".into()),
            Some("main".into()),
            None,
            vec![],
        )
        .unwrap()
        {
            Command::GitPull { repo, remote, branch } => {
                assert_eq!(repo, "/repo");
                assert_eq!(remote, "upstream");
                assert_eq!(branch.as_deref(), Some("main"));
            }
            other => panic!("expected GitPull, got {other:?}"),
        }

        match build_git_command(
            "commit",
            "/repo",
            None,
            None,
            Some("msg".into()),
            vec!["a.txt".into()],
        )
        .unwrap()
        {
            Command::GitCommit { repo, message, files } => {
                assert_eq!(repo, "/repo");
                assert_eq!(message, "msg");
                assert_eq!(files, vec!["a.txt".to_string()]);
            }
            other => panic!("expected GitCommit, got {other:?}"),
        }

        match build_git_command("push", "/repo", None, None, None, vec![]).unwrap() {
            Command::GitPush { remote, .. } => assert_eq!(remote, "origin"),
            other => panic!("expected GitPush, got {other:?}"),
        }
    }

    #[test]
    fn build_git_command_defaults_remote_to_origin() {
        match build_git_command("pull", "/repo", None, None, None, vec![]).unwrap() {
            Command::GitPull { remote, .. } => assert_eq!(remote, "origin"),
            other => panic!("expected GitPull, got {other:?}"),
        }
    }

    #[test]
    fn build_git_command_commit_defaults_empty_message() {
        match build_git_command("commit", "/repo", None, None, None, vec![]).unwrap() {
            Command::GitCommit { message, .. } => assert_eq!(message, ""),
            other => panic!("expected GitCommit, got {other:?}"),
        }
    }

    #[test]
    fn build_git_command_rejects_unknown_op() {
        let err = build_git_command("rebase", "/repo", None, None, None, vec![])
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown git op"), "got: {err}");
        assert!(err.contains("rebase"), "op name should be echoed: {err}");
    }

    #[test]
    fn git_result_text_renders_variants() {
        assert_eq!(
            git_result_text(CommandResult::Git {
                output: "Already up to date.".into(),
                success: true,
            }),
            "Already up to date."
        );
        assert_eq!(git_result_text(CommandResult::Ok), "ok");
        // A non-git variant falls back to its Debug rendering rather than panicking.
        let other = git_result_text(CommandResult::Exec {
            stdout: "hi".into(),
            stderr: String::new(),
            exit_code: 0,
        });
        assert!(other.contains("Exec"), "got: {other}");
    }
}
