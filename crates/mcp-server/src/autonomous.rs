//! Autonomous AI task execution.
//!
//! When enabled on a host, the agent can accept delegated AI tasks and run them
//! with the HOST'S OWN credentials (its configured AI CLI login, inherited from
//! the agent process environment — the agent stores no keys). The orchestrator
//! dispatches a task and gets an id back immediately; the task runs in the
//! background, and the result is collected later (e.g. via a cron reminder),
//! spending none of the orchestrator's tokens.
//!
//! Tasks are persisted in SQLite so results survive restarts.

use crate::config::AutonomousConfig;
use anyhow::{bail, Context, Result};
use remote_agents_shared::{AgentEvent, AutonomousTask, TaskStatus};
use rusqlite::Connection;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{error, info, warn};

pub struct AutonomousStore {
    config: AutonomousConfig,
    db: Mutex<Connection>,
    /// Outbound event channel: completion is pushed here, forwarded to the relay
    /// by the connection loop.
    events: mpsc::UnboundedSender<AgentEvent>,
}

impl AutonomousStore {
    /// Open (or create) the task store at `path`.
    pub fn load(
        path: PathBuf,
        config: AutonomousConfig,
        events: mpsc::UnboundedSender<AgentEvent>,
    ) -> Self {
        let conn = match open_db(&path) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to open tasks DB ({:?}): {}; using in-memory", path, e);
                Connection::open_in_memory().expect("in-memory sqlite")
            }
        };
        let _ = init_schema(&conn);
        Self {
            config,
            db: Mutex::new(conn),
            events,
        }
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    /// Accept a task: persist it as Queued and spawn the runner in the
    /// background. Returns the new task id immediately.
    pub fn dispatch(self: &Arc<Self>, prompt: &str) -> Result<String> {
        if !self.config.enabled {
            bail!("autonomous mode is not enabled on this host");
        }

        let id = uuid::Uuid::new_v4().to_string();
        let task = AutonomousTask {
            id: id.clone(),
            prompt: prompt.to_string(),
            status: TaskStatus::Queued,
            result: None,
            error: None,
            created_at: now_ms(),
            started_at: None,
            finished_at: None,
            exit_code: None,
        };
        self.insert(&task)?;

        // Run in the background; the orchestrator polls for the result later.
        let store = self.clone();
        let prompt = prompt.to_string();
        let id_bg = id.clone();
        tokio::spawn(async move {
            store.run_task(&id_bg, &prompt).await;
        });

        info!("Autonomous task '{}' queued", id);
        Ok(id)
    }

    /// Fetch a single task by id.
    pub fn get(&self, id: &str) -> Result<Option<AutonomousTask>> {
        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare(SELECT_COLUMNS_WHERE_ID)?;
        let mut rows = stmt.query_map([id], row_to_task)?;
        match rows.next() {
            Some(t) => Ok(Some(t?)),
            None => Ok(None),
        }
    }

    /// List all tasks, newest first.
    pub fn list(&self) -> Result<Vec<AutonomousTask>> {
        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM autonomous_tasks ORDER BY created_at DESC",
            COLUMNS
        ))?;
        let rows = stmt.query_map([], row_to_task)?;
        let mut out = Vec::new();
        for t in rows {
            out.push(t?);
        }
        Ok(out)
    }

    // --- internals ---------------------------------------------------------

    async fn run_task(&self, id: &str, prompt: &str) {
        if self.config.runner.is_empty() {
            self.finish(id, TaskStatus::Failed, None, Some("empty runner command".into()), None);
            return;
        }

        self.mark_running(id);

        let program = &self.config.runner[0];
        let mut cmd = Command::new(program);
        cmd.args(&self.config.runner[1..]);
        cmd.arg(prompt); // prompt appended as the final argument
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        if let Some(dir) = &self.config.workdir {
            cmd.current_dir(dir);
        } else if let Some(home) = dirs::home_dir() {
            cmd.current_dir(home);
        }
        // NOTE: env is inherited as-is → the host's existing AI CLI login is used.

        info!("Running autonomous task '{}' via {:?}", id, program);
        let fut = cmd.output();
        match timeout(Duration::from_secs(self.config.timeout), fut).await {
            Ok(Ok(output)) => {
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                let code = output.status.code();
                if output.status.success() {
                    self.finish(id, TaskStatus::Done, Some(stdout), None, code);
                } else {
                    let err = if stderr.trim().is_empty() {
                        format!("runner exited with code {:?}", code)
                    } else {
                        stderr
                    };
                    self.finish(id, TaskStatus::Failed, Some(stdout), Some(err), code);
                }
            }
            Ok(Err(e)) => {
                error!("Autonomous task '{}' failed to spawn: {}", id, e);
                self.finish(
                    id,
                    TaskStatus::Failed,
                    None,
                    Some(format!("failed to run '{}': {}", program, e)),
                    None,
                );
            }
            Err(_) => {
                warn!("Autonomous task '{}' timed out", id);
                self.finish(
                    id,
                    TaskStatus::Failed,
                    None,
                    Some(format!("timed out after {}s", self.config.timeout)),
                    None,
                );
            }
        }
    }

    fn insert(&self, task: &AutonomousTask) -> Result<()> {
        let conn = self.db.lock().unwrap();
        conn.execute(
            "INSERT INTO autonomous_tasks
                (id, prompt, status, result, error, created_at, started_at, finished_at, exit_code)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                task.id,
                task.prompt,
                status_str(task.status),
                task.result,
                task.error,
                task.created_at as i64,
                task.started_at.map(|v| v as i64),
                task.finished_at.map(|v| v as i64),
                task.exit_code,
            ],
        )
        .context("insert autonomous task")?;
        Ok(())
    }

    fn mark_running(&self, id: &str) {
        let conn = self.db.lock().unwrap();
        let _ = conn.execute(
            "UPDATE autonomous_tasks SET status = 'running', started_at = ?2 WHERE id = ?1",
            rusqlite::params![id, now_ms() as i64],
        );
    }

    fn finish(
        &self,
        id: &str,
        status: TaskStatus,
        result: Option<String>,
        error: Option<String>,
        exit_code: Option<i32>,
    ) {
        let conn = self.db.lock().unwrap();
        let _ = conn.execute(
            "UPDATE autonomous_tasks
                SET status = ?2, result = ?3, error = ?4, exit_code = ?5, finished_at = ?6
              WHERE id = ?1",
            rusqlite::params![
                id,
                status_str(status),
                result,
                error,
                exit_code,
                now_ms() as i64
            ],
        );
        info!("Autonomous task '{}' -> {:?}", id, status);

        // Push a completion event so the initiator learns early (and cancels its
        // reminder cron) without polling. Ignored if no one is listening.
        let _ = self.events.send(AgentEvent::TaskCompleted {
            task_id: id.to_string(),
            status,
        });
    }
}

const COLUMNS: &str =
    "id, prompt, status, result, error, created_at, started_at, finished_at, exit_code";

const SELECT_COLUMNS_WHERE_ID: &str =
    "SELECT id, prompt, status, result, error, created_at, started_at, finished_at, exit_code \
     FROM autonomous_tasks WHERE id = ?1";

fn row_to_task(row: &rusqlite::Row) -> rusqlite::Result<AutonomousTask> {
    Ok(AutonomousTask {
        id: row.get(0)?,
        prompt: row.get(1)?,
        status: status_from(&row.get::<_, String>(2)?),
        result: row.get(3)?,
        error: row.get(4)?,
        created_at: row.get::<_, i64>(5)? as u64,
        started_at: row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
        finished_at: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
        exit_code: row.get(8)?,
    })
}

fn status_str(s: TaskStatus) -> &'static str {
    match s {
        TaskStatus::Queued => "queued",
        TaskStatus::Running => "running",
        TaskStatus::Done => "done",
        TaskStatus::Failed => "failed",
    }
}

fn status_from(s: &str) -> TaskStatus {
    match s {
        "running" => TaskStatus::Running,
        "done" => TaskStatus::Done,
        "failed" => TaskStatus::Failed,
        _ => TaskStatus::Queued,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn open_db(path: &PathBuf) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    Connection::open(path).context("open tasks db")
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS autonomous_tasks (
            id          TEXT PRIMARY KEY,
            prompt      TEXT NOT NULL,
            status      TEXT NOT NULL,
            result      TEXT,
            error       TEXT,
            created_at  INTEGER NOT NULL,
            started_at  INTEGER,
            finished_at INTEGER,
            exit_code   INTEGER
        )",
        [],
    )
    .context("create autonomous_tasks schema")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(enabled: bool) -> Arc<AutonomousStore> {
        let path = std::env::temp_dir().join(format!(
            "auto-test-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let cfg = AutonomousConfig {
            enabled,
            // Use a trivial, always-present runner for the test.
            runner: vec!["echo".to_string()],
            ..Default::default()
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        Arc::new(AutonomousStore::load(path, cfg, tx))
    }

    #[tokio::test]
    async fn disabled_rejects_dispatch() {
        let s = store(false);
        assert!(s.dispatch("do something").is_err());
    }

    #[tokio::test]
    async fn dispatch_runs_and_persists_result() {
        let s = store(true);
        let id = s.dispatch("hello world").unwrap();

        // Poll until the background runner finishes.
        let mut task = None;
        for _ in 0..50 {
            let t = s.get(&id).unwrap().unwrap();
            if matches!(t.status, TaskStatus::Done | TaskStatus::Failed) {
                task = Some(t);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let task = task.expect("task did not finish in time");
        assert_eq!(task.status, TaskStatus::Done, "error: {:?}", task.error);
        // `echo` echoes the prompt back.
        assert!(task.result.unwrap_or_default().contains("hello world"));
        assert_eq!(s.list().unwrap().len(), 1);
    }
}
