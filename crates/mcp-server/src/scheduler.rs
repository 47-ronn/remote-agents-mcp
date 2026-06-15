//! Cron-like scheduler backed by SQLite (rusqlite).
//!
//! Tasks are 6-field cron expressions (`sec min hour day month weekday`) that
//! run a shell command on the agent. Definitions and run stats are persisted in
//! a SQLite database so they survive restarts; an in-memory cache keeps the
//! once-per-second tick loop allocation-free. The loop fires any task whose next
//! scheduled time has elapsed since it last ran.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use cron::Schedule as CronSchedule;
use remote_agents_shared::ScheduledTask;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

pub struct Scheduler {
    /// In-memory source of truth for the tick loop (mirrored to SQLite).
    tasks: RwLock<HashMap<String, ScheduledTask>>,
    /// In-memory baseline (last fire / load time) per task, so past cron
    /// occurrences before the agent started are not retro-fired.
    baseline: RwLock<HashMap<String, DateTime<Utc>>>,
    /// SQLite connection (wrapped for Sync; ops are short and local).
    db: Mutex<Connection>,
}

impl Scheduler {
    /// Open (or create) the SQLite store at `path` and load persisted tasks.
    pub fn load(path: PathBuf) -> Self {
        let conn = match open_db(&path) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to open schedule DB ({:?}): {}; using in-memory", path, e);
                Connection::open_in_memory().expect("in-memory sqlite")
            }
        };
        let _ = init_schema(&conn);

        let tasks = load_all(&conn).unwrap_or_else(|e| {
            warn!("Failed to read schedule DB: {}", e);
            HashMap::new()
        });

        Self {
            tasks: RwLock::new(tasks),
            baseline: RwLock::new(HashMap::new()),
            db: Mutex::new(conn),
        }
    }

    /// Validate a cron expression without storing anything.
    fn validate_cron(expr: &str) -> Result<CronSchedule> {
        CronSchedule::from_str(expr)
            .with_context(|| format!("invalid cron expression: '{}'", expr))
    }

    /// Add (or replace) a scheduled task.
    pub async fn add(&self, name: &str, cron: &str, command: &str) -> Result<()> {
        Self::validate_cron(cron)?;

        let task = ScheduledTask {
            name: name.to_string(),
            cron: cron.to_string(),
            command: command.to_string(),
            last_run: None,
            run_count: 0,
        };

        self.db_upsert(&task)?;
        {
            let mut tasks = self.tasks.write().await;
            tasks.insert(name.to_string(), task);
        }
        // Reset baseline to now so the next fire is the upcoming occurrence.
        self.baseline.write().await.insert(name.to_string(), Utc::now());
        info!("Scheduled task '{}' added: {}", name, cron);
        Ok(())
    }

    /// Remove a task by name.
    pub async fn remove(&self, name: &str) -> Result<()> {
        let removed = {
            let mut tasks = self.tasks.write().await;
            tasks.remove(name).is_some()
        };
        if !removed {
            bail!("no scheduled task named '{}'", name);
        }
        self.baseline.write().await.remove(name);
        self.db_delete(name)?;
        info!("Scheduled task '{}' removed", name);
        Ok(())
    }

    /// Snapshot of all scheduled tasks.
    pub async fn list(&self) -> Vec<ScheduledTask> {
        self.tasks.read().await.values().cloned().collect()
    }

    // --- SQLite helpers ----------------------------------------------------

    fn db_upsert(&self, task: &ScheduledTask) -> Result<()> {
        let conn = self.db.lock().unwrap();
        conn.execute(
            "INSERT INTO scheduled_tasks (name, cron, command, last_run, run_count)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(name) DO UPDATE SET
                cron = excluded.cron,
                command = excluded.command,
                last_run = excluded.last_run,
                run_count = excluded.run_count",
            rusqlite::params![
                task.name,
                task.cron,
                task.command,
                task.last_run.map(|v| v as i64),
                task.run_count as i64,
            ],
        )
        .context("upsert scheduled task")?;
        Ok(())
    }

    fn db_delete(&self, name: &str) -> Result<()> {
        let conn = self.db.lock().unwrap();
        conn.execute("DELETE FROM scheduled_tasks WHERE name = ?1", [name])
            .context("delete scheduled task")?;
        Ok(())
    }

    fn db_update_stats(&self, name: &str, last_run: u64, run_count: u64) -> Result<()> {
        let conn = self.db.lock().unwrap();
        conn.execute(
            "UPDATE scheduled_tasks SET last_run = ?2, run_count = ?3 WHERE name = ?1",
            rusqlite::params![name, last_run as i64, run_count as i64],
        )
        .context("update scheduled task stats")?;
        Ok(())
    }

    // --- run loop ----------------------------------------------------------

    /// Run the scheduling loop forever. Spawn this on a background task.
    pub async fn run(self: Arc<Self>) {
        // Seed baselines for tasks loaded from disk.
        {
            let now = Utc::now();
            let tasks = self.tasks.read().await;
            let mut baseline = self.baseline.write().await;
            for name in tasks.keys() {
                baseline.entry(name.clone()).or_insert(now);
            }
        }

        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            self.tick().await;
        }
    }

    /// Evaluate every task once and fire those that are due.
    async fn tick(&self) {
        let now = Utc::now();

        // Determine which tasks are due (collect to avoid holding the lock
        // across command execution).
        let due: Vec<ScheduledTask> = {
            let tasks = self.tasks.read().await;
            let baseline = self.baseline.read().await;
            tasks
                .values()
                .filter(|task| {
                    let after = baseline
                        .get(&task.name)
                        .copied()
                        .or_else(|| task.last_run.and_then(ms_to_dt))
                        .unwrap_or(now);
                    match CronSchedule::from_str(&task.cron) {
                        Ok(sched) => sched
                            .after(&after)
                            .next()
                            .map(|next| next <= now)
                            .unwrap_or(false),
                        Err(_) => false,
                    }
                })
                .cloned()
                .collect()
        };

        if due.is_empty() {
            return;
        }

        for task in due {
            // Advance baseline immediately so we don't double-fire within the
            // same due window.
            self.baseline.write().await.insert(task.name.clone(), now);

            info!("Running scheduled task '{}': {}", task.name, task.command);
            match crate::executor::run_shell(&task.command).await {
                Ok((stdout, stderr, code)) => {
                    debug!(
                        "Task '{}' exited {} ({} bytes out, {} bytes err)",
                        task.name,
                        code,
                        stdout.len(),
                        stderr.len()
                    );
                    if code != 0 {
                        warn!("Task '{}' exited non-zero: {}", task.name, stderr.trim());
                    }
                }
                Err(e) => error!("Task '{}' failed to run: {}", task.name, e),
            }

            // Record run stats in memory + SQLite.
            let new_count = {
                let mut tasks = self.tasks.write().await;
                if let Some(t) = tasks.get_mut(&task.name) {
                    t.last_run = Some(now.timestamp_millis() as u64);
                    t.run_count += 1;
                    t.run_count
                } else {
                    continue; // removed mid-run
                }
            };
            if let Err(e) =
                self.db_update_stats(&task.name, now.timestamp_millis() as u64, new_count)
            {
                warn!("Failed to persist run stats for '{}': {}", task.name, e);
            }
        }
    }
}

fn open_db(path: &PathBuf) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    Connection::open(path).context("open sqlite db")
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS scheduled_tasks (
            name      TEXT PRIMARY KEY,
            cron      TEXT NOT NULL,
            command   TEXT NOT NULL,
            last_run  INTEGER,
            run_count INTEGER NOT NULL DEFAULT 0
        )",
        [],
    )
    .context("create schema")?;
    Ok(())
}

fn load_all(conn: &Connection) -> Result<HashMap<String, ScheduledTask>> {
    let mut stmt =
        conn.prepare("SELECT name, cron, command, last_run, run_count FROM scheduled_tasks")?;
    let rows = stmt.query_map([], |row| {
        Ok(ScheduledTask {
            name: row.get(0)?,
            cron: row.get(1)?,
            command: row.get(2)?,
            last_run: row.get::<_, Option<i64>>(3)?.map(|v| v as u64),
            run_count: row.get::<_, i64>(4)? as u64,
        })
    })?;

    let mut map = HashMap::new();
    for task in rows {
        let task = task?;
        map.insert(task.name.clone(), task);
    }
    Ok(map)
}

fn ms_to_dt(ms: u64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(ms as i64).single()
}

/// Whether `expr` is a valid 6-field cron expression (public for fuzz/tests).
pub fn is_valid_cron(expr: &str) -> bool {
    CronSchedule::from_str(expr).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_cron() {
        assert!(Scheduler::validate_cron("not a cron").is_err());
        assert!(Scheduler::validate_cron("0 0 * * * *").is_ok());
        assert!(Scheduler::validate_cron("*/5 * * * * *").is_ok());
    }

    #[tokio::test]
    async fn add_list_remove_roundtrip() {
        let dir = std::env::temp_dir().join(format!("sched-test-{}", std::process::id()));
        let path = dir.join("schedule.db");
        let sched = Scheduler::load(path.clone());

        sched.add("backup", "0 0 3 * * *", "echo hi").await.unwrap();
        assert_eq!(sched.list().await.len(), 1);

        // Persisted and reloadable from SQLite.
        let reloaded = Scheduler::load(path.clone());
        assert_eq!(reloaded.list().await.len(), 1);
        assert_eq!(reloaded.list().await[0].command, "echo hi");

        sched.remove("backup").await.unwrap();
        assert_eq!(sched.list().await.len(), 0);
        assert!(sched.remove("backup").await.is_err());

        // Removal persisted too.
        let reloaded2 = Scheduler::load(path);
        assert_eq!(reloaded2.list().await.len(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rejects_bad_cron_on_add() {
        let path = std::env::temp_dir().join(format!("sched-bad-{}.db", std::process::id()));
        let sched = Scheduler::load(path.clone());
        assert!(sched.add("x", "garbage", "echo hi").await.is_err());
        let _ = std::fs::remove_file(&path);
    }
}
