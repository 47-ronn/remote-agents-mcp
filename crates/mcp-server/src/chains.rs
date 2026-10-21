//! Replicated task chains: "when task `src_task` finishes, dispatch a
//! follow-up to `dst_agent`".
//!
//! Ownership & consensus model:
//! - The host that arms a chain is its OWNER. It persists the chain locally
//!   (SQLite, survives restarts) and replicates it to autonomous peers with
//!   `ChainArm`; consensus = majority of the autonomous peers the owner could
//!   reach (owner counts itself; a lone host accepts with quorum 1).
//! - The owner watches the source task (its own task store, or TaskGet on the
//!   source host) and fires the follow-up with the source result as context.
//! - Replicas hold the chain so a restarted owner re-adopts it on startup
//!   (re-arm from replicas) — and as a safety net, any replica noticing the
//!   source finished while the chain is still `armed` may fire it; firing is
//!   idempotent (the destination dedups by chain_id via the prompt marker and
//!   the owner marks `fired` before dispatching, replicas follow via ChainArm
//!   state updates).
//!
//! Deliberately simple: no Raft, no leases. Conflicts are bounded by
//! "fire at most once per chain_id" on the destination side and by the
//! majority-ack arming, which guarantees at least one live copy while a
//! majority of autonomous hosts is up.

use anyhow::{Context, Result};
use remote_agents_shared::TaskChain;
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::warn;

pub struct ChainStore {
    db: Mutex<Connection>,
}

impl ChainStore {
    /// Open (or create) the chain store next to the autonomous tasks DB.
    pub fn load(path: PathBuf) -> Self {
        let conn = match open_db(&path) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to open chains DB ({:?}): {}; using in-memory", path, e);
                Connection::open_in_memory().expect("in-memory sqlite")
            }
        };
        let _ = init_schema(&conn);
        Self { db: Mutex::new(conn) }
    }

    /// Insert or update a chain (upsert by chain_id) — used by arming,
    /// replication, and state transitions.
    ///
    /// State monotonicity: a stale replica replay (e.g. an `armed` copy
    /// arriving after the chain already `fired` locally) must NOT resurrect
    /// the earlier state. Allowed transitions: a new row takes anything; an
    /// existing row only moves to the SAME or a LATER state, or updates
    /// non-state fields while keeping the local state. Returns the chain as
    /// stored AFTER the call.
    pub fn upsert(&self, incoming: &TaskChain) -> Result<TaskChain> {
        let conn = self.db.lock().unwrap();
        let existing: Option<(String,)> = conn
            .query_row(
                "SELECT state FROM task_chains WHERE chain_id = ?1",
                [&incoming.chain_id],
                |r| Ok((r.get::<_, String>(0)?,)),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(other),
            })?;

        let rank = |s: &str| -> i64 {
            match s {
                "armed" => 0,
                "fired" => 1,
                "done" | "cancelled" => 2,
                _ => 0,
            }
        };

        let effective = match &existing {
            None => incoming.clone(),
            Some((local_state,)) if rank(&incoming.state) >= rank(local_state) => incoming.clone(),
            // Incoming is STALE (earlier state than local): keep local state,
            // still refresh descriptive fields (prompt/owner/note may update).
            Some((local_state,)) => {
                let mut kept = incoming.clone();
                kept.state = local_state.clone();
                kept.fired_task = None; // local row's fired_task stays authoritative
                kept
            }
        };

        conn.execute(
            "INSERT INTO task_chains (chain_id, src_agent, src_task, dst_agent, prompt, owner, state, fired_task, note, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(chain_id) DO UPDATE SET
               src_agent = excluded.src_agent,
               src_task = excluded.src_task,
               dst_agent = excluded.dst_agent,
               prompt = excluded.prompt,
               owner = excluded.owner,
               state = excluded.state,
               note = COALESCE(excluded.note, task_chains.note)",
            rusqlite::params![
                effective.chain_id,
                effective.src_agent,
                effective.src_task,
                effective.dst_agent,
                effective.prompt,
                effective.owner,
                effective.state,
                effective.fired_task,
                effective.note,
                effective.created_at as i64,
            ],
        )
        .context("upsert task chain")?;
        Ok(effective)
    }

    /// All chains stored here (owner copies AND replicas).
    pub fn list(&self) -> Result<Vec<TaskChain>> {
        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT chain_id, src_agent, src_task, dst_agent, prompt, owner, state, fired_task, note, created_at
             FROM task_chains ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map([], row_to_chain)?;
        let mut out = Vec::new();
        for c in rows {
            out.push(c?);
        }
        Ok(out)
    }

    /// Chains this host OWNS and that are still armed (waiting to fire).
    pub fn armed_owned(&self, owner: &str) -> Result<Vec<TaskChain>> {
        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT chain_id, src_agent, src_task, dst_agent, prompt, owner, state, fired_task, note, created_at
             FROM task_chains WHERE owner = ?1 AND state = 'armed'",
        )?;
        let rows = stmt.query_map([owner], row_to_chain)?;
        let mut out = Vec::new();
        for c in rows {
            out.push(c?);
        }
        Ok(out)
    }

    pub fn get(&self, chain_id: &str) -> Result<Option<TaskChain>> {
        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT chain_id, src_agent, src_task, dst_agent, prompt, owner, state, fired_task, note, created_at
             FROM task_chains WHERE chain_id = ?1",
        )?;
        let mut rows = stmt.query_map([chain_id], row_to_chain)?;
        match rows.next() {
            Some(c) => Ok(Some(c?)),
            None => Ok(None),
        }
    }

    /// Transition a chain's state (armed → fired → done, or → cancelled).
    /// Returns false if the chain wasn't in `from` (e.g. already fired) —
    /// callers use this for idempotent firing.
    pub fn transition(&self, chain_id: &str, from: &str, to: &str) -> Result<bool> {
        let conn = self.db.lock().unwrap();
        let n = conn.execute(
            "UPDATE task_chains SET state = ?2 WHERE chain_id = ?1 AND state = ?3",
            rusqlite::params![chain_id, to, from],
        )?;
        Ok(n == 1)
    }

    /// Set the fired follow-up task id (after a successful dispatch).
    pub fn set_fired_task(&self, chain_id: &str, task_id: &str) -> Result<()> {
        let conn = self.db.lock().unwrap();
        conn.execute(
            "UPDATE task_chains SET fired_task = ?2 WHERE chain_id = ?1",
            rusqlite::params![chain_id, task_id],
        )?;
        Ok(())
    }

    /// Record a note (consensus report, firing error, …).
    pub fn set_note(&self, chain_id: &str, note: &str) -> Result<()> {
        let conn = self.db.lock().unwrap();
        conn.execute(
            "UPDATE task_chains SET note = ?2 WHERE chain_id = ?1",
            rusqlite::params![chain_id, note],
        )?;
        Ok(())
    }
}

fn row_to_chain(row: &rusqlite::Row) -> rusqlite::Result<TaskChain> {
    Ok(TaskChain {
        chain_id: row.get(0)?,
        src_agent: row.get(1)?,
        src_task: row.get(2)?,
        dst_agent: row.get(3)?,
        prompt: row.get(4)?,
        owner: row.get(5)?,
        state: row.get(6)?,
        fired_task: row.get(7)?,
        note: row.get(8)?,
        created_at: row.get::<_, i64>(9)? as u64,
    })
}

fn open_db(path: &std::path::Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    Connection::open(path).with_context(|| format!("open chain db {:?}", path))
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS task_chains (
            chain_id    TEXT PRIMARY KEY,
            src_agent   TEXT NOT NULL,
            src_task    TEXT NOT NULL,
            dst_agent   TEXT NOT NULL,
            prompt      TEXT NOT NULL,
            owner       TEXT NOT NULL,
            state       TEXT NOT NULL,
            fired_task  TEXT,
            note        TEXT,
            created_at  INTEGER NOT NULL
        )",
        [],
    )
    .context("create task_chains schema")?;
    Ok(())
}

/// The prompt actually sent to the destination when a chain fires: the source
/// task's terminal status and output as context, then the follow-up prompt.
/// A stable `chain_id` marker line lets anything downstream attribute the task.
pub fn follow_up_prompt(chain: &TaskChain, src_status: &str, src_output: &str) -> String {
    let ctx = src_output.trim();
    let ctx = if ctx.is_empty() { "(no output)" } else { ctx };
    format!(
        "[chain {id} | {src}#{task} finished: {status}]\n--- source result ---\n{ctx}\n--- end ---\n\n{prompt}",
        id = chain.chain_id,
        src = chain.src_agent,
        task = chain.src_task,
        status = src_status,
        ctx = truncate(ctx, 4000),
        prompt = chain.prompt,
    )
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        s.chars().take(n).collect::<String>() + "…"
    }
}

/// Count consensus: `acks` includes the owner itself (it persists locally
/// before replicating). Consensus = majority of the candidates contacted
/// (autonomous peers + owner); with no peers, a single-host quorum stands.
pub fn consensus_met(candidates: usize, acks: usize) -> bool {
    if candidates == 0 {
        return true; // lone host: owner-only quorum
    }
    acks * 2 > candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(state: &str) -> TaskChain {
        TaskChain {
            chain_id: "c1".into(),
            src_agent: "a".into(),
            src_task: "t1".into(),
            dst_agent: "b".into(),
            prompt: "go".into(),
            owner: "a".into(),
            state: state.into(),
            fired_task: None,
            note: None,
            created_at: 42,
        }
    }

    #[test]
    fn upsert_list_transition_roundtrip() {
        let dir = std::env::temp_dir().join(format!("chains-{}-{}", std::process::id(), now_ns()));
        let store = ChainStore::load(dir.join("c.db"));
        store.upsert(&chain("armed")).unwrap();
        assert_eq!(store.list().unwrap().len(), 1);
        assert_eq!(store.armed_owned("a").unwrap().len(), 1);
        assert_eq!(store.armed_owned("b").unwrap().len(), 0);

        // Idempotent transition: armed -> fired exactly once.
        assert!(store.transition("c1", "armed", "fired").unwrap());
        assert!(!store.transition("c1", "armed", "fired").unwrap());
        assert_eq!(store.armed_owned("a").unwrap().len(), 0);

        // A STALE replica replay (`armed` copy arriving after the chain fired
        // locally) must NOT resurrect the fired chain.
        store.upsert(&chain("armed")).unwrap();
        assert_eq!(store.get("c1").unwrap().unwrap().state, "fired");
    }

    #[test]
    fn follow_up_prompt_contains_marker_and_context() {
        let p = follow_up_prompt(&chain("armed"), "done", "build ok");
        assert!(p.contains("[chain c1 |"));
        assert!(p.contains("build ok"));
        assert!(p.trim_end_matches('\n').ends_with("go"));
    }

    #[test]
    fn consensus_requires_majority() {
        assert!(consensus_met(0, 1)); // lone host
        assert!(consensus_met(3, 2)); // 2/3
        assert!(!consensus_met(3, 1)); // 1/3
        assert!(consensus_met(4, 3)); // 3/4
        assert!(!consensus_met(4, 2)); // 2/4
    }

    fn now_ns() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    }
}
