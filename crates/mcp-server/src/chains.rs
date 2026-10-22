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
//!   idempotent (the owner marks `fired` before dispatching, replicas follow
//!   via ChainArm state updates).
//!
//! Deliberately simple: no Raft, no leases. Conflicts are bounded by
//! "fire at most once per chain_id" on the destination side and by the
//! majority-ack arming, which guarantees at least one live copy while a
//! majority of autonomous hosts is up.
//!
//! State machine (stringly on the wire/SQLite, ranked here): armed → fired →
//! done, any → cancelled. `done` is not set anywhere yet — the follow-up's
//! completion doesn't flow back; see the transition matrix in tests.

use anyhow::{Context, Result};
use remote_agents_shared::TaskChain;
use rusqlite::{Connection, OptionalExtension};
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::warn;

/// Column list shared by every SELECT — positional order for `row_to_chain`.
const CHAIN_COLS: &str =
    "chain_id, src_agent, src_task, dst_agent, prompt, owner, state, fired_task, note, created_at";

/// Monotonic rank of a chain state. Terminal states share the top rank so a
/// `done` copy can replace a `cancelled` one (and vice versa) — both mean
/// "no further transitions possible". Unknown/garbage states rank LOWEST so a
/// corrupt replica copy can never overwrite a live state.
fn state_rank(s: &str) -> i64 {
    match s {
        "armed" => 0,
        "fired" => 1,
        "done" | "cancelled" => 2,
        _ => i64::MIN,
    }
}

pub struct ChainStore {
    db: Mutex<Connection>,
}

impl ChainStore {
    /// Open (or create) the chain store next to the autonomous tasks DB.
    pub fn load(path: PathBuf) -> Self {
        let conn = match crate::db_util::open_db(&path) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to open chains DB ({:?}): {}; using in-memory", path, e);
                Connection::open_in_memory().expect("in-memory sqlite")
            }
        };
        if let Err(e) = init_schema(&conn) {
            warn!("chain schema init failed: {e}");
        }
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
    /// stored AFTER the call (for a stale incoming copy that keeps the local
    /// state, the local `fired_task` is re-read, not assumed).
    pub fn upsert(&self, incoming: &TaskChain) -> Result<TaskChain> {
        {
            let conn = self.db.lock().unwrap();
            let existing: Option<String> = conn
                .query_row(
                    "SELECT state FROM task_chains WHERE chain_id = ?1",
                    [&incoming.chain_id],
                    |r| r.get(0),
                )
                .optional()?;
            let local_state = existing.clone();

            let take_incoming =
                existing.is_none_or(|local| state_rank(&incoming.state) >= state_rank(&local));

            let effective = if take_incoming {
                incoming.clone()
            } else {
                // Incoming is STALE (earlier state than local): keep the local
                // state, still refresh descriptive fields (prompt/owner/note).
                let mut kept = incoming.clone();
                kept.state = local_state.unwrap_or_default();
                kept
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
        }
        // The row's fired_task is authoritative (a stale replay must not clear
        // it), so report what's actually stored.
        self.get(&incoming.chain_id)?
            .context("chain vanished during upsert")
    }

    /// All chains stored here (owner copies AND replicas).
    pub fn list(&self) -> Result<Vec<TaskChain>> {
        self.query_many(&format!(
            "SELECT {CHAIN_COLS} FROM task_chains ORDER BY created_at DESC"
        ), [])
    }

    /// Chains this host OWNS and that are still armed (waiting to fire).
    pub fn armed_owned(&self, owner: &str) -> Result<Vec<TaskChain>> {
        self.query_many(
            &format!("SELECT {CHAIN_COLS} FROM task_chains WHERE owner = ?1 AND state = 'armed'"),
            [owner],
        )
    }

    pub fn get(&self, chain_id: &str) -> Result<Option<TaskChain>> {
        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {CHAIN_COLS} FROM task_chains WHERE chain_id = ?1"
        ))?;
        let mut rows = stmt.query_map([chain_id], row_to_chain)?;
        match rows.next() {
            Some(c) => Ok(Some(c?)),
            None => Ok(None),
        }
    }

    fn query_many<P: rusqlite::Params>(&self, sql: &str, params: P) -> Result<Vec<TaskChain>> {
        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map(params, row_to_chain)?;
        let mut out = Vec::new();
        for c in rows {
            out.push(c?);
        }
        Ok(out)
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

    /// Fire a chain in one atomic statement: claim `armed → fired` AND record
    /// the follow-up task id together, so a crash between the two writes can
    /// never leave a `fired` row without its `fired_task`. Returns false if
    /// the chain wasn't armed (someone else fired it first).
    pub fn fire(&self, chain_id: &str, fired_task: &str) -> Result<bool> {
        let conn = self.db.lock().unwrap();
        let n = conn.execute(
            "UPDATE task_chains SET state = 'fired', fired_task = ?2 WHERE chain_id = ?1 AND state = 'armed'",
            rusqlite::params![chain_id, fired_task],
        )?;
        Ok(n == 1)
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
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_chains_owner_state ON task_chains(owner, state)",
        [],
    )
    .context("create task_chains index")?;
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
        ctx = crate::sessions::truncate(ctx, 4000),
        prompt = chain.prompt,
    )
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

        // Fire claims armed -> fired exactly once and records the follow-up
        // task atomically (transition() alone would not set fired_task).
        assert!(store.fire("c1", "ft-1").unwrap());
        assert!(!store.fire("c1", "ft-2").unwrap());
        assert_eq!(store.armed_owned("a").unwrap().len(), 0);

        // A STALE replica replay (`armed` copy arriving after the chain fired
        // locally) must NOT resurrect the fired chain — and must not clear the
        // recorded fired_task either.
        let mut stale = chain("armed");
        stale.fired_task = None;
        let stored = store.upsert(&stale).unwrap();
        assert_eq!(stored.state, "fired");
        assert_eq!(stored.fired_task.as_deref(), Some("ft-1"));
    }

    #[test]
    fn fire_claims_armed_and_records_task_atomically() {
        let dir = std::env::temp_dir().join(format!("chains-{}-{}", std::process::id(), now_ns()));
        let store = ChainStore::load(dir.join("c.db"));
        store.upsert(&chain("armed")).unwrap();
        assert!(store.fire("c1", "ft-9").unwrap());
        // Second fire loses the race — no overwrite.
        assert!(!store.fire("c1", "ft-10").unwrap());
        let c = store.get("c1").unwrap().unwrap();
        assert_eq!(c.state, "fired");
        assert_eq!(c.fired_task.as_deref(), Some("ft-9"));
    }

    #[test]
    fn garbage_state_never_overwrites_live_state() {
        let dir = std::env::temp_dir().join(format!("chains-{}-{}", std::process::id(), now_ns()));
        let store = ChainStore::load(dir.join("c.db"));
        store.upsert(&chain("armed")).unwrap();
        // A corrupt replica copy with an unknown state must not rank above
        // (or below) anything — the local state wins.
        store.upsert(&chain("FIRE")).unwrap();
        assert_eq!(store.get("c1").unwrap().unwrap().state, "armed");
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
