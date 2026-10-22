//! Tiny shared SQLite helpers (open with parent-dir creation). Previously
//! triplicated as private `open_db` in autonomous.rs, scheduler.rs, chains.rs.

use anyhow::{Context, Result};
use rusqlite::Connection;

/// Open (or create) a SQLite database, creating parent directories first.
pub(crate) fn open_db(path: &std::path::Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    Connection::open(path).with_context(|| format!("open sqlite db {:?}", path))
}
