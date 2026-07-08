//! Shared agent runtime state.
//!
//! The operating [`AgentMode`] must be mutable at runtime so that `SetMode`
//! commands take effect, while the rest of the [`Config`] is immutable. This
//! wrapper is cheaply cloneable (`Arc` internals) and shared across the
//! connection, executor and scheduler.

use crate::autonomous::AutonomousStore;
use crate::config::Config;
use crate::scheduler::Scheduler;
use crate::transfer::TransferStore;
use remote_agents_shared::{AgentEvent, AgentInfo, AgentMode, Cipher};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};

#[derive(Clone)]
pub struct AgentState {
    pub config: Arc<Config>,
    /// In-memory view of the operating mode (also a fallback when the shared
    /// mode file is absent/unreadable).
    mode: Arc<RwLock<AgentMode>>,
    /// Path to the machine-wide mode file shared by every session of this box
    /// (see [`crate::config::mode_path`]). `None` for ephemeral, in-memory
    /// states (tests), where mode is purely process-local.
    mode_path: Option<Arc<PathBuf>>,
    scheduler: Arc<Scheduler>,
    autonomous: Arc<AutonomousStore>,
    /// Receiver for outbound agent events; drained by the connection loop and
    /// pushed to the relay. Buffers while offline; survives reconnects.
    events_rx: Arc<Mutex<mpsc::UnboundedReceiver<AgentEvent>>>,
    /// Peer agents currently in the same room (so this host knows "who surrounds
    /// it" — their OS/platform/tags — and can tailor tasks accordingly).
    /// Maintained from the relay's AgentList/AgentJoined/AgentLeft messages.
    peers: Arc<RwLock<Vec<AgentInfo>>>,
    /// Progress registry for host↔host transfers this node initiated.
    transfers: Arc<TransferStore>,
    /// Registry of Cloudflare quick tunnels this node started.
    tunnels: Arc<crate::tunnel::TunnelStore>,
    /// Sender for info update notifications (e.g. mode change); connection loop
    /// sends UpdateAgent to relay when it receives from this channel.
    info_update_tx: mpsc::UnboundedSender<()>,
    /// Receiver for info update notifications.
    info_update_rx: Arc<Mutex<mpsc::UnboundedReceiver<()>>>,
}

impl AgentState {
    /// An ephemeral, in-memory state: mode is process-local (used by tests and
    /// any short-lived state that must not touch the shared machine mode file).
    pub fn new(config: Config) -> Self {
        Self::build(config, None)
    }

    /// A persistent state whose operating mode is shared machine-wide via a file
    /// next to the persisted `agent-id` (see [`crate::config::mode_path`]). Every
    /// `remote-agent` session on the same box reads/writes it, so a `set_mode` on
    /// one session is honored by all — the fix for a peer command (e.g. a file
    /// `FileRecv`) that the relay routes to a *different* session of the same
    /// machine than the one the operator set to `edit`. Used by the real agent
    /// entry points (`connection::run`, the MCP peer).
    pub fn new_persistent(config: Config) -> Self {
        let path = crate::config::mode_path();
        Self::build(config, Some(Arc::new(path)))
    }

    /// Test-only: a state whose machine-wide mode file lives at `path`, so a
    /// test can exercise cross-session sharing without touching the real
    /// `~/.local/share` mode file.
    #[cfg(test)]
    pub(crate) fn with_mode_file(config: Config, path: PathBuf) -> Self {
        Self::build(config, Some(Arc::new(path)))
    }

    fn build(config: Config, mode_path: Option<Arc<PathBuf>>) -> Self {
        // A mode file present on disk is authoritative for the box (it reflects
        // the latest `set_mode` by any session); otherwise fall back to config.
        let mode = mode_path
            .as_ref()
            .and_then(|p| read_mode_file(p))
            .unwrap_or(config.security.mode);
        let scheduler = Arc::new(Scheduler::load(schedule_path()));
        let (events_tx, events_rx) = mpsc::unbounded_channel::<AgentEvent>();
        let (info_update_tx, info_update_rx) = mpsc::unbounded_channel::<()>();
        let autonomous = Arc::new(AutonomousStore::load(
            tasks_path(),
            config.autonomous.clone(),
            events_tx,
        ));
        Self {
            config: Arc::new(config),
            mode: Arc::new(RwLock::new(mode)),
            mode_path,
            scheduler,
            autonomous,
            events_rx: Arc::new(Mutex::new(events_rx)),
            peers: Arc::new(RwLock::new(Vec::new())),
            transfers: Arc::new(TransferStore::default()),
            tunnels: Arc::new(crate::tunnel::TunnelStore::default()),
            info_update_tx,
            info_update_rx: Arc::new(Mutex::new(info_update_rx)),
        }
    }

    /// The shared host↔host transfer registry.
    pub fn transfers(&self) -> Arc<TransferStore> {
        self.transfers.clone()
    }

    /// The shared Cloudflare quick-tunnel registry.
    pub fn tunnels(&self) -> Arc<crate::tunnel::TunnelStore> {
        self.tunnels.clone()
    }

    /// Snapshot of the peer agents currently known to share this room.
    pub async fn peers(&self) -> Vec<AgentInfo> {
        self.peers.read().await.clone()
    }

    /// Replace the full peer set (from a relay `AgentList`).
    pub async fn set_peers(&self, peers: Vec<AgentInfo>) {
        *self.peers.write().await = peers;
    }

    /// Add or update one peer (from `AgentJoined`), keyed by agent id.
    pub async fn upsert_peer(&self, peer: AgentInfo) {
        let mut peers = self.peers.write().await;
        upsert_peer_in(&mut peers, peer);
    }

    /// Drop one peer by id (from `AgentLeft`).
    pub async fn remove_peer(&self, agent_id: &str) {
        let mut peers = self.peers.write().await;
        remove_peer_in(&mut peers, agent_id);
    }

    /// Receive the next outbound event (used by the connection loop).
    pub async fn next_event(&self) -> Option<AgentEvent> {
        self.events_rx.lock().await.recv().await
    }

    /// The shared autonomous task store/runner.
    pub fn autonomous(&self) -> Arc<AutonomousStore> {
        self.autonomous.clone()
    }

    /// Current operating mode. For a persistent state this re-reads the shared
    /// machine-wide mode file so a `set_mode` performed by *another* session of
    /// the same box is honored (e.g. an incoming `FileRecv` the relay routed
    /// here rather than to the session the operator set to `edit`). Falls back
    /// to the in-memory view if the file is absent/unreadable.
    pub async fn mode(&self) -> AgentMode {
        if let Some(path) = &self.mode_path {
            if let Some(m) = read_mode_file(path) {
                *self.mode.write().await = m;
                return m;
            }
        }
        *self.mode.read().await
    }

    /// Update the operating mode and notify the connection loop to send
    /// an UpdateAgent message to the relay. For a persistent state this also
    /// writes the shared machine-wide mode file so sibling sessions converge.
    pub async fn set_mode(&self, mode: AgentMode) {
        if let Some(path) = &self.mode_path {
            write_mode_file(path, mode);
        }
        *self.mode.write().await = mode;
        // Notify connection loop to send UpdateAgent; ignore send errors
        // (e.g. if not connected to relay).
        let _ = self.info_update_tx.send(());
    }

    /// Take the info update receiver (called once by connection loop).
    pub async fn take_info_update_rx(&self) -> mpsc::UnboundedReceiver<()> {
        // Replace with a dummy channel; the real one is taken by connection.
        let (_, dummy_rx) = mpsc::unbounded_channel::<()>();
        std::mem::replace(&mut *self.info_update_rx.lock().await, dummy_rx)
    }

    /// The shared scheduler.
    pub fn scheduler(&self) -> Arc<Scheduler> {
        self.scheduler.clone()
    }

    /// The mandatory end-to-end transport cipher. Derived from the room token
    /// by default, or from `security.encryption_key` when set.
    pub fn cipher(&self) -> Cipher {
        Cipher::for_transport(
            &self.config.token,
            self.config.security.encryption_key.as_deref(),
        )
    }

    /// Spawn the scheduler's background loop.
    pub fn start_scheduler(&self) {
        let scheduler = self.scheduler.clone();
        tokio::spawn(async move { scheduler.run().await });
    }
}

/// Insert `peer`, or replace the existing entry with the same id (last write
/// wins). Pure (operates on the locked vec) so the upsert semantics are
/// unit-testable without a live relay or disk-backed `AgentState`.
fn upsert_peer_in(peers: &mut Vec<AgentInfo>, peer: AgentInfo) {
    if let Some(slot) = peers.iter_mut().find(|p| p.id == peer.id) {
        *slot = peer;
    } else {
        peers.push(peer);
    }
}

/// Remove every peer whose id matches `agent_id`.
fn remove_peer_in(peers: &mut Vec<AgentInfo>, agent_id: &str) {
    peers.retain(|p| p.id != agent_id);
}

/// Path to the persisted schedule database (SQLite).
fn schedule_path() -> PathBuf {
    dirs::data_dir()
        .map(|p| p.join("remote-agents").join("schedule.db"))
        .unwrap_or_else(|| PathBuf::from("schedule.db"))
}

/// Path to the autonomous task database (SQLite).
fn tasks_path() -> PathBuf {
    dirs::data_dir()
        .map(|p| p.join("remote-agents").join("tasks.db"))
        .unwrap_or_else(|| PathBuf::from("tasks.db"))
}

/// Parse the one-word mode file body into an [`AgentMode`] (matches the serde
/// `rename_all = "lowercase"` on the type). Unknown/garbage ⇒ `None`.
fn parse_mode(s: &str) -> Option<AgentMode> {
    match s.trim() {
        "plan" => Some(AgentMode::Plan),
        "edit" => Some(AgentMode::Edit),
        "bypass" => Some(AgentMode::Bypass),
        "disabled" => Some(AgentMode::Disabled),
        _ => None,
    }
}

/// The lowercase token written to the shared mode file for `mode`.
fn mode_token(mode: AgentMode) -> &'static str {
    match mode {
        AgentMode::Plan => "plan",
        AgentMode::Edit => "edit",
        AgentMode::Bypass => "bypass",
        AgentMode::Disabled => "disabled",
    }
}

/// Read the machine-wide mode file, returning `None` if it is absent or holds
/// an unrecognized value (caller then falls back to config/in-memory).
fn read_mode_file(path: &std::path::Path) -> Option<AgentMode> {
    std::fs::read_to_string(path).ok().as_deref().and_then(parse_mode)
}

/// Write `mode` to the machine-wide mode file atomically (tmp + rename) so a
/// concurrent reader in a sibling process never sees a torn value. Best-effort:
/// a write failure just means siblings keep their current view. The tmp file is
/// PID-suffixed so two processes writing at once don't clobber each other's tmp.
fn write_mode_file(path: &std::path::Path, mode: AgentMode) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    if std::fs::write(&tmp, mode_token(mode)).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: &str, name: &str) -> AgentInfo {
        AgentInfo {
            id: id.to_string(),
            name: name.to_string(),
            mode: AgentMode::Edit,
            os: "linux".to_string(),
            arch: "x86_64".to_string(),
            hostname: "host".to_string(),
            tags: vec![],
            platform: Default::default(),
            autonomous: false,
            accepts_commands: true,
            connected_at: 0,
            session_id: None,
            version: String::new(), update_available: None, connections: None,
        }
    }

    #[test]
    fn upsert_peer_inserts_new_then_updates_in_place() {
        let mut peers = Vec::new();

        upsert_peer_in(&mut peers, peer("a", "alice"));
        upsert_peer_in(&mut peers, peer("b", "bob"));
        assert_eq!(peers.len(), 2);

        // Same id → replace in place (no duplicate, order preserved).
        upsert_peer_in(&mut peers, peer("a", "alice-renamed"));
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].id, "a");
        assert_eq!(peers[0].name, "alice-renamed");
        assert_eq!(peers[1].name, "bob");
    }

    #[test]
    fn remove_peer_drops_only_matching_id() {
        let mut peers = vec![peer("a", "alice"), peer("b", "bob")];

        remove_peer_in(&mut peers, "a");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, "b");

        // Removing an unknown id is a no-op.
        remove_peer_in(&mut peers, "zzz");
        assert_eq!(peers.len(), 1);
    }

    #[test]
    fn mode_file_round_trips_every_variant() {
        for m in [
            AgentMode::Plan,
            AgentMode::Edit,
            AgentMode::Bypass,
            AgentMode::Disabled,
        ] {
            assert_eq!(parse_mode(mode_token(m)), Some(m));
        }
        assert_eq!(parse_mode("  edit\n"), Some(AgentMode::Edit));
        assert_eq!(parse_mode("garbage"), None);
    }

    // The core of the machine-wide-mode fix: two sessions of the same box share
    // one mode file, so a `set_mode` on one is seen by `mode()` on the other —
    // exactly the case where the relay routes a peer command (e.g. `FileRecv`)
    // to a *different* session than the one the operator set to `edit`.
    #[tokio::test]
    async fn set_mode_is_shared_across_sessions_via_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mode");

        let session_a = AgentState::with_mode_file(Config::default(), path.clone());
        let session_b = AgentState::with_mode_file(Config::default(), path.clone());

        // Both start at the config default (plan); no file yet.
        assert_eq!(session_a.mode().await, AgentMode::Plan);
        assert_eq!(session_b.mode().await, AgentMode::Plan);

        // Operator flips session A to edit; B must observe it (it re-reads the
        // shared file), and the file holds the token.
        session_a.set_mode(AgentMode::Edit).await;
        assert_eq!(session_b.mode().await, AgentMode::Edit);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "edit");

        // A brand-new session started afterwards reads the current machine mode.
        let session_c = AgentState::with_mode_file(Config::default(), path.clone());
        assert_eq!(session_c.mode().await, AgentMode::Edit);
    }

    // An ephemeral (in-memory) state must NEVER touch a shared file — its mode
    // is process-local, guaranteeing tests can't clobber the real machine mode.
    #[tokio::test]
    async fn ephemeral_state_is_process_local() {
        let a = AgentState::new(Config::default());
        let b = AgentState::new(Config::default());
        a.set_mode(AgentMode::Bypass).await;
        assert_eq!(a.mode().await, AgentMode::Bypass);
        assert_eq!(b.mode().await, AgentMode::Plan); // unaffected
    }
}
