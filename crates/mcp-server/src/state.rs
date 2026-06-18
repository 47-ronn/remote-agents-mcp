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
    mode: Arc<RwLock<AgentMode>>,
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
}

impl AgentState {
    pub fn new(config: Config) -> Self {
        let mode = config.security.mode;
        let scheduler = Arc::new(Scheduler::load(schedule_path()));
        let (events_tx, events_rx) = mpsc::unbounded_channel::<AgentEvent>();
        let autonomous = Arc::new(AutonomousStore::load(
            tasks_path(),
            config.autonomous.clone(),
            events_tx,
        ));
        Self {
            config: Arc::new(config),
            mode: Arc::new(RwLock::new(mode)),
            scheduler,
            autonomous,
            events_rx: Arc::new(Mutex::new(events_rx)),
            peers: Arc::new(RwLock::new(Vec::new())),
            transfers: Arc::new(TransferStore::default()),
            tunnels: Arc::new(crate::tunnel::TunnelStore::default()),
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

    /// Current operating mode.
    pub async fn mode(&self) -> AgentMode {
        *self.mode.read().await
    }

    /// Update the operating mode.
    pub async fn set_mode(&self, mode: AgentMode) {
        *self.mode.write().await = mode;
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
}
