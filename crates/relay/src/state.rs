//! Relay registries: rooms and per-connection sessions.
//!
//! Uses sharded concurrent maps (`DashMap`) so there is no global lock on the
//! hot path. Each connection owns a bounded outbound channel; broadcasting is
//! just a non-blocking send into those channels (the socket write happens in a
//! dedicated per-connection writer task), which keeps routing lock-free of I/O.

use dashmap::DashMap;
use remote_agents_shared::AgentInfo;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Outbound channel to a single connection (carries serialized JSON frames).
pub type Tx = mpsc::Sender<String>;

/// Bound on a connection's outbound queue; a consumer that can't keep up is
/// disconnected rather than allowed to grow memory unbounded.
pub const OUTBOUND_CAP: usize = 64;

pub struct AgentSession {
    pub info: AgentInfo,
    pub tx: Tx,
}

pub struct McpSession {
    #[allow(dead_code)]
    pub id: String,
    pub tx: Tx,
}

/// A single room: connected agents and MCP clients, both keyed by **session
/// id** (one entry per live connection). A machine has a stable agent-id but may
/// hold several connections at once (many terminals / AI sessions on the same
/// box); those collapse to one logical peer on read (see `routing::dedup_*`).
#[derive(Default)]
pub struct Room {
    pub agents: DashMap<String, AgentSession>,
    pub mcp: DashMap<String, McpSession>,
    /// In-flight commands: `request_id → originating session id`. Lets a command
    /// result route back to the specific peer that issued it (peer-model: a room
    /// has many potential initiators), instead of broadcasting to all clients.
    pub pending: DashMap<String, String>,
}

impl Room {
    pub fn is_empty(&self) -> bool {
        self.agents.is_empty() && self.mcp.is_empty()
    }
}

/// Global relay state: all rooms plus the optional server auth token.
pub struct RelayState {
    pub rooms: DashMap<String, Arc<Room>>,
    /// When set, every connection's auth token MUST equal this value.
    pub token: Option<String>,
    /// Close a connection that sends no frame within this window. Agents ping
    /// every 30s, so a silent gap means the TCP died without a close (NAT/idle
    /// timeout); reaping it stops a phantom session lingering in the room.
    pub idle_timeout: std::time::Duration,
}

impl RelayState {
    pub fn new(token: Option<String>) -> Self {
        Self {
            rooms: DashMap::new(),
            token,
            idle_timeout: std::time::Duration::from_secs(90),
        }
    }

    /// Override the idle-reaper window (tests use a short value).
    pub fn with_idle_timeout(mut self, d: std::time::Duration) -> Self {
        self.idle_timeout = d;
        self
    }

    /// Set the idle window from a seconds value. `0` disables reaping (the
    /// window becomes effectively infinite), for operators who'd rather rely on
    /// TCP keepalive / a load balancer.
    pub fn with_idle_timeout_secs(self, secs: u64) -> Self {
        // tokio timers accept large durations; ~136 years is "never" in practice.
        let d = std::time::Duration::from_secs(if secs == 0 { u32::MAX as u64 } else { secs });
        self.with_idle_timeout(d)
    }

    /// Get or create a room by name.
    pub fn room(&self, name: &str) -> Arc<Room> {
        self.rooms
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Room::default()))
            .clone()
    }

    /// Drop a room if it has no remaining connections.
    pub fn gc_room(&self, name: &str) {
        self.rooms
            .remove_if(name, |_, room| room.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use remote_agents_shared::AgentMode;

    fn dummy_tx() -> Tx {
        mpsc::channel(OUTBOUND_CAP).0
    }

    fn agent_info(id: &str) -> AgentInfo {
        AgentInfo {
            id: id.to_string(),
            name: "agent".to_string(),
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
            version: String::new(), update_available: None,
        }
    }

    fn add_agent(room: &Room, id: &str) {
        room.agents.insert(
            id.to_string(),
            AgentSession {
                info: agent_info(id),
                tx: dummy_tx(),
            },
        );
    }

    #[test]
    fn idle_timeout_secs_maps_value_and_disables_on_zero() {
        use std::time::Duration;
        assert_eq!(
            RelayState::new(None).with_idle_timeout_secs(45).idle_timeout,
            Duration::from_secs(45)
        );
        // 0 → effectively never (far larger than any real ping gap).
        assert!(
            RelayState::new(None).with_idle_timeout_secs(0).idle_timeout
                >= Duration::from_secs(u32::MAX as u64)
        );
        // Default constructor keeps the 90s window.
        assert_eq!(RelayState::new(None).idle_timeout, Duration::from_secs(90));
    }

    #[test]
    fn room_get_or_create_is_idempotent() {
        let state = RelayState::new(None);
        let a = state.room("gpu");
        let b = state.room("gpu");
        // Same name → same shared Room, not a second instance.
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(state.rooms.len(), 1);

        state.room("other");
        assert_eq!(state.rooms.len(), 2);
    }

    #[test]
    fn room_is_empty_tracks_sessions() {
        let room = Room::default();
        assert!(room.is_empty());

        add_agent(&room, "a1");
        assert!(!room.is_empty());

        // An MCP-only room is also non-empty.
        let room2 = Room::default();
        room2.mcp.insert(
            "s1".to_string(),
            McpSession { id: "s1".to_string(), tx: dummy_tx() },
        );
        assert!(!room2.is_empty());
    }

    #[test]
    fn gc_room_removes_only_empty_rooms() {
        let state = RelayState::new(None);

        // Empty room is collected.
        state.room("empty");
        assert_eq!(state.rooms.len(), 1);
        state.gc_room("empty");
        assert_eq!(state.rooms.len(), 0);

        // Occupied room survives GC.
        let busy = state.room("busy");
        add_agent(&busy, "a1");
        state.gc_room("busy");
        assert_eq!(state.rooms.len(), 1);

        // After its last connection leaves, GC reclaims it.
        busy.agents.remove("a1");
        state.gc_room("busy");
        assert_eq!(state.rooms.len(), 0);
    }
}
