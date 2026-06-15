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

/// A single room: connected agents (keyed by agent id) and MCP clients (keyed
/// by session id).
#[derive(Default)]
pub struct Room {
    pub agents: DashMap<String, AgentSession>,
    pub mcp: DashMap<String, McpSession>,
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
}

impl RelayState {
    pub fn new(token: Option<String>) -> Self {
        Self {
            rooms: DashMap::new(),
            token,
        }
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
