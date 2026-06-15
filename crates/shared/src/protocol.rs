//! WebSocket message protocol between clients and relay server.
//!
//! Command payloads and results are end-to-end encrypted: the `payload` /
//! `result` fields carry an opaque base64 ciphertext ([`Command::encrypt`] /
//! [`CommandResult::encrypt`]) so the relay forwards them blind. Only routing
//! metadata (`request_id`, `target`, `agent_id`) and error strings travel in
//! the clear.

use serde::{Deserialize, Serialize};
use crate::crypto::{Cipher, CryptoError};
use crate::types::*;
use crate::udp::{UdpAnswer, UdpChannelResult, UdpOffer};

/// Error wrapping (de)serialization + (de)cryption of an encrypted payload.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

// ============================================================================
// Client → Relay Messages
// ============================================================================

/// Messages sent from clients (MCP or Agent) to the relay server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Authenticate with the relay
    Auth {
        room: String,
        token: String,
        role: ClientRole,
        #[serde(skip_serializing_if = "Option::is_none")]
        // Boxed: `AgentInfo` is the largest payload across all ClientMessage
        // variants; boxing keeps the common (command/result/ping) messages small.
        agent_info: Option<Box<AgentInfo>>,
    },

    /// Request list of agents in the room (MCP only)
    ListAgents,

    /// Send a command to agent(s) (MCP only). `payload` is an encrypted
    /// [`Command`] envelope (see [`Command::encrypt`]).
    Command {
        request_id: String,
        target: Target,
        payload: String,
    },

    /// Command result from agent (Agent only). `result` is an encrypted
    /// [`CommandResult`] envelope (see [`CommandResult::encrypt`]).
    CommandResult {
        request_id: String,
        result: String,
    },

    /// Command error from agent (Agent only)
    CommandError {
        request_id: String,
        error: String,
    },

    /// Unsolicited event from an agent (e.g. autonomous task finished)
    Notify {
        event: AgentEvent,
    },

    // ========================================================================
    // UDP Signaling Messages
    // ========================================================================

    /// Offer to establish a direct UDP channel with another peer
    UdpOffer(UdpOffer),

    /// Answer to a UDP channel offer
    UdpAnswer(UdpAnswer),

    /// Report UDP channel establishment result
    UdpResult(UdpChannelResult),

    /// Heartbeat
    Ping,

    /// Graceful disconnect
    Close,
}

/// Commands that can be executed on agents
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    /// Execute a shell command
    Exec {
        command: String,
        #[serde(default)]
        timeout_ms: Option<u64>,
        /// Working directory
        #[serde(skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
    },

    /// Read a file
    ReadFile {
        path: String,
    },

    /// Write a file
    WriteFile {
        path: String,
        content: String,
        /// Create backup before writing (when mode=Edit)
        #[serde(default = "default_true")]
        create_backup: bool,
    },

    /// List directory contents
    ListDir {
        path: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pattern: Option<String>,
    },

    /// Get git status
    GitStatus {
        repo: String,
    },

    /// Git pull
    GitPull {
        repo: String,
        #[serde(default = "default_origin")]
        remote: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
    },

    /// Git commit
    GitCommit {
        repo: String,
        message: String,
        files: Vec<String>,
    },

    /// Git push
    GitPush {
        repo: String,
        #[serde(default = "default_origin")]
        remote: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
    },

    /// Add a scheduled task (runs a shell command on a cron schedule)
    ScheduleAdd {
        /// Unique task name
        name: String,
        /// Cron expression (6-field: sec min hour day month weekday)
        cron: String,
        /// Shell command to run
        command: String,
    },

    /// Remove a scheduled task by name
    ScheduleRemove {
        name: String,
    },

    /// List scheduled tasks
    ScheduleList,

    /// Dispatch an autonomous AI task to the host (runs with the host's own
    /// credentials). Returns immediately with a task id.
    TaskDispatch {
        prompt: String,
    },

    /// Get a single autonomous task by id (status + result)
    TaskGet {
        id: String,
    },

    /// List autonomous tasks on the host
    TaskList,

    /// Change agent mode
    SetMode {
        mode: AgentMode,
    },

    /// Get agent info
    GetInfo,

    // === Distributed compute (MapReduce, Phase 13) ===
    /// Run a map function over one partition of a job's input data.
    MapTask {
        /// Identifies the overall job this partition belongs to.
        job_id: String,
        /// 0-based index of this partition within the job.
        partition_id: u32,
        /// Map function source, evaluated by the agent's compute runtime.
        map_fn: String,
        /// Partition input. Opaque to the protocol; `map_fn` interprets it.
        data: String,
    },

    /// Run a reduce function over collected map outputs for a job.
    ReduceTask {
        job_id: String,
        /// Reduce function source.
        reduce_fn: String,
        /// Map outputs to fold together.
        inputs: Vec<String>,
    },
}

fn default_true() -> bool { true }
fn default_origin() -> String { "origin".to_string() }

// ============================================================================
// Relay → Client Messages
// ============================================================================

/// Messages sent from relay server to clients
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// Authentication successful
    AuthOk {
        session_id: String,
    },

    /// Authentication failed
    AuthFailed {
        reason: String,
    },

    /// List of agents (response to ListAgents)
    AgentList {
        agents: Vec<AgentInfo>,
    },

    /// Agent joined the room
    AgentJoined {
        agent: AgentInfo,
    },

    /// Agent left the room
    AgentLeft {
        agent_id: String,
    },

    /// Agent mode changed
    AgentModeChanged {
        agent_id: String,
        mode: AgentMode,
    },

    /// Command to execute (sent to agents). `payload` is an encrypted
    /// [`Command`] envelope.
    Command {
        request_id: String,
        from_session: String,
        payload: String,
    },

    /// Command result (forwarded from agent to MCP). `result` is an encrypted
    /// [`CommandResult`] envelope.
    CommandResult {
        request_id: String,
        agent_id: String,
        result: String,
    },

    /// Command error (forwarded from agent to MCP)
    CommandError {
        request_id: String,
        agent_id: String,
        error: String,
    },

    /// Event forwarded from an agent to MCP clients
    Event {
        agent_id: String,
        event: AgentEvent,
    },

    // ========================================================================
    // UDP Signaling Messages (forwarded by relay)
    // ========================================================================

    /// UDP offer forwarded to target peer
    UdpOffer {
        from_session: String,
        offer: UdpOffer,
    },

    /// UDP answer forwarded to offering peer
    UdpAnswer {
        from_session: String,
        answer: UdpAnswer,
    },

    /// UDP channel result notification
    UdpResult {
        from_session: String,
        result: UdpChannelResult,
    },

    /// Your public endpoint as seen by the relay (for STUN-like discovery)
    YourEndpoint {
        /// Your public IP:port as seen by the relay
        endpoint: crate::udp::Endpoint,
    },

    /// Heartbeat response
    Pong,

    /// Error message
    Error {
        message: String,
    },
}

/// Result of a command execution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "result_type", rename_all = "snake_case")]
pub enum CommandResult {
    /// Shell command result
    Exec {
        stdout: String,
        stderr: String,
        exit_code: i32,
    },

    /// File content
    File {
        content: String,
        size: u64,
    },

    /// Directory listing
    Dir {
        entries: Vec<DirEntry>,
    },

    /// Git status
    GitStatus {
        status: GitStatus,
    },

    /// Git operation textual output (pull/commit/push)
    Git {
        output: String,
        success: bool,
    },

    /// Agent info
    Info {
        info: AgentInfo,
    },

    /// Current agent mode (response to SetMode)
    Mode {
        mode: AgentMode,
    },

    /// List of scheduled tasks
    Schedule {
        tasks: Vec<ScheduledTask>,
    },

    /// An autonomous task was accepted and queued
    TaskQueued {
        id: String,
    },

    /// A single autonomous task (status + result)
    Task {
        task: AutonomousTask,
    },

    /// List of autonomous tasks
    TaskList {
        tasks: Vec<AutonomousTask>,
    },

    // === Distributed compute (MapReduce, Phase 13) ===
    /// Result of a [`Command::MapTask`] for one partition.
    MapResult {
        job_id: String,
        partition_id: u32,
        /// Map output (opaque; the coordinator's reduce step interprets it).
        output: String,
        success: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },

    /// Result of a [`Command::ReduceTask`].
    ReduceResult {
        job_id: String,
        output: String,
        success: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },

    /// Generic success
    Ok,
}

// ============================================================================
// Serialization helpers
// ============================================================================

impl Command {
    /// Serialize and encrypt this command into a base64 envelope string.
    pub fn encrypt(&self, cipher: &Cipher) -> Result<String, EnvelopeError> {
        let json = serde_json::to_string(self)?;
        Ok(cipher.encrypt_str(&json)?)
    }

    /// Decrypt and deserialize a command from a base64 envelope string.
    pub fn decrypt(envelope: &str, cipher: &Cipher) -> Result<Self, EnvelopeError> {
        let json = cipher.decrypt_str(envelope)?;
        Ok(serde_json::from_str(&json)?)
    }
}

impl CommandResult {
    /// Serialize and encrypt this result into a base64 envelope string.
    pub fn encrypt(&self, cipher: &Cipher) -> Result<String, EnvelopeError> {
        let json = serde_json::to_string(self)?;
        Ok(cipher.encrypt_str(&json)?)
    }

    /// Decrypt and deserialize a result from a base64 envelope string.
    pub fn decrypt(envelope: &str, cipher: &Cipher) -> Result<Self, EnvelopeError> {
        let json = cipher.decrypt_str(envelope)?;
        Ok(serde_json::from_str(&json)?)
    }
}

impl ClientMessage {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

impl ServerMessage {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_message_serialization() {
        let msg = ClientMessage::Auth {
            room: "dev".to_string(),
            token: "secret".to_string(),
            role: ClientRole::Agent,
            agent_info: Some(Box::new(AgentInfo {
                id: "agent-1".to_string(),
                name: "Dev Server".to_string(),
                mode: AgentMode::Plan,
                os: "linux".to_string(),
                arch: "x86_64".to_string(),
                hostname: "dev-server".to_string(),
                tags: vec!["web".to_string()],
                platform: Default::default(),
                autonomous: false,
                connected_at: 1234567890,
                session_id: None,
            })),
        };

        let json = msg.to_json().unwrap();
        let parsed: ClientMessage = ClientMessage::from_json(&json).unwrap();
        
        match parsed {
            ClientMessage::Auth { room, .. } => assert_eq!(room, "dev"),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_command_serialization() {
        let cmd = Command::Exec {
            command: "ls -la".to_string(),
            timeout_ms: Some(5000),
            cwd: None,
        };

        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("exec"));
        assert!(json.contains("ls -la"));
    }

    #[test]
    fn command_envelope_roundtrip() {
        let cipher = Cipher::for_transport("room-token", None);
        let cmd = Command::Exec {
            command: "whoami".to_string(),
            timeout_ms: None,
            cwd: None,
        };
        let envelope = cmd.encrypt(&cipher).unwrap();
        // Ciphertext must not leak the plaintext command.
        assert!(!envelope.contains("whoami"));
        let decrypted = Command::decrypt(&envelope, &cipher).unwrap();
        matches!(decrypted, Command::Exec { .. });
    }

    #[test]
    fn envelope_wrong_key_fails() {
        let a = Cipher::for_transport("token-a", None);
        let b = Cipher::for_transport("token-b", None);
        let env = CommandResult::Ok.encrypt(&a).unwrap();
        assert!(CommandResult::decrypt(&env, &b).is_err());
    }

    #[test]
    fn transport_key_matches_both_ends() {
        // Agent and MCP independently derive the same key from the token.
        let agent = Cipher::for_transport("shared-token", None);
        let mcp = Cipher::for_transport("shared-token", None);
        let env = CommandResult::Ok.encrypt(&agent).unwrap();
        assert!(CommandResult::decrypt(&env, &mcp).is_ok());
    }

    #[test]
    fn map_task_serde_roundtrip() {
        let cmd = Command::MapTask {
            job_id: "job-1".into(),
            partition_id: 3,
            map_fn: "x => x*2".into(),
            data: "[1,2,3]".into(),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("map_task")); // snake_case tag
        let back: Command = serde_json::from_str(&json).unwrap();
        match back {
            Command::MapTask { job_id, partition_id, map_fn, data } => {
                assert_eq!(job_id, "job-1");
                assert_eq!(partition_id, 3);
                assert_eq!(map_fn, "x => x*2");
                assert_eq!(data, "[1,2,3]");
            }
            other => panic!("expected MapTask, got {other:?}"),
        }
    }

    #[test]
    fn reduce_task_serde_roundtrip() {
        let cmd = Command::ReduceTask {
            job_id: "job-1".into(),
            reduce_fn: "(a,b) => a+b".into(),
            inputs: vec!["2".into(), "4".into(), "6".into()],
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("reduce_task"));
        let back: Command = serde_json::from_str(&json).unwrap();
        match back {
            Command::ReduceTask { inputs, .. } => assert_eq!(inputs.len(), 3),
            other => panic!("expected ReduceTask, got {other:?}"),
        }
    }

    #[test]
    fn map_result_omits_error_when_none_and_keeps_it_when_some() {
        let ok = CommandResult::MapResult {
            job_id: "j".into(),
            partition_id: 0,
            output: "42".into(),
            success: true,
            error: None,
        };
        let json = serde_json::to_string(&ok).unwrap();
        assert!(!json.contains("error"), "error should be skipped when None");

        let failed = CommandResult::MapResult {
            job_id: "j".into(),
            partition_id: 1,
            output: String::new(),
            success: false,
            error: Some("boom".into()),
        };
        let json = serde_json::to_string(&failed).unwrap();
        assert!(json.contains("boom"));
    }

    #[test]
    fn map_result_envelope_roundtrip() {
        let cipher = Cipher::for_transport("room-token", None);
        let res = CommandResult::ReduceResult {
            job_id: "j".into(),
            output: "done".into(),
            success: true,
            error: None,
        };
        let env = res.encrypt(&cipher).unwrap();
        let back = CommandResult::decrypt(&env, &cipher).unwrap();
        match back {
            CommandResult::ReduceResult { output, success, .. } => {
                assert_eq!(output, "done");
                assert!(success);
            }
            other => panic!("expected ReduceResult, got {other:?}"),
        }
    }
}
