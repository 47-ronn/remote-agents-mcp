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
    #[error("decompression failed: {0}")]
    Decompress(#[from] std::io::Error),
}

// ============================================================================
// Client → Relay Messages
// ============================================================================

/// Messages sent from clients (MCP or Agent) to the relay server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Authenticate with the relay. Peer model: there are no roles — every node
    /// joins as an equal peer and must carry its `agent_info` identity.
    Auth {
        room: String,
        token: String,
        // Boxed: `AgentInfo` is the largest payload across all ClientMessage
        // variants; boxing keeps the common (command/result/ping) messages small.
        agent_info: Option<Box<AgentInfo>>,
    },

    /// Request the list of peers in the room
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

    /// Update this agent's info (e.g. after mode change)
    UpdateAgent {
        agent_info: Box<AgentInfo>,
    },
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
    /// credentials). Returns immediately with a task id. `initiator` is the
    /// dispatching peer's id, recorded as the task's leader (peer model).
    TaskDispatch {
        prompt: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        initiator: Option<String>,
    },

    /// Get a single autonomous task by id (status + result)
    TaskGet {
        id: String,
    },

    /// List autonomous tasks on the host
    TaskList,

    // === AI-provider sessions (claude / opencode history) ===
    /// List the host's provider conversations (metadata only).
    SessionList,

    /// Fetch the full transcript of one provider session.
    SessionGet {
        provider: String,
        id: String,
    },

    /// Continue a provider session with a new prompt (resume context on the host
    /// that owns it). Runs as an autonomous task with a resume runner.
    SessionResume {
        provider: String,
        id: String,
        prompt: String,
    },

    /// Terminate a live provider session running on the host (kill its process).
    SessionTerminate {
        id: String,
    },

    // === File transfer (search, binary-safe chunked read, thumbnails) ===
    /// Metadata for one file (size/mime/is_image) without reading its body.
    FileStat {
        path: String,
    },

    /// Read a binary-safe slice `[offset, offset+len)` of a file, base64-encoded.
    /// The web client pulls a file chunk-by-chunk; each chunk is its own request.
    FileChunk {
        path: String,
        offset: u64,
        len: u64,
    },

    /// Produce a downscaled JPEG preview of an image (longest side `max_px`).
    FileThumb {
        path: String,
        max_px: u32,
    },

    /// Search for files under `roots` matching `query` by name/content/image.
    FileSearch {
        /// Directories to search; empty → host's default roots (home + common dirs).
        #[serde(default)]
        roots: Vec<String>,
        query: String,
        kind: SearchKind,
    },

    /// Send a local file to another host over the UDP data channel (WS fallback).
    /// Returns immediately with a transfer id; poll progress with `TransferGet`.
    SendFileTo {
        src_path: String,
        /// Destination peer's agent id.
        dest_id: String,
        /// Absolute path to write on the destination.
        dest_path: String,
    },

    /// Receive one slice of a host↔host transfer and write it to `dest_path` at
    /// `offset` (the destination side of `SendFileTo`; rides the peer command
    /// path, UDP-preferred). On `eof`, the whole file is verified against
    /// `sha256`. Requires write mode + path allow-list on the receiver.
    FileRecv {
        transfer_id: String,
        dest_path: String,
        offset: u64,
        /// Base64 of the raw bytes for this slice.
        bytes: String,
        eof: bool,
        /// Lowercase-hex SHA-256 of the whole file, present only on the final
        /// (`eof`) slice for verification.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sha256: Option<String>,
    },

    /// Get the status/progress of a host↔host transfer by id.
    TransferGet {
        id: String,
    },

    // === Cloudflare quick tunnels (dev: expose a local port publicly) ===
    /// Start a Cloudflare quick tunnel to a local address (e.g.
    /// `http://localhost:3000`, or a bare port). Downloads `cloudflared` on
    /// demand. Returns the public `*.trycloudflare.com` URL. Requires edit/bypass.
    TunnelStart {
        target: String,
    },
    /// List this host's running quick tunnels.
    TunnelList,
    /// Stop a running quick tunnel by id.
    TunnelStop {
        id: String,
    },

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

    /// Agent joined the room. Boxed to keep this from being a giant `ServerMessage`
    /// variant (mirrors `ClientMessage::Auth`); serde treats `Box<AgentInfo>` as
    /// the inline object, so the wire format is unchanged.
    AgentJoined {
        agent: Box<AgentInfo>,
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
        /// Wall-clock execution time in milliseconds.
        #[serde(default)]
        duration_ms: Option<u64>,
        /// `true` if the command was killed due to timeout.
        #[serde(default)]
        timed_out: Option<bool>,
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

    // === File transfer ===
    /// File metadata (response to `FileStat`; also each `FileSearch` hit shape).
    FileMeta {
        meta: FileMeta,
    },

    /// One binary-safe file chunk (base64) with an end-of-file marker.
    FileChunk {
        /// Base64 of the raw bytes for the requested slice.
        data: String,
        /// True when this slice reaches the end of the file.
        eof: bool,
    },

    /// A downscaled image preview (base64 JPEG) with its pixel dimensions.
    FileThumb {
        data: String,
        w: u32,
        h: u32,
    },

    /// Files matching a `FileSearch`.
    FileSearch {
        hits: Vec<FileMeta>,
    },

    /// A host↔host transfer was accepted and queued.
    TransferQueued {
        id: String,
    },

    /// Progress/status of a host↔host transfer (response to `TransferGet`).
    Transfer {
        status: TransferStatus,
    },

    /// A Cloudflare quick tunnel was started (carries its public URL).
    TunnelStarted {
        tunnel: TunnelInfo,
    },

    /// All running quick tunnels on this host.
    TunnelList {
        tunnels: Vec<TunnelInfo>,
    },

    // === AI-provider sessions ===
    /// Provider session metadata + the ids of sessions currently live on the host.
    SessionList {
        sessions: Vec<SessionMeta>,
        #[serde(default)]
        active: Vec<String>,
    },

    /// Full transcript of one session.
    SessionTranscript {
        messages: Vec<SessionMessage>,
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
    /// Serialize, transparently compress (large payloads), and encrypt this
    /// command into a base64 envelope string.
    pub fn encrypt(&self, cipher: &Cipher) -> Result<String, EnvelopeError> {
        let json = serde_json::to_vec(self)?;
        Ok(cipher.encrypt(&crate::compress::maybe_compress(&json))?)
    }

    /// Decrypt, decompress if needed, and deserialize a command.
    pub fn decrypt(envelope: &str, cipher: &Cipher) -> Result<Self, EnvelopeError> {
        let raw = cipher.decrypt(envelope)?;
        let json = crate::compress::maybe_decompress(&raw)?;
        Ok(serde_json::from_slice(&json)?)
    }
}

impl CommandResult {
    /// Serialize, transparently compress (large payloads), and encrypt this
    /// result into a base64 envelope string.
    pub fn encrypt(&self, cipher: &Cipher) -> Result<String, EnvelopeError> {
        let json = serde_json::to_vec(self)?;
        Ok(cipher.encrypt(&crate::compress::maybe_compress(&json))?)
    }

    /// Decrypt, decompress if needed, and deserialize a result.
    pub fn decrypt(envelope: &str, cipher: &Cipher) -> Result<Self, EnvelopeError> {
        let raw = cipher.decrypt(envelope)?;
        let json = crate::compress::maybe_decompress(&raw)?;
        Ok(serde_json::from_slice(&json)?)
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
                accepts_commands: true,
                connected_at: 1234567890,
                session_id: None,
                version: String::new(), update_available: None, connections: None,
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
    fn large_result_compresses_on_the_wire_and_roundtrips() {
        let cipher = Cipher::for_transport("room-token", None);
        // A big, highly compressible result — like a provider transcript or a
        // file-transfer chunk of text.
        let big = "compress me please ".repeat(2000); // ~38 KB
        let result = CommandResult::Exec {
            stdout: big.clone(),
            stderr: String::new(),
            exit_code: 0,
            duration_ms: Some(42),
            timed_out: Some(false),
        };
        let envelope = result.encrypt(&cipher).unwrap();
        // The base64 envelope is far smaller than the plaintext — proving the
        // zstd layer engaged INSIDE the encrypted envelope (not just in the
        // unit-tested helper). Encryption alone would keep it ~same size.
        assert!(
            envelope.len() < big.len() / 2,
            "compressible result should shrink on the wire (envelope {} vs plaintext {})",
            envelope.len(),
            big.len()
        );
        // ...and it decrypts + decompresses back to the original.
        match CommandResult::decrypt(&envelope, &cipher).unwrap() {
            CommandResult::Exec { stdout, exit_code, .. } => {
                assert_eq!(stdout, big);
                assert_eq!(exit_code, 0);
            }
            other => panic!("expected Exec, got {other:?}"),
        }
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

    #[test]
    fn server_message_roundtrips_with_snake_case_tag() {
        // Server messages had no roundtrip coverage; lock the wire contract.
        let msg = ServerMessage::AuthOk { session_id: "sess-9".into() };
        let json = msg.to_json().unwrap();
        assert!(json.contains("\"type\":\"auth_ok\""), "tag: {json}");
        match ServerMessage::from_json(&json).unwrap() {
            ServerMessage::AuthOk { session_id } => assert_eq!(session_id, "sess-9"),
            other => panic!("expected AuthOk, got {other:?}"),
        }

        // A Command envelope carries only clear-text routing metadata.
        let cmd = ServerMessage::Command {
            request_id: "r1".into(),
            from_session: "mcp-1".into(),
            payload: "<ciphertext>".into(),
        };
        let json = cmd.to_json().unwrap();
        assert!(json.contains("\"type\":\"command\""));
        match ServerMessage::from_json(&json).unwrap() {
            ServerMessage::Command { request_id, from_session, .. } => {
                assert_eq!(request_id, "r1");
                assert_eq!(from_session, "mcp-1");
            }
            other => panic!("expected Command, got {other:?}"),
        }
    }

    #[test]
    fn client_udp_offer_variant_roundtrips() {
        // Newtype tuple variants inside an internally-tagged enum are subtle —
        // lock that UdpOffer/Answer/Result survive a JSON roundtrip.
        use crate::udp::{Endpoint, UdpOffer};
        use std::net::{IpAddr, Ipv4Addr};
        let offer = UdpOffer {
            channel_id: "c1".into(),
            from_session: "a".into(),
            to_session: "b".into(),
            local_endpoint: Endpoint::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1111),
            public_endpoint: None,
            nonce: [7u8; 16],
        };
        let msg = ClientMessage::UdpOffer(offer);
        let json = msg.to_json().unwrap();
        match ClientMessage::from_json(&json).unwrap() {
            ClientMessage::UdpOffer(o) => {
                assert_eq!(o.channel_id, "c1");
                assert_eq!(o.nonce, [7u8; 16]);
            }
            other => panic!("expected UdpOffer, got {other:?}"),
        }
    }

    #[test]
    fn git_pull_defaults_remote_to_origin_on_deserialize() {
        // An MCP client may omit `remote`; serde must fill it with "origin"
        // (wire-compat with the #[serde(default = "default_origin")] attribute).
        let json = r#"{"cmd":"git_pull","repo":"/srv/app"}"#;
        match serde_json::from_str::<Command>(json).unwrap() {
            Command::GitPull { repo, remote, branch } => {
                assert_eq!(repo, "/srv/app");
                assert_eq!(remote, "origin");
                assert_eq!(branch, None);
            }
            other => panic!("expected GitPull, got {other:?}"),
        }
    }

    #[test]
    fn command_decrypt_rejects_malformed_envelope() {
        let cipher = Cipher::for_transport("room-token", None);
        // Not valid base64 / not a real envelope → decrypt error, never a panic.
        assert!(Command::decrypt("!!!not-base64!!!", &cipher).is_err());
        assert!(Command::decrypt("", &cipher).is_err());
    }
}
