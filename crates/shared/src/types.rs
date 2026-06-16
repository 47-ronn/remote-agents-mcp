//! Common types used across the system

use serde::{Deserialize, Serialize};

/// serde default for boolean fields that default to `true`.
fn default_true_bool() -> bool {
    true
}

/// Agent operation mode - determines what operations are allowed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AgentMode {
    /// Read-only operations (read_file, ls, git_status, safe exec)
    #[default]
    Plan,
    /// Write operations with automatic backup
    Edit,
    /// Full access without restrictions (dangerous!)
    Bypass,
    /// Agent is offline/blocked - no operations allowed
    Disabled,
}

impl AgentMode {
    pub fn allows_write(&self) -> bool {
        matches!(self, AgentMode::Edit | AgentMode::Bypass)
    }

    pub fn allows_exec(&self) -> bool {
        !matches!(self, AgentMode::Disabled)
    }

    pub fn requires_backup(&self) -> bool {
        matches!(self, AgentMode::Edit)
    }
}

/// Richer platform metadata so orchestrators (and peer agents) can tailor
/// commands to a host's OS — e.g. choose `apt` vs `brew`, `sh` vs `powershell`.
///
/// Filled best-effort at agent startup via [`PlatformInfo::detect`] (cheap file
/// reads + env vars, no subprocesses). Unknown fields stay `None`.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PlatformInfo {
    /// OS family: `linux` | `macos` | `windows` (from `env::consts::OS`).
    pub family: String,
    /// CPU architecture (from `env::consts::ARCH`), e.g. `x86_64`, `aarch64`.
    pub arch: String,
    /// Human-readable distro/OS name, e.g. "Ubuntu 22.04.5 LTS".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distro: Option<String>,
    /// Kernel release, e.g. "6.8.0-45-generic".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kernel: Option<String>,
    /// Default user shell, e.g. "/bin/bash" (or "cmd"/"powershell" on Windows).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell: Option<String>,
}

impl PlatformInfo {
    /// Detect the current host's platform. Best-effort: never fails, leaving
    /// fields it cannot determine as `None`.
    pub fn detect() -> Self {
        Self {
            family: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            distro: detect_distro(),
            kernel: detect_kernel(),
            shell: detect_shell(),
        }
    }
}

/// Parse the `PRETTY_NAME` (preferred) from an os-release file body.
/// Exposed for testing the parser without touching the filesystem.
pub fn parse_os_release(contents: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let value = line.strip_prefix("PRETTY_NAME=")?;
        Some(value.trim().trim_matches('"').to_string())
    })
}

fn detect_distro() -> Option<String> {
    if cfg!(target_os = "linux") {
        // /etc/os-release is the cross-distro standard; /usr/lib is the fallback.
        for path in ["/etc/os-release", "/usr/lib/os-release"] {
            if let Ok(contents) = std::fs::read_to_string(path) {
                if let Some(name) = parse_os_release(&contents) {
                    return Some(name);
                }
            }
        }
    }
    None
}

fn detect_kernel() -> Option<String> {
    if cfg!(target_os = "linux") {
        return std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
    }
    None
}

fn detect_shell() -> Option<String> {
    if cfg!(windows) {
        std::env::var("ComSpec")
            .ok()
            .or_else(|| Some("cmd".to_string()))
    } else {
        std::env::var("SHELL").ok().filter(|s| !s.is_empty())
    }
}

/// Information about a connected agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    /// Unique agent ID
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Current operation mode
    pub mode: AgentMode,
    /// Operating system
    pub os: String,
    /// CPU architecture
    pub arch: String,
    /// Hostname
    pub hostname: String,
    /// Tags for filtering
    pub tags: Vec<String>,
    /// Richer platform metadata (distro, kernel, shell) for OS-aware tasking.
    /// `#[serde(default)]` keeps wire-compat with agents that predate this field.
    #[serde(default)]
    pub platform: PlatformInfo,
    /// Whether this host can run autonomous AI tasks with its own credentials
    #[serde(default)]
    pub autonomous: bool,
    /// Whether this peer executes commands sent by other peers. In the peer
    /// model there is no controller/target role — every node is equal; this
    /// capability is what `--no-agent` toggles. Send-only nodes (prod
    /// controllers, browser dashboards) advertise `false`: visible and able to
    /// dispatch work, but never running others' commands. Defaults to `true`
    /// (wire-compat: nodes predating the field are full peers).
    #[serde(default = "default_true_bool")]
    pub accepts_commands: bool,
    /// Connection timestamp (Unix ms)
    pub connected_at: u64,
    /// Session ID for this connection (used for UDP signaling)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Newer published version available for this host, if any (e.g. "0.1.2").
    /// Set when the launcher's npm-registry check found a release newer than the
    /// running binary, so orchestrators can flag stale hosts in `list_agents`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_available: Option<String>,
}

/// Metadata for one AI-provider conversation (claude / opencode) stored on a
/// host. The full transcript is fetched lazily on demand (see SessionGet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMeta {
    /// AI provider: `claude` | `opencode`.
    pub provider: String,
    /// Provider-native session id (claude: uuid; opencode: `ses_…`).
    pub id: String,
    /// Human-readable title (provider-generated or first user message).
    pub title: String,
    /// Last-activity timestamp (Unix ms).
    pub updated: u64,
    /// Working directory / project the session belongs to, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
}

/// One message in a session transcript.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMessage {
    /// `user` | `assistant` | `system`.
    pub role: String,
    /// Message text (tool/structured parts are flattened to text).
    pub text: String,
    /// Timestamp (Unix ms), if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts: Option<u64>,
}

/// Lifecycle status of an autonomous task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    /// Accepted, not yet started
    Queued,
    /// Runner process is executing
    Running,
    /// Completed successfully
    Done,
    /// Failed (non-zero exit, spawn error, or timeout)
    Failed,
}

/// An autonomous AI task delegated to a remote host. The host runs the task
/// with ITS OWN credentials (its configured AI CLI login), so the orchestrator
/// spends no tokens — it dispatches, then collects the result later.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutonomousTask {
    /// Unique task id
    pub id: String,
    /// Peer id of the node that initiated this task (the "leader" for it; other
    /// nodes are executors). Peer model: roles live per-task in metadata, not on
    /// the network. `None` for tasks created before this field / by an
    /// unidentified initiator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initiator: Option<String>,
    /// The prompt / instructions given to the host's AI runner
    pub prompt: String,
    /// Current status
    pub status: TaskStatus,
    /// Captured output once finished
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    /// Error detail when failed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Created timestamp (Unix ms)
    pub created_at: u64,
    /// Started timestamp (Unix ms)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,
    /// Finished timestamp (Unix ms)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<u64>,
    /// Runner process exit code, if it ran
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

/// Target for a command
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Target {
    /// Single agent by ID
    Agent { id: String },
    /// All agents in the room
    All,
    /// Agents matching any of the tags
    Tagged { tags: Vec<String> },
    /// Agents whose OS family matches (e.g. "linux", "macos", "windows").
    /// Lets the orchestrator target hosts by platform for OS-specific commands.
    Platform { family: String },
}

/// Directory entry for ls command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    pub modified: Option<u64>,
}

/// A scheduled task definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledTask {
    /// Unique task name
    pub name: String,
    /// Cron expression (6-field: sec min hour day month weekday)
    pub cron: String,
    /// Shell command to run
    pub command: String,
    /// Last run timestamp (Unix ms), if ever run
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run: Option<u64>,
    /// Number of times this task has run
    #[serde(default)]
    pub run_count: u64,
}

/// An unsolicited event pushed from an agent to MCP clients (via the relay).
/// Carries only non-secret metadata; sensitive results are fetched separately
/// over the encrypted command path.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum AgentEvent {
    /// An autonomous task finished (succeeded or failed).
    TaskCompleted { task_id: String, status: TaskStatus },
}

/// Git repository status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitStatus {
    pub branch: String,
    pub clean: bool,
    pub ahead: u32,
    pub behind: u32,
    pub staged: Vec<String>,
    pub modified: Vec<String>,
    pub untracked: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_mode_permission_matrix() {
        // The security gate the executor relies on: who may write / exec / backup.
        assert!(!AgentMode::Plan.allows_write());
        assert!(AgentMode::Edit.allows_write());
        assert!(AgentMode::Bypass.allows_write());
        assert!(!AgentMode::Disabled.allows_write());

        // Exec is allowed in every mode except Disabled.
        assert!(AgentMode::Plan.allows_exec());
        assert!(AgentMode::Edit.allows_exec());
        assert!(AgentMode::Bypass.allows_exec());
        assert!(!AgentMode::Disabled.allows_exec());

        // Only Edit takes automatic backups (Bypass is unrestricted, no backup).
        assert!(AgentMode::Edit.requires_backup());
        assert!(!AgentMode::Plan.requires_backup());
        assert!(!AgentMode::Bypass.requires_backup());
        assert!(!AgentMode::Disabled.requires_backup());
    }

    #[test]
    fn agent_mode_default_is_plan() {
        // The safe default: a freshly-constructed mode is read-only.
        assert_eq!(AgentMode::default(), AgentMode::Plan);
    }

    #[test]
    fn agent_mode_serde_is_lowercase() {
        for (mode, tok) in [
            (AgentMode::Plan, "\"plan\""),
            (AgentMode::Edit, "\"edit\""),
            (AgentMode::Bypass, "\"bypass\""),
            (AgentMode::Disabled, "\"disabled\""),
        ] {
            assert_eq!(serde_json::to_string(&mode).unwrap(), tok);
            assert_eq!(serde_json::from_str::<AgentMode>(tok).unwrap(), mode);
        }
    }

    #[test]
    fn target_serde_wire_format() {
        // Internally-tagged with `type`, lowercase variant names — the on-wire
        // contract the relay routes on.
        let agent = serde_json::to_value(Target::Agent { id: "a1".into() }).unwrap();
        assert_eq!(agent["type"], "agent");
        assert_eq!(agent["id"], "a1");

        assert_eq!(serde_json::to_value(Target::All).unwrap()["type"], "all");

        let tagged = serde_json::to_value(Target::Tagged { tags: vec!["gpu".into()] }).unwrap();
        assert_eq!(tagged["type"], "tagged");
        assert_eq!(tagged["tags"][0], "gpu");

        let platform = serde_json::to_value(Target::Platform { family: "linux".into() }).unwrap();
        assert_eq!(platform["type"], "platform");
        assert_eq!(platform["family"], "linux");

        // Round-trips back to the same variant.
        let json = r#"{"type":"agent","id":"x"}"#;
        assert!(matches!(
            serde_json::from_str::<Target>(json).unwrap(),
            Target::Agent { id } if id == "x"
        ));
    }

    #[test]
    fn parse_os_release_unquoted_value() {
        // Some distros emit PRETTY_NAME without surrounding quotes.
        assert_eq!(parse_os_release("PRETTY_NAME=Alpine\n").as_deref(), Some("Alpine"));
    }

    #[test]
    fn parse_os_release_extracts_pretty_name() {
        let body = "NAME=\"Ubuntu\"\nVERSION_ID=\"22.04\"\nPRETTY_NAME=\"Ubuntu 22.04.5 LTS\"\nID=ubuntu\n";
        assert_eq!(parse_os_release(body).as_deref(), Some("Ubuntu 22.04.5 LTS"));
    }

    #[test]
    fn parse_os_release_none_when_absent() {
        assert!(parse_os_release("ID=arch\nNAME=Arch\n").is_none());
    }

    #[test]
    fn detect_fills_family_and_arch() {
        let p = PlatformInfo::detect();
        // These always come from the compiler's target and are never empty.
        assert!(!p.family.is_empty());
        assert!(!p.arch.is_empty());
        assert_eq!(p.family, std::env::consts::OS);
        assert_eq!(p.arch, std::env::consts::ARCH);
    }

    #[test]
    fn agent_info_accepts_commands_defaults_true_on_wire() {
        // A node predating the capability field (or a full peer) is treated as
        // accepting commands — defaults to true when absent.
        let json = r#"{
            "id":"a","name":"a","mode":"plan","os":"linux","arch":"x86_64",
            "hostname":"h","tags":[],"connected_at":0
        }"#;
        let info: AgentInfo = serde_json::from_str(json).unwrap();
        assert!(info.accepts_commands, "missing field must default to true");

        // A send-only (--no-agent) peer round-trips as false.
        let send_only = AgentInfo { accepts_commands: false, ..info.clone() };
        let s = serde_json::to_string(&send_only).unwrap();
        let back: AgentInfo = serde_json::from_str(&s).unwrap();
        assert!(!back.accepts_commands);
    }

    #[test]
    fn agent_info_update_available_is_optional_on_wire() {
        // Older agents (and up-to-date ones) omit the field entirely.
        let json = r#"{
            "id":"a","name":"a","mode":"plan","os":"linux","arch":"x86_64",
            "hostname":"h","tags":[],"connected_at":0
        }"#;
        let info: AgentInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.update_available, None);
        // None is skipped on serialize (keeps the common case small).
        assert!(!serde_json::to_string(&info).unwrap().contains("update_available"));

        // When set, it round-trips.
        let mut withv = info.clone();
        withv.update_available = Some("0.1.2".into());
        let s = serde_json::to_string(&withv).unwrap();
        assert!(s.contains("update_available"));
        let back: AgentInfo = serde_json::from_str(&s).unwrap();
        assert_eq!(back.update_available.as_deref(), Some("0.1.2"));
    }

    #[test]
    fn agent_info_deserializes_without_platform_field() {
        // An older agent that predates `platform` must still parse, defaulting it.
        let json = r#"{
            "id":"a","name":"a","mode":"plan","os":"linux","arch":"x86_64",
            "hostname":"h","tags":[],"connected_at":0
        }"#;
        let info: AgentInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.platform, PlatformInfo::default());
        assert!(info.platform.family.is_empty());
    }

    #[test]
    fn platform_info_roundtrips_and_skips_none() {
        let p = PlatformInfo {
            family: "linux".into(),
            arch: "x86_64".into(),
            distro: Some("Ubuntu 22.04".into()),
            kernel: None,
            shell: Some("/bin/bash".into()),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(!json.contains("kernel"), "None fields are skipped");
        let back: PlatformInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(p, back);
    }
}
