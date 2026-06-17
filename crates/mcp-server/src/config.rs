//! Agent configuration

use anyhow::{Context, Result};
use remote_agents_shared::AgentMode;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Agent ID (auto-generated if not set)
    #[serde(default = "default_id")]
    pub id: String,

    /// Human-readable agent name
    #[serde(default = "default_name")]
    pub name: String,

    /// Room to join
    #[serde(default = "default_room")]
    pub room: String,

    /// Authentication token
    #[serde(default)]
    pub token: String,

    /// Relay server URL
    #[serde(default = "default_relay")]
    pub relay_url: String,

    /// Agent tags for filtering
    #[serde(default)]
    pub tags: Vec<String>,

    /// Whether this node executes commands sent by other peers. In the peer
    /// model every node is equal; this capability (advertised in AgentInfo) is
    /// what replaces the old controller/target role split. `--no-agent` sets it
    /// false for send-only nodes (prod controllers, browser dashboards): the
    /// node stays visible and can dispatch work, but never runs others' commands.
    #[serde(default = "default_accepts_commands")]
    pub accepts_commands: bool,

    /// Security settings
    #[serde(default)]
    pub security: SecurityConfig,

    /// Autonomous AI task execution settings
    #[serde(default)]
    pub autonomous: AutonomousConfig,
}

fn default_accepts_commands() -> bool {
    true
}

/// Per-host autonomous AI runner configuration.
///
/// When `enabled`, the host can accept delegated AI tasks and run them with its
/// OWN credentials. The runner inherits the agent process environment (the
/// host's existing `claude`/CLI login, `ANTHROPIC_API_KEY`, etc.) — the agent
/// never stores any keys itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutonomousConfig {
    /// Whether this host runs autonomous AI tasks. `None` (the default) means
    /// auto-detect: available iff the `runner` program resolves on PATH. Set
    /// `Some(true)` to force on or `Some(false)` to force off (send a node a
    /// runner it doesn't have, or force-off, to keep it a plain executor).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,

    /// Runner command + leading args. The task prompt is appended as the final
    /// argument. Default: `["claude", "-p"]` (Claude Code headless).
    #[serde(default = "default_runner")]
    pub runner: Vec<String>,

    /// Working directory for the runner (default: the user's home directory).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,

    /// Maximum runtime for a single task, in seconds.
    #[serde(default = "default_task_timeout")]
    pub timeout: u64,
}

impl Default for AutonomousConfig {
    fn default() -> Self {
        Self {
            enabled: None,
            runner: default_runner(),
            workdir: None,
            timeout: default_task_timeout(),
        }
    }
}

fn default_runner() -> Vec<String> {
    vec!["claude".to_string(), "-p".to_string()]
}

/// Effective autonomous availability: an explicit `enabled` override, else
/// auto-detect by whether the runner's program is on PATH. This is what
/// `AgentInfo.autonomous` advertises, so the fleet can route AI tasks to hosts
/// that can actually run them.
pub fn autonomous_available(cfg: &AutonomousConfig) -> bool {
    cfg.enabled.unwrap_or_else(|| ai_runner_available(&cfg.runner))
}

/// Whether the runner's program resolves (so this host can launch the AI CLI).
/// Does NOT verify the CLI is logged in — that only surfaces when a task runs.
fn ai_runner_available(runner: &[String]) -> bool {
    runner.first().is_some_and(|p| program_on_path(p))
}

/// Resolve a program name to an executable: an absolute/relative path is checked
/// directly, otherwise each `PATH` entry is searched.
fn program_on_path(prog: &str) -> bool {
    if prog.is_empty() {
        return false;
    }
    if prog.contains('/') {
        return std::path::Path::new(prog).is_file();
    }
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(prog).is_file()))
        .unwrap_or(false)
}

fn default_task_timeout() -> u64 {
    3600 // 1 hour
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Operation mode
    #[serde(default)]
    pub mode: AgentMode,

    /// Enable backups before writes
    #[serde(default = "default_true")]
    pub backup_enabled: bool,

    /// Backup directory
    #[serde(default = "default_backup_dir")]
    pub backup_dir: String,

    /// Max backup versions per file
    #[serde(default = "default_max_versions")]
    pub max_backup_versions: usize,

    /// Allowed paths (empty = all allowed)
    #[serde(default)]
    pub allowed_paths: Vec<String>,

    /// Denied paths (always blocked for read/write, even when allow list is empty)
    #[serde(default = "default_denied_paths")]
    pub denied_paths: Vec<String>,

    /// Denied commands (substring patterns)
    #[serde(default = "default_denied_commands")]
    pub denied_commands: Vec<String>,

    /// Commands permitted in Plan (read-only) mode
    #[serde(default = "crate::safety::default_readonly_commands")]
    pub readonly_commands: Vec<String>,

    /// Max file size in bytes for read/write (0 = unlimited)
    #[serde(default = "default_max_file_size")]
    pub max_file_size: u64,

    /// Max size in bytes for a chunked file transfer / download (0 = unlimited).
    /// Separate from `max_file_size` so bulk transfers aren't capped by the
    /// in-band 10 MiB read/write guard.
    #[serde(default = "default_max_transfer_size")]
    pub max_transfer_size: u64,

    /// Chunk size in bytes for chunked file reads/downloads. Kept well under the
    /// relay's ~1 MiB WebSocket frame limit after base64 + encryption overhead.
    #[serde(default = "default_transfer_chunk_size")]
    pub transfer_chunk_size: u64,

    /// Roots for `FileSearch` when the request supplies none (empty → derived at
    /// runtime: home + Pictures/Documents/Downloads/Desktop).
    #[serde(default)]
    pub search_roots: Vec<String>,

    /// Max number of hits returned by a single `FileSearch`.
    #[serde(default = "default_search_max_results")]
    pub search_max_results: usize,

    /// Command timeout in seconds
    #[serde(default = "default_timeout")]
    pub command_timeout: u64,

    /// Optional override for the end-to-end encryption key (AES-GCM-256).
    /// E2E encryption is ALWAYS on; by default the key is derived from the room
    /// token. Set this to decouple the encryption key from the auth token — the
    /// MCP server must then use the same value (REMOTE_AGENTS_KEY).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption_key: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            id: default_id(),
            name: default_name(),
            room: default_room(),
            token: String::new(),
            relay_url: default_relay(),
            tags: Vec::new(),
            accepts_commands: default_accepts_commands(),
            security: SecurityConfig::default(),
            autonomous: AutonomousConfig::default(),
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            mode: AgentMode::Plan,
            backup_enabled: true,
            backup_dir: default_backup_dir(),
            max_backup_versions: 10,
            allowed_paths: Vec::new(),
            denied_paths: default_denied_paths(),
            denied_commands: default_denied_commands(),
            readonly_commands: crate::safety::default_readonly_commands(),
            max_file_size: default_max_file_size(),
            max_transfer_size: default_max_transfer_size(),
            transfer_chunk_size: default_transfer_chunk_size(),
            search_roots: Vec::new(),
            search_max_results: default_search_max_results(),
            command_timeout: 300,
            encryption_key: None,
        }
    }
}

fn default_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn default_name() -> String {
    hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

fn default_room() -> String {
    "default".to_string()
}

fn default_relay() -> String {
    // Neutral local default; configure your own relay via config/flag/env.
    "ws://127.0.0.1:8080".to_string()
}

/// Override config fields from `REMOTE_AGENTS_*` environment variables. Lets MCP
/// client configs supply connection settings via `env` (a common pattern) rather
/// than CLI args. Precedence: CLI flag > env var > config.toml > built-in default.
pub fn apply_env(cfg: &mut Config) {
    let set = |target: &mut String, key: &str| {
        if let Ok(v) = std::env::var(key) {
            if !v.is_empty() {
                *target = v;
            }
        }
    };
    set(&mut cfg.relay_url, "REMOTE_AGENTS_RELAY");
    set(&mut cfg.room, "REMOTE_AGENTS_ROOM");
    set(&mut cfg.token, "REMOTE_AGENTS_TOKEN");
    set(&mut cfg.name, "REMOTE_AGENTS_NAME");
}

fn default_backup_dir() -> String {
    dirs::data_dir()
        .map(|p| p.join("remote-agents").join("backups"))
        .unwrap_or_else(|| PathBuf::from("~/.remote-agents/backups"))
        .to_string_lossy()
        .to_string()
}

fn default_true() -> bool {
    true
}

fn default_max_versions() -> usize {
    10
}

fn default_timeout() -> u64 {
    300
}

fn default_denied_commands() -> Vec<String> {
    vec![
        "rm -rf /".to_string(),
        "rm -rf /*".to_string(),
        "mkfs".to_string(),
        ":(){:|:&};:".to_string(), // Fork bomb
        "dd if=/dev/zero".to_string(),
        "> /dev/sda".to_string(),
    ]
}

fn default_denied_paths() -> Vec<String> {
    vec![
        "/etc/shadow".to_string(),
        "/etc/gshadow".to_string(),
        "/etc/sudoers".to_string(),
        "/boot".to_string(),
        "/proc/sysrq-trigger".to_string(),
    ]
}

fn default_max_file_size() -> u64 {
    10 * 1024 * 1024 // 10 MiB
}

fn default_max_transfer_size() -> u64 {
    100 * 1024 * 1024 // 100 MiB
}

fn default_transfer_chunk_size() -> u64 {
    256 * 1024 // 256 KiB
}

fn default_search_max_results() -> usize {
    200
}

/// Get config file path
pub fn config_path() -> PathBuf {
    dirs::config_dir()
        .map(|p| p.join("remote-agents").join("config.toml"))
        .unwrap_or_else(|| PathBuf::from("~/.config/remote-agents/config.toml"))
}

/// Serialize the config to TOML for display, masking secrets so
/// `remote-agent config` never prints the room token or encryption key to the
/// terminal/logs. Set secrets show as `***`; unset ones stay empty/absent.
pub fn redacted_toml(cfg: &Config) -> Result<String> {
    let mut c = cfg.clone();
    if !c.token.is_empty() {
        c.token = "***".to_string();
    }
    if c.security.encryption_key.is_some() {
        c.security.encryption_key = Some("***".to_string());
    }
    Ok(toml::to_string_pretty(&c)?)
}

/// Load configuration from file
pub fn load_config() -> Result<Config> {
    let path = config_path();

    if !path.exists() {
        // No config file: still give the agent a STABLE identity across restarts
        // (otherwise fleet targeting / task reminders keyed by id break every
        // time the process restarts, since the serde default mints a new uuid).
        return Ok(Config {
            id: persistent_id(),
            ..Config::default()
        });
    }

    let content = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config from {:?}", path))?;

    let mut cfg: Config = toml::from_str(&content).with_context(|| "Failed to parse config")?;

    // If the file doesn't pin an explicit id, the deserialized id is a fresh
    // (volatile) uuid from the serde default. Replace it with the persisted one
    // so the agent keeps the same identity across restarts. An explicit id in
    // config.toml always wins.
    if !toml_has_explicit_id(&content) {
        cfg.id = persistent_id();
    }

    Ok(cfg)
}

/// Whether `content` (a config.toml body) sets a non-empty top-level `id`.
/// Used to decide if the persisted stable id should fill in instead.
fn toml_has_explicit_id(content: &str) -> bool {
    content
        .parse::<toml::Value>()
        .ok()
        .and_then(|v| {
            v.get("id")
                .and_then(|id| id.as_str())
                .map(|s| !s.trim().is_empty())
        })
        .unwrap_or(false)
}

/// Path to the persisted stable agent id.
fn id_path() -> PathBuf {
    dirs::data_dir()
        .map(|p| p.join("remote-agents").join("agent-id"))
        .unwrap_or_else(|| PathBuf::from("agent-id"))
}

/// Path to the cache file the npm launcher (`run.js`) writes when a newer
/// release is published. The launcher does the version comparison (it knows the
/// accurate *installed* package version — the compiled-in Cargo version can lag
/// the npm release), writing the newer version here and clearing it (empty)
/// once up to date. So this side only needs to read, never compare.
fn latest_version_path() -> PathBuf {
    dirs::data_dir()
        .map(|p| p.join("remote-agents").join("latest-version"))
        .unwrap_or_else(|| PathBuf::from("latest-version"))
}

/// Interpret the cache file body: a non-empty version string means an upgrade
/// is available; empty/whitespace/absent means up to date. Pure, so it is
/// unit-testable without the cache file.
fn update_available_from(cache_body: Option<&str>) -> Option<String> {
    let v = cache_body?.trim();
    if v.is_empty() {
        None
    } else {
        Some(v.to_string())
    }
}

/// The newer published version available for this host, if any (from the
/// launcher-written cache; see [`latest_version_path`]).
pub fn update_available() -> Option<String> {
    update_available_from(fs::read_to_string(latest_version_path()).ok().as_deref())
}

/// A stable agent id that survives restarts. Reads the id file if present and
/// non-empty; otherwise mints a new uuid, persists it, and returns it.
pub fn persistent_id() -> String {
    persistent_id_at(&id_path())
}

/// Testable core of [`persistent_id`], parameterised on the storage path.
fn persistent_id_at(path: &Path) -> String {
    if let Ok(existing) = fs::read_to_string(path) {
        let existing = existing.trim();
        if !existing.is_empty() {
            return existing.to_string();
        }
    }
    let id = uuid::Uuid::new_v4().to_string();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    // Best-effort persist; a write failure just means a new id next restart.
    let _ = fs::write(path, &id);
    id
}

/// Initialize default config file
pub fn init_config() -> Result<()> {
    let path = config_path();

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let config = Config::default();
    let content = toml::to_string_pretty(&config)?;

    fs::write(&path, content)?;

    println!("Config initialized at {:?}", path);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();

        assert!(!config.id.is_empty());
        assert!(!config.name.is_empty());
        assert_eq!(config.relay_url, "ws://127.0.0.1:8080");
        assert!(config.tags.is_empty());
        assert_eq!(config.security.mode, AgentMode::Plan);
        // The default relay is a neutral localhost, never a baked-in remote host.
        assert!(!config.relay_url.contains("workers.dev"));
    }

    #[test]
    fn redacted_toml_masks_secrets() {
        let mut cfg = Config {
            token: "supersecret-token".into(),
            ..Default::default()
        };
        cfg.security.encryption_key = Some("topsecret-key".into());

        let out = redacted_toml(&cfg).unwrap();
        assert!(!out.contains("supersecret-token"), "token leaked: {out}");
        assert!(!out.contains("topsecret-key"), "key leaked: {out}");
        assert!(out.contains("***"));

        // An empty token is not masked to *** (stays empty, signalling "unset").
        let empty = Config { token: String::new(), ..Default::default() };
        assert!(redacted_toml(&empty).unwrap().contains("token = \"\""));
    }

    #[test]
    fn apply_env_overrides_from_environment() {
        std::env::set_var("REMOTE_AGENTS_RELAY", "ws://relay.example:9000");
        std::env::set_var("REMOTE_AGENTS_ROOM", "envroom");
        std::env::set_var("REMOTE_AGENTS_TOKEN", "envtok");
        std::env::set_var("REMOTE_AGENTS_NAME", "envname");

        let mut cfg = Config::default();
        apply_env(&mut cfg);
        assert_eq!(cfg.relay_url, "ws://relay.example:9000");
        assert_eq!(cfg.room, "envroom");
        assert_eq!(cfg.token, "envtok");
        assert_eq!(cfg.name, "envname");

        // Empty/unset env must not clobber existing values.
        std::env::set_var("REMOTE_AGENTS_RELAY", "");
        let mut cfg2 = Config::default();
        let before = cfg2.relay_url.clone();
        apply_env(&mut cfg2);
        assert_eq!(cfg2.relay_url, before);

        for k in [
            "REMOTE_AGENTS_RELAY",
            "REMOTE_AGENTS_ROOM",
            "REMOTE_AGENTS_TOKEN",
            "REMOTE_AGENTS_NAME",
        ] {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn test_parse_minimal_config() {
        let toml = r#"
            room = "test-room"
            token = "secret123"
        "#;

        let config: Config = toml::from_str(toml).unwrap();

        assert_eq!(config.room, "test-room");
        assert_eq!(config.token, "secret123");
        // Defaults should be applied
        assert_eq!(config.security.mode, AgentMode::Plan);
    }

    #[test]
    fn test_parse_full_config() {
        let toml = r#"
            id = "custom-id"
            name = "my-agent"
            room = "production"
            token = "prod-token"
            relay_url = "wss://custom.relay.com"
            tags = ["backend", "api"]

            [security]
            mode = "edit"
            backup_enabled = true
            max_backup_versions = 5

            [autonomous]
            enabled = true
            timeout = 7200
        "#;

        let config: Config = toml::from_str(toml).unwrap();

        assert_eq!(config.id, "custom-id");
        assert_eq!(config.name, "my-agent");
        assert_eq!(config.room, "production");
        assert_eq!(config.tags, vec!["backend", "api"]);
        assert_eq!(config.security.mode, AgentMode::Edit);
        assert_eq!(config.security.max_backup_versions, 5);
        assert_eq!(config.autonomous.enabled, Some(true));
        assert_eq!(config.autonomous.timeout, 7200);
    }

    #[test]
    fn test_security_defaults() {
        let sec = SecurityConfig::default();

        assert_eq!(sec.mode, AgentMode::Plan);
        assert!(sec.backup_enabled);
        assert!(!sec.denied_commands.is_empty());
        assert!(!sec.denied_paths.is_empty());
        assert!(sec.denied_commands.iter().any(|c| c.contains("rm -rf /")));
    }

    #[test]
    fn autonomous_available_detects_runner_on_path() {
        // Explicit override wins over detection.
        let forced_on = AutonomousConfig { enabled: Some(true), runner: vec!["nope-xyz".into()], ..Default::default() };
        assert!(autonomous_available(&forced_on));
        let forced_off = AutonomousConfig { enabled: Some(false), runner: vec!["sh".into()], ..Default::default() };
        assert!(!autonomous_available(&forced_off));

        // Auto-detect (enabled = None): a program that's always on PATH vs a bogus one.
        let present = AutonomousConfig { enabled: None, runner: vec!["sh".into()], ..Default::default() };
        assert!(autonomous_available(&present), "sh should be on PATH");
        let absent = AutonomousConfig { enabled: None, runner: vec!["definitely-not-a-real-binary-xyz".into()], ..Default::default() };
        assert!(!autonomous_available(&absent));

        // Empty runner → not available.
        let empty = AutonomousConfig { enabled: None, runner: vec![], ..Default::default() };
        assert!(!autonomous_available(&empty));

        // Absolute path: existing executable vs missing.
        let abs_ok = AutonomousConfig { enabled: None, runner: vec!["/bin/sh".into()], ..Default::default() };
        assert!(autonomous_available(&abs_ok));
        let abs_no = AutonomousConfig { enabled: None, runner: vec!["/no/such/bin".into()], ..Default::default() };
        assert!(!autonomous_available(&abs_no));
    }

    #[test]
    fn test_autonomous_defaults() {
        let auto = AutonomousConfig::default();

        assert_eq!(auto.enabled, None); // None = auto-detect by runner-on-PATH
        assert_eq!(auto.runner, vec!["claude", "-p"]);
        assert_eq!(auto.timeout, 3600);
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let config = Config::default();
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: Config = toml::from_str(&toml_str).unwrap();

        assert_eq!(config.relay_url, parsed.relay_url);
        assert_eq!(config.security.mode, parsed.security.mode);
    }

    #[test]
    fn persistent_id_generates_then_reuses() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "agent-id-test-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        let _ = fs::remove_file(&path);

        // First call mints + persists an id.
        let first = persistent_id_at(&path);
        assert!(!first.is_empty());
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), first);

        // Subsequent calls return the SAME id (stable across restarts).
        assert_eq!(persistent_id_at(&path), first);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn persistent_id_regenerates_on_empty_file() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "agent-id-empty-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        // A blank/whitespace file must not be treated as a valid id.
        fs::write(&path, "  \n").unwrap();
        let id = persistent_id_at(&path);
        assert!(!id.is_empty());
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), id);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn update_available_from_reads_cache_body() {
        // A non-empty cached version means an upgrade is available.
        assert_eq!(update_available_from(Some("0.1.2")), Some("0.1.2".to_string()));
        // Whitespace is trimmed.
        assert_eq!(update_available_from(Some(" 0.2.0 \n")), Some("0.2.0".to_string()));
        // Empty / whitespace-only / absent cache → up to date.
        assert_eq!(update_available_from(Some("")), None);
        assert_eq!(update_available_from(Some("   \n")), None);
        assert_eq!(update_available_from(None), None);
    }

    #[test]
    fn toml_explicit_id_detection() {
        assert!(toml_has_explicit_id("id = \"abc\"\nname = \"x\"\n"));
        // Absent id → not explicit (persisted id should fill in).
        assert!(!toml_has_explicit_id("name = \"x\"\n"));
        // Empty id → not explicit.
        assert!(!toml_has_explicit_id("id = \"\"\n"));
        // Whitespace-only id → not explicit.
        assert!(!toml_has_explicit_id("id = \"   \"\n"));
        // Malformed toml → safely false.
        assert!(!toml_has_explicit_id("this is not toml ::: ="));
    }
}
