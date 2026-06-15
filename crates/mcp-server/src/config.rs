//! Agent configuration

use anyhow::{Context, Result};
use remote_agents_shared::AgentMode;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

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

    /// Security settings
    #[serde(default)]
    pub security: SecurityConfig,

    /// Autonomous AI task execution settings
    #[serde(default)]
    pub autonomous: AutonomousConfig,
}

/// Per-host autonomous AI runner configuration.
///
/// When `enabled`, the host can accept delegated AI tasks and run them with its
/// OWN credentials. The runner inherits the agent process environment (the
/// host's existing `claude`/CLI login, `ANTHROPIC_API_KEY`, etc.) — the agent
/// never stores any keys itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutonomousConfig {
    /// Enable autonomous task execution on this host.
    #[serde(default)]
    pub enabled: bool,

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
            enabled: false,
            runner: default_runner(),
            workdir: None,
            timeout: default_task_timeout(),
        }
    }
}

fn default_runner() -> Vec<String> {
    vec!["claude".to_string(), "-p".to_string()]
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

/// Get config file path
pub fn config_path() -> PathBuf {
    dirs::config_dir()
        .map(|p| p.join("remote-agents").join("config.toml"))
        .unwrap_or_else(|| PathBuf::from("~/.config/remote-agents/config.toml"))
}

/// Load configuration from file
pub fn load_config() -> Result<Config> {
    let path = config_path();

    if !path.exists() {
        return Ok(Config::default());
    }

    let content = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config from {:?}", path))?;

    toml::from_str(&content).with_context(|| "Failed to parse config")
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
        assert!(config.autonomous.enabled);
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
    fn test_autonomous_defaults() {
        let auto = AutonomousConfig::default();

        assert!(!auto.enabled);
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
}
