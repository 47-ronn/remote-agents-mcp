//! Remote Agent - connects to relay and executes commands
//!
//! Modes of operation:
//! - `run`: Connect to relay room as a peer agent
//! - `mcp`: Run as MCP stdio server for local AI tools (Claude, opencode)
//! - `hybrid`: Both relay connection AND MCP server (for full integration)

use anyhow::Result;
use clap::{Parser, Subcommand};
use remote_agent::{config, daemon, install_mcp, mcp_server};
use remote_agent::connection;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "remote-agent")]
#[command(version)]
#[command(about = "Remote agent for distributed shell access")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the agent (connect to relay)
    Run {
        /// Room name to join
        #[arg(short, long)]
        room: Option<String>,

        /// Authentication token
        #[arg(short, long)]
        token: Option<String>,

        /// Relay server URL
        #[arg(long)]
        relay: Option<String>,

        /// Agent name (default: hostname)
        #[arg(short, long)]
        name: Option<String>,

        /// Tags for this agent (comma-separated)
        #[arg(long)]
        tags: Option<String>,

        /// Send-only peer: stay visible and dispatch work, but NEVER execute
        /// commands from other peers (for prod controllers / browser dashboards).
        #[arg(long)]
        no_agent: bool,
    },

    /// Run as MCP stdio server (for Claude Desktop, opencode, etc.). The node
    /// also joins the room as a FULL peer (executes commands from others) unless
    /// `--no-agent` is given.
    Mcp {
        /// Agent name (default: hostname)
        #[arg(short, long)]
        name: Option<String>,

        /// Room name to join (enables remote agent control)
        #[arg(short, long)]
        room: Option<String>,

        /// Authentication token for relay
        #[arg(short, long)]
        token: Option<String>,

        /// Relay server URL
        #[arg(long)]
        relay: Option<String>,

        /// Tags for this node as a peer (comma-separated)
        #[arg(long)]
        tags: Option<String>,

        /// Send-only peer: dispatch work but never execute others' commands.
        #[arg(long)]
        no_agent: bool,
    },

    /// Alias for `mcp` (kept for compatibility): `mcp` mode is already a full
    /// peer that executes commands AND serves the local AI over one connection.
    Hybrid {
        /// Agent name (default: hostname)
        #[arg(short, long)]
        name: Option<String>,

        /// Room name to join
        #[arg(short, long)]
        room: Option<String>,

        /// Authentication token for relay
        #[arg(short, long)]
        token: Option<String>,

        /// Relay server URL
        #[arg(long)]
        relay: Option<String>,

        /// Tags for this node as an agent (comma-separated)
        #[arg(long)]
        tags: Option<String>,

        /// Send-only peer: dispatch work but never execute others' commands.
        #[arg(long)]
        no_agent: bool,
    },

    /// Generate default config file
    Init,

    /// Show current configuration
    Config,

    /// Install the agent as an auto-starting background service
    Install {
        /// Room name to bake into the service command line
        #[arg(short, long)]
        room: Option<String>,

        /// Authentication token to bake into the service command line
        #[arg(short, long)]
        token: Option<String>,

        /// Relay server URL
        #[arg(long)]
        relay: Option<String>,
    },

    /// Remove the installed background service
    Uninstall,

    /// Launch the agent as a detached background process (Windows only).
    /// Simpler than a full service install but won't auto-restart on crash.
    Launch {
        /// Room name to join
        #[arg(short, long)]
        room: Option<String>,

        /// Authentication token
        #[arg(short, long)]
        token: Option<String>,

        /// Relay server URL
        #[arg(long)]
        relay: Option<String>,
    },

    /// Register this binary as an MCP server in a popular AI agent's config
    /// (Claude Desktop/Code, Cursor, Cline, Roo, Kilo, Windsurf, Zed, opencode,
    /// Continue, Goose). Connection flags are baked into the server's args.
    InstallMcp {
        /// Target client id (omit to list supported clients)
        #[arg(short, long)]
        client: Option<String>,

        /// Name to register the server under in the client config
        #[arg(long, default_value = "remote-agents")]
        server_name: String,

        /// Room name to bake into the MCP server command
        #[arg(short, long)]
        room: Option<String>,

        /// Authentication token to bake into the MCP server command
        #[arg(short, long)]
        token: Option<String>,

        /// Relay server URL
        #[arg(long)]
        relay: Option<String>,

        /// Agent name for this node as a peer
        #[arg(short, long)]
        name: Option<String>,

        /// Tags for this node as a peer (comma-separated)
        #[arg(long)]
        tags: Option<String>,

        /// Register as a send-only controller (never executes others' commands)
        #[arg(long)]
        no_agent: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize TLS provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("remote_agent=info".parse()?)
                .add_directive("tokio_tungstenite=warn".parse()?),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            room,
            token,
            relay,
            name,
            tags,
            no_agent,
        } => {
            run_agent(room, token, relay, name, tags, no_agent).await?;
        }
        Commands::Mcp { name, room, token, relay, tags, no_agent } => {
            run_mcp(name, room, token, relay, tags, no_agent).await?;
        }
        Commands::Hybrid { name, room, token, relay, tags, no_agent } => {
            // `mcp` mode is already a full peer (executes + serves MCP), so hybrid
            // is just an alias for it.
            run_mcp(name, room, token, relay, tags, no_agent).await?;
        }
        Commands::Init => {
            config::init_config()?;
        }
        Commands::Config => {
            let cfg = config::load_config()?;
            println!("{}", config::redacted_toml(&cfg)?);
        }
        Commands::Install { room, token, relay } => {
            let mut args = Vec::new();
            if let Some(room) = room {
                args.push("--room".to_string());
                args.push(room);
            }
            if let Some(token) = token {
                args.push("--token".to_string());
                args.push(token);
            }
            if let Some(relay) = relay {
                args.push("--relay".to_string());
                args.push(relay);
            }
            daemon::install(&args)?;
        }
        Commands::Uninstall => {
            daemon::uninstall()?;
        }
        Commands::Launch { room, token, relay } => {
            let mut args = Vec::new();
            if let Some(room) = room {
                args.push("--room".to_string());
                args.push(room);
            }
            if let Some(token) = token {
                args.push("--token".to_string());
                args.push(token);
            }
            if let Some(relay) = relay {
                args.push("--relay".to_string());
                args.push(relay);
            }
            daemon::launch_detached(&args)?;
        }
        Commands::InstallMcp {
            client,
            server_name,
            room,
            token,
            relay,
            name,
            tags,
            no_agent,
        } => {
            let Some(client) = client else {
                println!("Supported clients:\n{}", install_mcp::supported_clients());
                return Ok(());
            };
            // Build the MCP server's argv: `mcp` plus the connection flags, in
            // the same form the README documents for a hand-written config.
            let mut args = vec!["mcp".to_string()];
            let mut push = |flag: &str, val: Option<String>| {
                if let Some(v) = val {
                    args.push(flag.to_string());
                    args.push(v);
                }
            };
            push("--relay", relay);
            push("--room", room);
            push("--token", token);
            push("--name", name);
            push("--tags", tags);
            if no_agent {
                args.push("--no-agent".to_string());
            }
            let exe = std::env::current_exe()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| "remote-agents".to_string());
            install_mcp::install_mcp(&client, &server_name, &exe, &args)?;
        }
    }

    Ok(())
}

/// Build the effective config from CLI overrides.
/// Precedence: CLI flag > env var > config.toml > built-in default.
fn build_config(
    room: Option<String>,
    token: Option<String>,
    relay: Option<String>,
    name: Option<String>,
    tags: Option<String>,
    no_agent: bool,
) -> config::Config {
    let mut cfg = config::load_config().unwrap_or_default();
    config::apply_env(&mut cfg);

    if let Some(room) = room {
        cfg.room = room;
    }
    if let Some(token) = token {
        cfg.token = token;
    }
    if let Some(relay) = relay {
        cfg.relay_url = relay;
    }
    if let Some(name) = name {
        cfg.name = name;
    }
    if let Some(tags) = tags {
        cfg.tags = tags.split(',').map(|s| s.trim().to_string()).collect();
    }
    // --no-agent overrides to send-only; otherwise keep config/default (true).
    if no_agent {
        cfg.accepts_commands = false;
    }
    cfg
}

async fn run_agent(
    room: Option<String>,
    token: Option<String>,
    relay: Option<String>,
    name: Option<String>,
    tags: Option<String>,
    no_agent: bool,
) -> Result<()> {
    let cfg = build_config(room, token, relay, name, tags, no_agent);

    info!(
        "Starting agent '{}' connecting to room '{}' at {}",
        cfg.name, cfg.room, cfg.relay_url
    );

    // Connect and run
    connection::run(&cfg).await
}

/// Hybrid: run the agent connection (Agent role — this node is visible and
/// executes commands) AND the MCP server (Mcp role — controls the fleet via the
/// local AI) concurrently, from one process. The MCP stdio server drives the
/// process lifetime (it returns when its client closes stdin); the agent side
/// is then aborted.
async fn run_mcp(
    name: Option<String>,
    room: Option<String>,
    token: Option<String>,
    relay: Option<String>,
    tags: Option<String>,
    no_agent: bool,
) -> Result<()> {
    let cfg = build_config(room, token, relay, name, tags, no_agent);

    // MCP stdio server + full peer (executes commands from the room unless
    // --no-agent). The single relay connection both serves and executes.
    mcp_server::run_mcp_server(&cfg).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hybrid_subcommand_parses_with_room_and_tags() {
        let cli = Cli::try_parse_from([
            "remote-agent",
            "hybrid",
            "--room",
            "gpu",
            "--token",
            "t",
            "--tags",
            "macos,dev",
        ])
        .expect("hybrid args should parse");
        match cli.command {
            Commands::Hybrid { room, token, tags, .. } => {
                assert_eq!(room.as_deref(), Some("gpu"));
                assert_eq!(token.as_deref(), Some("t"));
                assert_eq!(tags.as_deref(), Some("macos,dev"));
            }
            other => panic!("expected Hybrid, got {other:?}"),
        }
    }

    #[test]
    fn build_config_applies_cli_overrides() {
        let cfg = build_config(
            Some("room1".into()),
            Some("tok".into()),
            Some("ws://r".into()),
            Some("node-x".into()),
            Some("a, b ,c".into()),
            false,
        );
        assert_eq!(cfg.room, "room1");
        assert_eq!(cfg.token, "tok");
        assert_eq!(cfg.relay_url, "ws://r");
        assert_eq!(cfg.name, "node-x");
        // Tags are split and trimmed.
        assert_eq!(cfg.tags, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        // Default: a peer accepts commands.
        assert!(cfg.accepts_commands);
    }

    #[test]
    fn no_agent_flag_makes_node_send_only() {
        let cfg = build_config(None, None, None, None, None, true);
        assert!(!cfg.accepts_commands, "--no-agent must disable command execution");
    }

    #[test]
    fn run_subcommand_parses_no_agent() {
        let cli = Cli::try_parse_from(["remote-agent", "run", "--room", "gpu", "--no-agent"])
            .expect("run --no-agent should parse");
        match cli.command {
            Commands::Run { no_agent, room, .. } => {
                assert!(no_agent);
                assert_eq!(room.as_deref(), Some("gpu"));
            }
            other => panic!("expected Run, got {other:?}"),
        }
    }
}
