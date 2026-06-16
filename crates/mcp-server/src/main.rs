//! Remote Agent - connects to relay and executes commands
//!
//! Modes of operation:
//! - `run`: Connect to relay room as a peer agent
//! - `mcp`: Run as MCP stdio server for local AI tools (Claude, opencode)
//! - `hybrid`: Both relay connection AND MCP server (for full integration)

use anyhow::Result;
use clap::{Parser, Subcommand};
use remote_agent::{config, daemon, mcp_server};
use remote_agent::connection;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "remote-agent")]
#[command(about = "Remote agent for distributed shell access")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
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
    },

    /// Run as MCP stdio server (for Claude Desktop, opencode, etc.)
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
        } => {
            run_agent(room, token, relay, name, tags).await?;
        }
        Commands::Mcp { name, room, token, relay } => {
            run_mcp(name, room, token, relay).await?;
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
    }

    Ok(())
}

async fn run_agent(
    room: Option<String>,
    token: Option<String>,
    relay: Option<String>,
    name: Option<String>,
    tags: Option<String>,
) -> Result<()> {
    // Config precedence: CLI flag > env var > config.toml > default.
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

    info!(
        "Starting agent '{}' connecting to room '{}' at {}",
        cfg.name, cfg.room, cfg.relay_url
    );

    // Connect and run
    connection::run(&cfg).await
}

async fn run_mcp(
    name: Option<String>,
    room: Option<String>,
    token: Option<String>,
    relay: Option<String>,
) -> Result<()> {
    // Config precedence: CLI flag > env var > config.toml > default.
    let mut cfg = config::load_config().unwrap_or_default();
    config::apply_env(&mut cfg);

    if let Some(name) = name {
        cfg.name = name;
    }
    if let Some(room) = room {
        cfg.room = room;
    }
    if let Some(token) = token {
        cfg.token = token;
    }
    if let Some(relay) = relay {
        cfg.relay_url = relay;
    }

    // MCP mode with optional relay connection for remote agent control
    mcp_server::run_mcp_server(&cfg).await
}
