//! Self-hosted Rust WebSocket relay — a drop-in alternative to the Cloudflare
//! Worker relay. Same protocol and URL scheme, so agents and the MCP server
//! switch to it by pointing `relay_url` at `ws://<host>:<port>`.

use clap::Parser;
use remote_agents_relay::{router, state::RelayState};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "remote-agents-relay")]
#[command(about = "Self-hosted WebSocket relay for remote-agents")]
struct Cli {
    /// Address to bind, e.g. 0.0.0.0:8080
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: String,

    /// Optional server auth token. When set, every connection's auth token
    /// must equal it. Omit for Cloudflare-parity (token from query string).
    #[arg(long)]
    token: Option<String>,

    /// Close a connection idle (no frame received) for this many seconds.
    /// Agents ping every 30s, so the default reaps a silently-dead TCP after
    /// three missed pings. Set 0 to disable (rely on TCP keepalive instead).
    #[arg(long, default_value_t = 90)]
    idle_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("remote_agents_relay=info")),
        )
        .init();

    let cli = Cli::parse();
    if cli.token.is_none() {
        tracing::warn!(
            "running WITHOUT --token: room access is not gated at the relay (any \
             token-consistent client can join a room and read its metadata). \
             E2E encryption still protects payloads. Use --token in production."
        );
    }
    let state = Arc::new(RelayState::new(cli.token).with_idle_timeout_secs(cli.idle_timeout_secs));

    let listener = tokio::net::TcpListener::bind(&cli.bind).await?;
    info!(
        "relay listening on ws://{}/ws/room/:room (idle reap {}s)",
        cli.bind, cli.idle_timeout_secs
    );
    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
