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
    let state = Arc::new(RelayState::new(cli.token));

    let listener = tokio::net::TcpListener::bind(&cli.bind).await?;
    info!("relay listening on ws://{}/ws/room/:room", cli.bind);
    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
