//! Self-hosted Rust WebSocket relay — a drop-in alternative to the Cloudflare
//! Worker relay. Same protocol and URL scheme, so agents and the MCP server
//! switch to it by pointing `relay_url` at `ws://<host>:<port>`.

use clap::Parser;
use remote_agents_relay::{router, state::RelayState, Limits};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, warn};
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

    /// Minimum room-token length when no --token is configured: the room token
    /// is then the room's ONLY gate, and a short/weak one is guessable online
    /// (the roster answers "empty vs populated", confirming a correct guess).
    /// Shorter tokens — including empty dev-token rooms — are rejected.
    /// Default 32; set 0 to disable for token-less dev rooms.
    #[arg(long, default_value_t = 32)]
    min_token_len: usize,

    /// Max new connections per IP per 10 s (WebSocket upgrades plus HTTP info
    /// calls), as abuse gating against online token guessing: over-budget IPs
    /// get 429 + Retry-After, and repeated failed auths ban the IP for 5 min.
    /// Generous for legit fleets (a mass reconnect behind one NAT fits); 0
    /// disables the connection budget.
    #[arg(long, default_value_t = 60)]
    conn_rate: usize,

    /// Trust X-Forwarded-For / X-Real-IP when attributing connections for the
    /// per-IP limiter. Set ONLY behind a reverse proxy you control (otherwise
    /// every client shares the proxy's address); without it the TCP peer
    /// address is used and cannot be spoofed.
    #[arg(long, default_value_t = false)]
    trust_proxy: bool,
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
            "running WITHOUT --token: rooms are still token-addressed (a wrong-token \
             host cannot see another token group's roster), but there is no single \
             server secret — token strength is the only gate. Use --token in production \
             for one shared secret + loud auth_failed on mismatch."
        );
    }
    let state = Arc::new(
        RelayState::new(cli.token)
            .with_idle_timeout_secs(cli.idle_timeout_secs)
            .with_min_token_len(cli.min_token_len)
            .with_rate_limits(Limits {
                max_conns: cli.conn_rate,
                ..Default::default()
            })
            .with_trust_proxy(cli.trust_proxy),
    );

    if state.token.is_none() {
        if state.min_token_len > 0 {
            info!(
                "enforcing minimum room-token length: {} chars \
                 (shorter/empty tokens rejected; --min-token-len 0 to disable)",
                state.min_token_len
            );
        } else {
            warn!(
                "--min-token-len 0: short/empty room tokens are ACCEPTED — the \
                 token is each room's only gate; use long random tokens"
            );
        }
    }
    if state.limiter.limits().max_conns == 0 {
        warn!("--conn-rate 0: per-IP connection budget disabled (auth-failure bans stay active)");
    }

    let listener = tokio::net::TcpListener::bind(&cli.bind).await?;
    info!(
        "relay listening on ws://{}/ws/room/:room (idle reap {}s, conn budget {}/10s per IP{})",
        cli.bind,
        cli.idle_timeout_secs,
        state.limiter.limits().max_conns,
        if cli.trust_proxy { ", trusting proxy headers" } else { "" }
    );
    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
