//! Self-hosted WebSocket relay library surface. The binary is a thin CLI over
//! [`router`]; exposing it as a library also lets integration tests spin the
//! server on an ephemeral port.

pub mod handler;
pub mod routing;
pub mod state;

use axum::{
    extract::{ws::WebSocketUpgrade, ConnectInfo, Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use state::RelayState;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

/// Build the relay's HTTP/WS router over the given shared state.
pub fn router(state: Arc<RelayState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/room/:room", get(room_info))
        .route("/ws/room/:room", get(ws_handler))
        .with_state(state)
}

#[derive(Deserialize)]
struct WsQuery {
    token: Option<String>,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Path(room): Path<String>,
    Query(q): Query<WsQuery>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    State(state): State<Arc<RelayState>>,
) -> Response {
    // The client's public IP, used to reflect a `YourEndpoint` for UDP
    // hole-punching. Prefer proxy headers (TLS is terminated by a proxy in the
    // recommended deployment), falling back to the direct TCP peer address.
    let client_ip = client_ip(&headers, peer);
    ws.on_upgrade(move |socket| handler::handle_socket(socket, room, q.token, client_ip, state))
}

/// Resolve the client IP: first `X-Forwarded-For` (leftmost), then `X-Real-IP`,
/// else the direct TCP peer.
fn client_ip(headers: &HeaderMap, peer: SocketAddr) -> IpAddr {
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        if let Some(first) = xff.split(',').next() {
            if let Ok(ip) = first.trim().parse::<IpAddr>() {
                return ip;
            }
        }
    }
    if let Some(ip) = headers
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<IpAddr>().ok())
    {
        return ip;
    }
    peer.ip()
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok", "service": "remote-agents-relay" }))
}

async fn room_info(
    Path(room): Path<String>,
    State(state): State<Arc<RelayState>>,
) -> impl IntoResponse {
    let (agents, mcp): (Vec<_>, usize) = match state.rooms.get(&room) {
        Some(r) => (r.agents.iter().map(|e| e.info.clone()).collect(), r.mcp.len()),
        None => (Vec::new(), 0),
    };
    Json(serde_json::json!({ "agents": agents, "mcp_clients": mcp }))
}
