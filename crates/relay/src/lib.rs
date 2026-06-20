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

/// Hard ceiling on a single inbound WebSocket message.
///
/// The relay accepts connections from untrusted clients; axum's default
/// (64 MiB) lets any one of them force the relay to buffer a 64 MiB frame before
/// `from_json` runs — a per-connection memory-amplification DoS. The relay only
/// ever routes small control messages plus opaque encrypted envelopes, which the
/// host caps at 900 KB to stay under the Cloudflare Workers' ~1 MiB WS limit.
/// Since this relay is a drop-in alternative to that worker, mirror the 1 MiB
/// cap: comfortably above any legitimate frame, far below the 64 MiB default.
const MAX_WS_MESSAGE: usize = 1024 * 1024;

/// Build the relay's HTTP/WS router over the given shared state.
pub fn router(state: Arc<RelayState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/rooms", get(rooms_list))
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
    ws.max_message_size(MAX_WS_MESSAGE)
        .max_frame_size(MAX_WS_MESSAGE)
        .on_upgrade(move |socket| handler::handle_socket(socket, room, q.token, client_ip, state))
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

/// List every active room with its connection / distinct-host / observer counts.
/// The Cloudflare worker stubs this out — Durable Objects have no global
/// registry — but the self-hosted relay holds all rooms in shared state, so it
/// can give operators a real fleet-wide view. `connections` counts raw sockets,
/// `agents` counts distinct machines (one host may hold several terminals).
async fn rooms_list(State(state): State<Arc<RelayState>>) -> impl IntoResponse {
    let mut rooms: Vec<_> = state
        .rooms
        .iter()
        .map(|e| {
            let room = e.value();
            serde_json::json!({
                "room": e.key(),
                "connections": room.agents.len(),
                "agents": crate::routing::dedup_agents(room).len(),
                "mcp_clients": room.mcp.len(),
            })
        })
        .collect();
    // Stable ordering for clients/tests.
    rooms.sort_by(|a, b| a["room"].as_str().cmp(&b["room"].as_str()));
    Json(serde_json::json!({ "rooms": rooms }))
}

async fn room_info(
    Path(room): Path<String>,
    State(state): State<Arc<RelayState>>,
) -> impl IntoResponse {
    // Dedup to one entry per machine (a host with several terminals is one
    // logical peer), carrying the relay-computed `connections` count — matching
    // the worker's `/info` so panels behave the same on either relay.
    let (agents, mcp) = match state.rooms.get(&room) {
        Some(r) => (crate::routing::dedup_agents(r.value()), r.mcp.len()),
        None => (Vec::new(), 0),
    };
    Json(serde_json::json!({ "agents": agents, "mcp_clients": mcp }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn peer() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 1234)
    }

    #[test]
    fn client_ip_prefers_first_x_forwarded_for() {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", "203.0.113.5, 70.0.0.9".parse().unwrap());
        assert_eq!(client_ip(&h, peer()), "203.0.113.5".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn client_ip_uses_x_real_ip_when_no_xff() {
        let mut h = HeaderMap::new();
        h.insert("x-real-ip", "198.51.100.7".parse().unwrap());
        assert_eq!(client_ip(&h, peer()), "198.51.100.7".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn client_ip_falls_back_to_tcp_peer() {
        assert_eq!(client_ip(&HeaderMap::new(), peer()), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
    }

    #[test]
    fn client_ip_ignores_malformed_xff() {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", "not-an-ip".parse().unwrap());
        // No usable header → TCP peer.
        assert_eq!(client_ip(&h, peer()), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
    }
}
