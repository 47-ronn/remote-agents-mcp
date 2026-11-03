//! Self-hosted WebSocket relay library surface. The binary is a thin CLI over
//! [`router`]; exposing it as a library also lets integration tests spin the
//! server on an ephemeral port.

pub mod handler;
pub mod limiter;
pub mod routing;
pub mod state;

pub use limiter::{ct_eq, Limits, RateLimiter, Verdict};

use axum::{
    extract::{ws::WebSocketUpgrade, ConnectInfo, Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use remote_agents_shared::room_key;
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

/// Query params accepted by the HTTP info endpoints (`/api/room/:room`,
/// `/api/rooms`). The token may also travel in an `Authorization: Bearer`
/// header (preferred), so panels don't have to put the secret in the URL.
#[derive(Deserialize)]
struct InfoQuery {
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
    // The limiter key: the TCP peer unless the operator opted into trusting
    // proxy headers (behind a proxy every client shares the peer address, but
    // a direct client could spoof X-Forwarded-For to dodge per-IP budgets).
    let gate_ip = if state.trust_proxy { client_ip } else { peer.ip() };
    // Abuse gate: per-IP connection budget + auth-failure bans. Denied before
    // the upgrade, so a guess-flood never reaches the auth handshake. Only the
    // first and every 100th denial are logged (the limiter's `nth`), so a
    // flood stays visible without flooding the journal. No client IP in the
    // message — logs never carry addresses, only the opaque actor tag.
    if let Verdict::Deny { retry_after, nth } = state.limiter.gate_connection(gate_ip) {
        if nth == 1 || nth.is_multiple_of(100) {
            tracing::warn!(
                "rate-limited connection attempt (denial #{}, retry after {:?}) actor={}",
                nth,
                retry_after,
                state.actor(gate_ip)
            );
        }
        return rate_limited(retry_after).into_response();
    }
    // The room token, presented out-of-band: Authorization header preferred
    // (keeps the secret out of URLs → proxy/access logs), query param as the
    // legacy fallback. `None` means the auth frame carries it (header-mode
    // clients); the token-derived room key is computed post-auth in the
    // handler. A wrong token still derives a different key → isolated room.
    let provided = provided_token(q.token.as_ref(), &headers);
    // Echo a requested `bearer.*` subprotocol back in the 101 response — a
    // browser WebSocket that offered subprotocols FAILS the handshake when
    // the server selects none, and this subprotocol is the panel's
    // out-of-URL token channel.
    let bearer_protocols: Vec<String> = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .into_iter()
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .filter(|p| p.starts_with("bearer."))
        .map(str::to_string)
        .collect();
    let mut ws = ws.max_message_size(MAX_WS_MESSAGE).max_frame_size(MAX_WS_MESSAGE);
    if !bearer_protocols.is_empty() {
        ws = ws.protocols(bearer_protocols);
    }
    ws.on_upgrade(move |socket| {
            handler::handle_socket(socket, room, provided, client_ip, gate_ip, state)
        })
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
/// When the relay runs with a server token (`--token`) this operator endpoint
/// requires it; otherwise it stays open but exposes only names + counts (never
/// a roster). Rooms show their human names; a room appearing TWICE under one
/// name means a mis-keyed token group that never reached the real room.
async fn rooms_list(
    Query(q): Query<InfoQuery>,
    headers: HeaderMap,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    State(state): State<Arc<RelayState>>,
) -> Result<Json<serde_json::Value>, HttpErr> {
    let gate_ip = if state.trust_proxy { client_ip(&headers, peer) } else { peer.ip() };
    if let Verdict::Deny { retry_after, nth } = state.limiter.gate_connection(gate_ip) {
        if nth == 1 || nth.is_multiple_of(100) {
            tracing::warn!(
                "rate-limited rooms listing (denial #{}, retry after {:?}) actor={}",
                nth,
                retry_after,
                state.actor(gate_ip)
            );
        }
        return Err(rate_limited(retry_after));
    }
    if let Some(server) = &state.token {
        match provided_token(q.token.as_ref(), &headers) {
            Some(t) if ct_eq(&t, server) => {}
            _ => {
                state.limiter.record_failure(gate_ip);
                tracing::warn!(
                    "auth failed on rooms listing (bad server token) actor={}",
                    state.actor(gate_ip)
                );
                return Err(HttpErr(StatusCode::UNAUTHORIZED, "invalid token".to_string(), None));
            }
        }
    }
    let mut rooms: Vec<_> = state
        .rooms
        .iter()
        .map(|e| {
            let room = e.value();
            serde_json::json!({
                "room": room.name,
                "connections": room.agents.len(),
                "agents": crate::routing::dedup_agents(room).len(),
                "mcp_clients": room.mcp.len(),
            })
        })
        .collect();
    // Stable ordering for clients/tests.
    rooms.sort_by(|a, b| a["room"].as_str().cmp(&b["room"].as_str()));
    Ok(Json(serde_json::json!({ "rooms": rooms })))
}

/// One room's deduped roster + observer count — the endpoint the panel polls.
/// The caller MUST present the room token (query `?token=` or
/// `Authorization: Bearer`): it both derives the token-addressed room key and,
/// under a server token (`--token`), must equal it. A wrong token derives a
/// different key → an empty roster, never the real room's metadata.
async fn room_info(
    Path(room): Path<String>,
    Query(q): Query<InfoQuery>,
    headers: HeaderMap,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    State(state): State<Arc<RelayState>>,
) -> Result<Json<serde_json::Value>, HttpErr> {
    let gate_ip = if state.trust_proxy { client_ip(&headers, peer) } else { peer.ip() };
    if let Verdict::Deny { retry_after, nth } = state.limiter.gate_connection(gate_ip) {
        if nth == 1 || nth.is_multiple_of(100) {
            tracing::warn!(
                "rate-limited room info request (denial #{}, retry after {:?}) actor={}",
                nth,
                retry_after,
                state.actor(gate_ip)
            );
        }
        return Err(rate_limited(retry_after));
    }
    let key = http_room_key(&state, &room, &q, &headers, gate_ip)?;
    // Dedup to one entry per machine (a host with several terminals is one
    // logical peer), carrying the relay-computed `connections` count — matching
    // the worker's `/info` so panels behave the same on either relay.
    let (agents, mcp) = match state.rooms.get(&key) {
        Some(r) => (crate::routing::dedup_agents(r.value()), r.mcp.len()),
        None => (Vec::new(), 0),
    };
    Ok(Json(serde_json::json!({ "agents": agents, "mcp_clients": mcp })))
}

/// Extract the caller's room token, in order of preference:
/// 1. `Authorization: Bearer <token>` header (non-browser clients);
/// 2. `Sec-WebSocket-Protocol: bearer.<base64url(token)>` — the ONLY header a
///    browser `WebSocket` can set, so the panel can keep its token out of the
///    URL (URLs land in proxy/access logs; subprotocols do not);
/// 3. the query param (legacy).
fn provided_token(query_token: Option<&String>, headers: &HeaderMap) -> Option<String> {
    if let Some(t) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        return Some(t.to_string());
    }
    if let Some(p) = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("bearer."))
    {
        if let Some(t) = b64url_decode(p) {
            return Some(t);
        }
    }
    query_token.cloned()
}

/// Minimal URL-safe base64 decoder for the subprotocol-carried token (the
/// room token is arbitrary printable ASCII; a strict standard alphabet
/// decoder would reject `+/=`). Returns None on non-base64url input — the
/// caller then falls through to the query param instead of failing closed on
/// a subprotocol it does not understand.
fn b64url_decode(s: &str) -> Option<String> {
    let s = s.trim_end_matches('=');
    let mut out = Vec::with_capacity(s.len() * 3 / 4 + 3);
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    for &b in s.as_bytes() {
        let v = match b {
            b'A'..=b'Z' => (b - b'A') as u32,
            b'a'..=b'z' => (b - b'a' + 26) as u32,
            b'0'..=b'9' => (b - b'0' + 52) as u32,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        };
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    String::from_utf8(out).ok()
}

/// Compact error for the HTTP info endpoints (a full `Response` as the `Err`
/// variant trips clippy's result-large-err; this is 3 words). Converts into a
/// JSON error response, optionally with a `Retry-After` header.
struct HttpErr(StatusCode, String, Option<u32>);

impl IntoResponse for HttpErr {
    fn into_response(self) -> Response {
        let HttpErr(status, msg, retry_after) = self;
        match retry_after {
            Some(secs) => (
                status,
                [(header::RETRY_AFTER, secs.to_string())],
                Json(serde_json::json!({ "error": msg })),
            )
                .into_response(),
            None => (status, Json(serde_json::json!({ "error": msg }))).into_response(),
        }
    }
}

/// 429 with a `Retry-After` header, per the limiter's verdict.
fn rate_limited(retry_after: std::time::Duration) -> HttpErr {
    HttpErr(
        StatusCode::TOO_MANY_REQUESTS,
        "rate limited; retry later".to_string(),
        Some(retry_after.as_secs().max(1) as u32),
    )
}

/// HTTP endpoints present the room token too: it is required to derive the
/// room key (rooms are token-addressed), and when the relay runs with a server
/// token (`--token`) the presented token must additionally equal it (constant
/// time). In room-token mode a minimum length may be enforced — a short token
/// is guessable, and this endpoint's empty-vs-populated roster would confirm
/// a correct guess.
fn http_room_key(
    state: &RelayState,
    room: &str,
    q: &InfoQuery,
    headers: &HeaderMap,
    gate_ip: IpAddr,
) -> Result<String, HttpErr> {
    let Some(token) = provided_token(q.token.as_ref(), headers) else {
        return Err(HttpErr(
            StatusCode::UNAUTHORIZED,
            "token required to address a room".to_string(),
            None,
        ));
    };
    if let Some(server) = &state.token {
        if !ct_eq(&token, server) {
            state.limiter.record_failure(gate_ip);
            tracing::warn!(
                "auth failed on room info (bad server token) actor={}",
                state.actor(gate_ip)
            );
            return Err(HttpErr(StatusCode::UNAUTHORIZED, "invalid token".to_string(), None));
        }
    } else if state.min_token_len > 0 && token.len() < state.min_token_len {
        state.limiter.record_failure(gate_ip);
        // Only the length, never the token value or the client address.
        tracing::warn!(
            "auth failed on room info (room token too short: {} < {} chars) actor={}",
            token.len(),
            state.min_token_len,
            state.actor(gate_ip)
        );
        return Err(HttpErr(
            StatusCode::UNAUTHORIZED,
            format!(
                "room token too short: {} chars < {} required \
                 (start the relay with --min-token-len 0 for dev/tokenless rooms)",
                token.len(),
                state.min_token_len
            ),
            None,
        ));
    }
    Ok(room_key(room, &token))
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

    // base64url("A8q3skmiGv4Ybi2G05tMaSdma=Mo00") — a real-shaped room token
    // (printable ASCII incl. '=' and '%'-style specials survive the encode).
    #[test]
    fn provided_token_from_subprotocol_header() {
        let mut h = HeaderMap::new();
        h.insert(
            "sec-websocket-protocol",
            "bearer.QThxM3NrbWlHdjRZYmkyRzA1dE1hU2RtYT1NbzAw".parse().unwrap(),
        );
        assert_eq!(
            provided_token(None, &h).as_deref(),
            Some("A8q3skmiGv4Ybi2G05tMaSdma=Mo00")
        );
    }

    #[test]
    fn provided_token_prefers_authorization_over_subprotocol() {
        let mut h = HeaderMap::new();
        h.insert("authorization", "Bearer sekrit".parse().unwrap());
        h.insert("sec-websocket-protocol", "bearer.Zm9v".parse().unwrap());
        assert_eq!(provided_token(None, &h).as_deref(), Some("sekrit"));
    }

    #[test]
    fn provided_token_falls_through_on_non_bearer_subprotocol() {
        let mut h = HeaderMap::new();
        h.insert("sec-websocket-protocol", "chat, superchat".parse().unwrap());
        assert_eq!(
            provided_token(Some(&"legacy".to_string()), &h).as_deref(),
            Some("legacy")
        );
    }

    #[test]
    fn b64url_decode_rejects_invalid_and_non_utf8() {
        assert_eq!(b64url_decode("Zm9v"), Some("foo".to_string())); // sanity
        assert_eq!(b64url_decode(""), Some(String::new()));
        assert_eq!(b64url_decode("!!!"), None); // not base64url
        assert_eq!(b64url_decode("_w"), None); // 0xFF byte → invalid UTF-8
    }
}
