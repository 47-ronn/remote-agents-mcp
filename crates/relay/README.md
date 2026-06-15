# remote-agents-relay

Self-hosted WebSocket relay — a drop-in alternative to the Cloudflare Worker
relay. Same protocol and URL scheme, so agents and the MCP server switch to it
by pointing `relay_url` at it. Avoids Cloudflare's per-request billing.

## Run

```bash
cargo run --release -p remote-agents-relay -- --bind 0.0.0.0:8080
# optional server-enforced token (all clients must use it):
cargo run --release -p remote-agents-relay -- --bind 0.0.0.0:8080 --token secret
```

Point clients at it (no code changes):

```bash
remote-agent run --relay ws://RELAY_HOST:8080 --room dev --token secret
# MCP server:
REMOTE_AGENTS_RELAY=ws://RELAY_HOST:8080 remote-agents-mcp
```

Endpoints: `GET /health`, `GET /api/room/:room`, `GET /ws/room/:room?token=…`.

## Design

- Multi-threaded tokio; sharded room/session registries (`DashMap`) — no global
  lock on the hot path.
- One bounded outbound channel per connection drained by a dedicated writer
  task, so routing never blocks on socket I/O. A consumer that can't keep up is
  dropped (bounded memory) rather than stalling the room.
- Reuses `remote_agents_shared` protocol types, so it stays in lock-step with
  the agent, MCP server, and the Cloudflare worker.
- Blind passthrough of command payloads/results — end-to-end encryption is
  unaffected (the relay never sees plaintext).

## TLS

The relay speaks plain `ws://`. For `wss://`, terminate TLS at a reverse proxy
or load balancer (nginx/caddy) in front of it.

## Scaling toward ~1M connections

~1M mostly-idle WebSocket connections is primarily a **memory/FD** challenge,
not CPU (control-plane messages are infrequent). On a single tuned node:

```bash
# File descriptors (per process and system-wide)
ulimit -n 1200000
sysctl -w fs.file-max=2000000
# Accept backlog
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_max_syn_backlog=65535
# TCP memory (tune to available RAM)
sysctl -w net.ipv4.tcp_mem='764145 1018863 1528290'
```

- `mimalloc` is the global allocator to reduce fragmentation at scale.
- Budget memory: a few KB per idle connection → ~1M ≈ single-digit GB RAM. Size
  the box accordingly.
- Use TCP keepalive and the app-level `ping`/`pong` (agents ping every 30s) to
  reap dead connections.

Beyond a single node, scale horizontally with multiple relays behind a
room-sticky load balancer, or add a pub/sub backplane (Redis/NATS) so a room's
agents and MCP clients can live on different nodes. That is out of scope for v1.

## Fuzzing

`fuzz/` holds a coverage-guided target (`cargo-fuzz` / libFuzzer) for the routing
logic — it builds a room from an arbitrary agent/tag set and resolves an
arbitrary target, asserting the result is always a subset of registered agents
with the expected cardinality (and never panics).

```bash
cd crates/relay
cargo +nightly fuzz build
cargo +nightly fuzz run routing -- -max_total_time=30
```

The untrusted WS-frame parsing path (`ClientMessage::from_json`) is additionally
covered by the agent crate's `protocol_client`/`protocol_server` targets, since
the relay reuses the same shared protocol types.
