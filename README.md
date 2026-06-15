# Remote Agents

[![CI](https://github.com/ObsidianMotorman/tunshell_mcp_agents/actions/workflows/ci.yml/badge.svg)](https://github.com/ObsidianMotorman/tunshell_mcp_agents/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A unified, [MCP](https://modelcontextprotocol.io)-compatible system for
controlling fleets of remote machines through AI agents (Claude, opencode).
Agents connect outbound to a relay; an MCP server lets the AI run commands,
manage files, drive git, schedule tasks, and orchestrate the whole fleet — all
over end-to-end-encrypted channels.

## Features

- **Single Rust binary** (`remote-agent`) — runs as an agent daemon (`run`), an
  MCP stdio server (`mcp`), or installs itself as a service (`install`).
- **End-to-end encryption** (AES-GCM-256) on by default; the relay forwards only
  ciphertext.
- **Safety modes** per host — `plan` (read-only), `edit` (writes with backups),
  `bypass`, `disabled` — with path/command allow- & deny-lists.
- **Fleet as one computer** — run any operation (`exec`/`read`/`write`/`git`)
  across all agents, by tags, or by OS family; results aggregated per host.
- **Distributed MapReduce** — partition data across the fleet, map with a shell
  command, reduce the outputs, with per-partition retry.
- **Autonomous mode** — delegate AI tasks to a host that runs them with its own
  credentials (token-saving orchestration).
- **Two interchangeable relays** — Cloudflare Workers (Durable Objects) or a
  self-hosted Rust WebSocket relay; switch by changing `relay_url`.
- **Direct UDP data channel** with hole-punching and WebSocket fallback.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                  opencode / Claude Desktop                   │
│   remote-agent mcp  (Rust binary, MCP stdio server)          │
└───────────────────────────────┬──────────────────────────────┘
                                 │ wss:// (CF Worker or self-hosted relay)
                                 ▼
              ┌───────────────────────────────────┐
              │   Relay  (rooms route by token)   │
              └───────────────────────────────────┘
                 ▲              ▲              ▲
                 │ wss          │ wss          │ wss
          ┌──────┴─────┐ ┌──────┴─────┐ ┌──────┴─────┐
          │   Agent    │ │   Agent    │ │   Agent    │
          │ (daemon)   │ │ (daemon)   │ │ (daemon)   │
          └────────────┘ └────────────┘ └────────────┘
```

## Workspace layout

| Crate / dir            | Purpose                                                   |
|------------------------|-----------------------------------------------------------|
| `crates/shared`        | Wire protocol, AES-GCM crypto, UDP channel types          |
| `crates/mcp-server`    | The `remote-agent` binary: agent, MCP server, executors   |
| `crates/relay`         | Self-hosted Rust WebSocket relay (`remote-agents-relay`)  |
| `worker/`              | Cloudflare Worker relay (Durable Objects)                 |

## Install

```bash
# Via npm (downloads the prebuilt binary for your platform)
npm install -g remote-agents        # then: remote-agents --help
# or run on demand:
npx remote-agents mcp --help
```

```bash
# From source
cargo build --release --workspace
cargo install --path crates/mcp-server   # → ~/.cargo/bin/remote-agent
```

Prebuilt binaries for macOS / Linux / Windows are also attached to each GitHub
release.

## Quick start

### 1. Run an agent on a remote host

```bash
remote-agent run --relay wss://<your-relay-host> --room dev --token <secret>
# install as a user service (systemd / launchd) instead:
remote-agent install --room dev --token <secret> --relay wss://<your-relay-host>
```

### 2. Choose a relay

**Self-hosted (Rust):**

```bash
remote-agents-relay --bind 0.0.0.0:8080
# agents/MCP then use relay_url = ws://<host>:8080
```

**Cloudflare Worker:**

```bash
cd worker
npm install
CLOUDFLARE_API_TOKEN=<token> npx wrangler deploy
# → wss://<your-worker-subdomain>.workers.dev
```

### 3. Wire up the MCP server (Claude Desktop / opencode)

The MCP server is the same `remote-agent` binary in `mcp` mode (stdio):

```json
{
  "mcpServers": {
    "remote-agents": {
      "command": "/path/to/remote-agent",
      "args": [
        "mcp",
        "--relay", "wss://<your-relay-host>",
        "--room", "myroom",
        "--token", "<secret>"
      ]
    }
  }
}
```

Connection settings are resolved as **CLI flag > env var > `config.toml` >
default**, so you can instead supply them via `env` in the MCP config:

```json
{
  "mcpServers": {
    "remote-agents": {
      "command": "/path/to/remote-agent",
      "args": ["mcp"],
      "env": {
        "REMOTE_AGENTS_RELAY": "wss://<your-relay-host>",
        "REMOTE_AGENTS_ROOM": "myroom",
        "REMOTE_AGENTS_TOKEN": "<secret>"
      }
    }
  }
}
```

Without any relay/room/token the server runs locally only (no remote agent
control); nothing points at a hosted endpoint by default.

## MCP tools

| Tool | Description |
|------|-------------|
| `exec` | Run a shell command (locally or on a remote agent via `agent_id`) |
| `read_file` / `write_file` / `list_dir` | File operations (write requires Edit/Bypass) |
| `get_info` / `set_mode` | Inspect / change an agent's mode at runtime |
| `git_status` / `git_pull` / `git_commit` / `git_push` | Git operations |
| `schedule_add` / `schedule_remove` / `schedule_list` | Cron-style tasks on a host |
| `task_dispatch` / `task_get` / `task_list` / `task_wait` | Autonomous AI tasks run with the host's own credentials |
| `list_agents` | List agents connected to the relay room |
| `fleet_exec` / `fleet_read` / `fleet_write` / `fleet_git` | Run an operation across the fleet — `target = all \| tag1,tag2 \| os:<family>` |
| `mapreduce` | Distributed map/reduce over the fleet (shell map/reduce functions) |

Each agent advertises platform metadata (OS family, distro, kernel, shell) and
is aware of its peers, so the orchestrator can target hosts by OS and tailor
commands per platform.

## Security modes

| Mode | Behavior |
|------|----------|
| `plan` | Read-only (read, ls, git status, safe exec) |
| `edit` | Writes allowed, with automatic backups |
| `bypass` | Unrestricted |
| `disabled` | Agent rejects all operations |

Command payloads are encrypted end-to-end (AES-GCM-256) with a key derived from
the room token (or an explicit `encryption_key`); the relay only ever sees
ciphertext. A hard deny-list applies even in `bypass` mode.

## Development

```bash
cargo test --workspace                          # unit + integration tests
cargo clippy --workspace --all-targets -- -D warnings
cargo run --release -p remote-agents-relay -- --bind 127.0.0.1:8080
(cd worker && npx tsc --noEmit -p .)            # worker typecheck

# Fuzzing (nightly + cargo-fuzz)
cargo +nightly fuzz run <target> --fuzz-dir crates/mcp-server/fuzz
```

CI (`.github/workflows/ci.yml`) runs the test suite, Clippy (deny-warnings), and
the worker typecheck on every push and pull request.

## License

MIT
