# Remote Agents

[![CI](https://github.com/47-ronn/tunshell_mcp_agents/actions/workflows/ci.yml/badge.svg)](https://github.com/47-ronn/tunshell_mcp_agents/actions/workflows/ci.yml)
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

## Running: one binary, two ways

`remote-agents` is **one binary** that behaves the same whether you launch it
directly with flags or an AI host (opencode / Claude) starts it as an MCP
server. Connection settings resolve identically in both cases:
**CLI flag > `REMOTE_AGENTS_*` env var > `config.toml` > default**.

It is a flat **peer network** — there are no controller/agent roles. Every node
joins a relay room as an equal peer: visible to all, able to dispatch work, and
(unless `--no-agent`) able to execute commands from others.

| Mode | Command | The node… |
|------|---------|-----------|
| `run` | `remote-agents run …` | is a headless full peer (executes + dispatches), no local AI |
| `mcp` | `remote-agents mcp …` | is a full peer **plus** an MCP server for a local AI (opencode / Claude) |
| `hybrid` | `remote-agents hybrid …` | alias for `mcp` (kept for compatibility) |

Every mode is a **full peer that accepts commands by default**. Add `--no-agent`
to make a node **send-only** (stays visible and dispatches work, but never runs
others' commands — for prod controllers or browser dashboards). `--no-agent`
also works in an MCP `env` block as `REMOTE_AGENTS_*` config.

Common flags: `--relay <wss://host>` `--room <name>` `--token <secret>`
`--name <id>` `--tags a,b` `--no-agent`.

### Keeping a host always online

A `mcp` node lives only as long as the AI host (opencode / Claude) keeps it
running — close the session and the node leaves the room. For a host that should
stay in the fleet **24/7, independent of any AI session**, install it as a
background service running `run`:

```bash
remote-agents install --room dev --token <secret> --relay wss://<your-relay-host>
# systemd user service (Linux) / launchd LaunchAgent (macOS); auto-starts,
# survives logout/reboot, auto-restarts. Remove with: remote-agents uninstall
```

A machine has **one persistent identity** (`agent-id`), and the relay keys peers
by id, so don't run both a `run` service and an `mcp` session on the same machine
with the same id — they'd evict each other. Typical topology: target hosts run
the `run` service (always online); the workstation that drives the fleet runs
`mcp` per session.

## Quick start

### 1. Run an agent on a remote host (with flags)

```bash
# Install once (downloads the prebuilt binary for your platform):
npm install -g remote-agents

# Run as a peer agent:
remote-agents run --relay wss://<your-relay-host> --room dev --token <secret> \
  --name web-1 --tags backend

# ...or install it as an auto-starting user service (systemd / launchd):
remote-agents install --room dev --token <secret> --relay wss://<your-relay-host>
```

### 2. Choose a relay

**Self-hosted (Rust):**

```bash
remote-agents-relay --bind 0.0.0.0:8080
# agents/MCP then use relay_url = ws://<host>:8080
# optional: --token <secret> to gate room access at the relay;
#           --idle-timeout-secs <n> to reap silently-dead sockets (default 90, 0 disables)
# monitoring: GET /health, /api/rooms (all active rooms + counts),
#             /api/room/:room (one room's agents)
```

**Cloudflare Worker:**

```bash
cd worker
npm install
CLOUDFLARE_API_TOKEN=<token> npx wrangler deploy
# → wss://<your-worker-subdomain>.workers.dev
```

### 3. Install as an MCP server for Claude Desktop / opencode

After `npm install -g remote-agents`, point your AI host at the same binary in
`mcp` mode (stdio). The machine joins the room as a full peer (executes commands
from others) — add `"--no-agent"` to the args if it should be a send-only
controller instead:

```json
{
  "mcpServers": {
    "remote-agents": {
      "command": "remote-agents",
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

(opencode uses the same shape under its own `mcp` config key — see
`~/.config/opencode/opencode.json`.)

Connection settings are resolved as **CLI flag > env var > `config.toml` >
default**, so you can instead supply them via `env` in the MCP config:

```json
{
  "mcpServers": {
    "remote-agents": {
      "command": "remote-agents",
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

### One-command client registration

Instead of hand-editing each agent's config, let the binary write it. The
connection flags are baked into the registered server's args:

```bash
remote-agents install-mcp --client cursor \
  --relay wss://<your-relay-host> --room myroom --token <secret>
# ✓ Registered MCP server 'remote-agents' for Cursor (created ~/.cursor/mcp.json)

remote-agents install-mcp            # no --client: list supported clients
```

Supported: `claude-desktop`, `claude-code`, `cursor`, `cline`, `roo`, `kilo`,
`windsurf`, `zed`, `opencode` (config merged in place, preserving any servers
you already have) and `continue`, `goose` (YAML — a ready-to-paste snippet is
printed). Add `--server-name`, `--name`, `--tags`, or `--no-agent` to customize
the registered entry.

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
| `fleet_exec` / `fleet_read` / `fleet_write` / `fleet_git` / `fleet_search` | Run an operation across the fleet — `target = all \| tag1,tag2 \| os:<family>` |
| `file_search` / `file_stat` / `send_file` / `transfer_get` | Find files on a host, and move a file host→host (UDP, SHA-256 verified) |
| `mapreduce` | Distributed map/reduce over the fleet (shell map/reduce functions) |

Each agent advertises platform metadata (OS family, distro, kernel, shell) and
is aware of its peers, so the orchestrator can target hosts by OS and tailor
commands per platform.

## File search, download & transfer

Find and move files across the fleet — over the same end-to-end-encrypted
channel:

- **Search** a host's files by name, content, or images-only (`file_search`,
  with sensible default roots: home + Pictures/Documents/Downloads/Desktop). When
  a deterministic search comes up empty, the host's AI can locate the file.
- **Preview & download** to the browser: images get a host-generated thumbnail;
  any file downloads via a **binary-safe, chunked pull** through the relay (each
  chunk is its own request, staying under the relay's frame limit — no UDP needed
  in the browser).
- **Host↔host transfer**: `send_file` streams a file from one host to another
  over the **direct UDP data channel** (a channel is opened on demand, with
  automatic relay fallback), verified end-to-end with SHA-256. Receiving writes
  to disk and requires Edit/Bypass mode on the destination.

The browser panel (`fleet-chat`) exposes all of this: a 📁 Files view to search,
preview photos in chat, download, and move files between hosts with live
progress.

It also surfaces each host's **local AI-chat history**, labelled by host and
provider. Resumable providers (`claude`, `opencode`) can be continued from the
panel; the VS Code agents (`cline`, `roo`, `kilo`) and `zed` are imported
read-only — their transcripts are shown for browsing but have no headless resume.

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
