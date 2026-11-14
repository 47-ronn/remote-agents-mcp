---
name: remote-agents
description: Control a fleet of remote machines through the `remote-agents` MCP server. Tools cover exec, files, transfers, git, tunnels, schedules, autonomous AI tasks, task chains, AI-chat history search (BM25 over claude/opencode/codex/... transcripts), and fleet-wide fan-out (exec/read/write/git/search by 'all', tags, or os:family). Use when the user mentions remote hosts, the fleet, running commands on other machines, transferring files between hosts, or searching past AI chats across machines.
---

# Remote Agents — fleet control

A peer-to-peer fleet of machines ("agents") connected to a relay room; every
command is end-to-end encrypted (AES-256-GCM) — the relay sees only ciphertext.
This MCP server is a **full peer**: it can drive every other host in the room.

## Core concepts

- **agent_id** — almost every tool takes an optional `agent_id`. Omitted →
  runs on the **local host**. Provided → forwarded to that remote agent.
  Resolve ids first with `list_agents` (returns name, hostname, os, version,
  tags, mode, `responsive`).
- **target** (fleet_* tools) — fan-out selector: `'all'`, comma-separated tags
  (`'backend,db'`), or `'os:linux'` / `'os:windows'`. One call runs everywhere,
  results come back per-host.
- **safety modes** — each host has one: `plan` (read-only), `edit` (writes,
  backups), `bypass`, `disabled`. Check `get_info`; change with `set_mode`.
  A write on a host in `plan` mode fails — that's by design.
- **Autonomous tasks** — `task_dispatch` sends a prompt to a host, which runs
  it with **its own** AI-provider credentials (the token never leaves that
  host). Stream output via `task_get` (status `running` → live partial output),
  answer its questions with `task_input`, abort with `task_cancel`.
- **Chains** — `chain_arm` wires a follow-up: when the source task finishes,
  the prompt is dispatched to `dst_agent` with the source result as context.
- **History search** — every host BM25-indexes the local AI-chat transcripts
  (claude / opencode / codex / cline / roo / kilo / zed / cursor / gemini /
  qwen / goose / continue). `session_search` = one host, `fleet_session_search`
  = merged by score across hosts. Hits cite `seq` — open context with
  `session_get {provider, id, around_seq}` instead of pulling whole chats.

## Tool map

| Area | Tools |
|---|---|
| Shell | `exec`, `fleet_exec` |
| Files (one host) | `read_file`, `write_file`, `list_dir`, `file_search`, `file_stat` |
| Files (fleet-wide) | `fleet_read`, `fleet_write`, `fleet_search` |
| Transfers | `send_file`, `sync_dir`, `transfer_get` (direct UDP hole-punch, relay fallback) |
| Git | `git_status/pull/commit/push` (one host), `fleet_git` (all hosts) |
| Tunnels | `tunnel_start`, `tunnel_list`, `tunnel_stop` (Cloudflare quick tunnels) |
| Host info | `get_info`, `set_mode`, `list_agents`, `fleet_update_check` |
| Schedules (cron) | `schedule_add`, `schedule_remove`, `schedule_list` |
| AI tasks | `task_dispatch`, `task_get`, `task_list`, `task_cancel`, `task_input` |
| Chains | `chain_arm`, `chain_list`, `chain_cancel` |
| History | `session_list`, `session_get`, `session_search`, `fleet_session_search` |
| Compute | `mapreduce` — partition `data` across workers; map_fn gets a JSON array on stdin, reduce_fn folds outputs |

## Working patterns

**Discover first.** Always `list_agents` before addressing hosts; ids change
across restarts. Use `name` for humans, `id` for tools.

**Local vs remote.** Don't pass `agent_id` for work on this machine; passing
your own id wastes a relay round-trip.

**Long output.** `exec` returns full output but big dumps should be filtered
(`| tail`, `| grep`) or written to a file and read with `read_file`.

**Bulk ops.** Prefer one `fleet_exec` over N× `exec`. For parallel data
processing use `mapreduce` with `workers` set to the number of hosts.

**Search before you re-derive.** Past decisions live in old chats:
`fleet_session_search {query, target: 'all'}` surfaces prior solutions and
failed approaches, then `session_get` with `around_seq` reads just the cited
window — much cheaper than whole transcripts.

**Transfers.** `send_file`/`sync_dir` go host→host directly (UDP
hole-punching, SHA-256 verified) with relay fallback; poll `transfer_get`.
`sync_dir` supports `dry_run`, `checksum`, `delete`, and gitignore-flavored
`exclude` patterns.

**Timeouts.** `exec` accepts `timeout_ms`; default kills the whole process
group. Long builds: give an explicit generous timeout or run them as
`sched`uled/`task_dispatch` jobs instead.

## Failure modes

- `No matching agents found` — stale `agent_id`; re-list agents.
- `NOT RESPONDING` in `list_agents` — host is alive but busy/offline; retry later.
- Decrypt/key errors — the host sits in a different room or under a different
  token.
- A write failed → check the host's mode (`get_info`); `plan` blocks writes.
