//! AI-provider session discovery: list/read/terminate the host's local
//! provider conversations so the web panel can surface them as dialogs (with
//! host + provider labels), continue them, and flag live ones.
//!
//! - claude: JSONL transcripts under `~/.claude/projects/<proj>/<uuid>.jsonl`.
//! - opencode: queried via its CLI (`opencode session list/export`), so the
//!   1.3 GB SQLite store is never touched directly.
//! - cline/roo/kilo (VS Code agents) and zed: read-only task/thread stores.
//! - cursor: agent-transcript JSONL trees under
//!   `~/.cursor/projects/<proj>/agent-transcripts/<sid>/<sid>.jsonl`.
//! - codex: rollout JSONL trees under `~/.codex/sessions|archived_sessions`;
//!   injected context turns are filtered, resume via `codex exec resume`.
//! - gemini / qwen: chat recordings (JSONL) under `~/.gemini/tmp/<h>/` and
//!   `~/.qwen/{tmp,chat}/` — one family parser, two roots.
//! - goose: `~/.local/share/goose/sessions/sessions.db` (SQLite, best-effort
//!   column introspection; `content_json` text collected recursively).
//! - continue: one JSON doc per session under `~/.continue/sessions/`.

use anyhow::{bail, Context, Result};
use remote_agents_shared::{SessionMessage, SessionMeta};
#[cfg(unix)]
use std::os::unix::process::CommandExt; // process_group
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::{Duration, Instant, UNIX_EPOCH};

/// Cap per provider so a huge history doesn't blow up the list.
const MAX_PER_PROVIDER: usize = 200;
/// Re-scan no more often than this (the list is polled by the panel).
const CACHE_TTL: Duration = Duration::from_secs(30);
/// Bound on a provider CLI call.
const CLI_TIMEOUT: Duration = Duration::from_secs(8);
/// Keep at most this many of the most-recent messages in a returned transcript.
const MAX_TRANSCRIPT_MESSAGES: usize = 1000;
/// …and at most this many text bytes. A long session can reach ~1 MiB of text
/// (the relay's per-frame limit even before JSON/encryption overhead), and the
/// panel re-fetches an open live dialog every few seconds — so bound it to the
/// most recent turns. Both caps keep the tail; a marker notes what was dropped.
const MAX_TRANSCRIPT_BYTES: usize = 700_000;

static CACHE: Mutex<Option<(Instant, Vec<SessionMeta>)>> = Mutex::new(None);

/// All provider sessions on this host (metadata only), newest first. Cached.
pub fn list_sessions() -> Vec<SessionMeta> {
    list_sessions_with(MAX_PER_PROVIDER)
}

/// Same, with a per-provider cap chosen by the caller. The panel uses
/// [`MAX_PER_PROVIDER`]; the search indexer passes a much larger cap so the
/// full local history becomes searchable, not just the newest 200.
pub fn list_sessions_with(limit: usize) -> Vec<SessionMeta> {
    if let Ok(guard) = CACHE.lock() {
        if let Some((at, v)) = guard.as_ref() {
            if at.elapsed() < CACHE_TTL {
                return v.clone();
            }
        }
    }
    let mut all = claude_sessions(limit);
    all.extend(opencode_sessions(limit));
    all.extend(vscode_agent_sessions(limit));
    all.extend(zed_sessions(limit));
    all.extend(cursor_sessions(limit));
    all.extend(codex_sessions(limit));
    all.extend(gemini_family_sessions("gemini", ".gemini", limit));
    all.extend(gemini_family_sessions("qwen", ".qwen", limit));
    all.extend(goose_sessions(limit));
    all.extend(continue_sessions(limit));
    all.sort_by_key(|s| std::cmp::Reverse(s.updated));
    if let Ok(mut g) = CACHE.lock() {
        *g = Some((Instant::now(), all.clone()));
    }
    all
}

/// Drop the cached session list so the next `list_sessions` re-scans. Called
/// when an autonomous task finishes: a chat turn (`claude -p` / `opencode run`)
/// just created or extended a provider session, and the web wants to adopt it
/// immediately (to resume it for context), not after the 30s TTL. Also marks
/// the search index stale so the next search sees the new turns.
pub fn invalidate_cache() {
    if let Ok(mut g) = CACHE.lock() {
        *g = None;
    }
    crate::session_index::mark_stale();
}

/// Full transcript of one session, capped to the most recent messages so it
/// fits the relay frame and the panel stays responsive.
pub fn get_transcript(provider: &str, id: &str) -> Result<Vec<SessionMessage>> {
    Ok(cap_transcript(full_transcript(provider, id)?))
}

/// The complete transcript with no display caps — used by the search indexer
/// (which needs the whole text) and by window extraction.
pub(crate) fn full_transcript(provider: &str, id: &str) -> Result<Vec<SessionMessage>> {
    let msgs = match provider {
        "claude" => claude_transcript(id),
        "opencode" => opencode_transcript(id),
        "cline" | "roo" | "kilo" => vscode_agent_transcript(provider, id),
        "zed" => zed_transcript(id),
        "cursor" => cursor_transcript(id),
        "codex" => codex_transcript(id),
        "gemini" => gemini_family_transcript("gemini", ".gemini", id),
        "qwen" => gemini_family_transcript("qwen", ".qwen", id),
        "goose" => goose_transcript(id),
        "continue" => continue_transcript(id),
        other => bail!("unknown provider '{other}'"),
    }?;
    Ok(msgs)
}

/// A `window`-sized slice of the transcript centered on message `seq` — the
/// cited context for a search hit. A system marker notes the position; the
/// byte/message caps still apply (tail-kept) so a pathological message can't
/// blow the relay frame.
pub fn windowed_transcript(provider: &str, id: &str, seq: usize, half: usize) -> Result<Vec<SessionMessage>> {
    let msgs = full_transcript(provider, id)?;
    let total = msgs.len();
    if total == 0 {
        return Ok(msgs);
    }
    let center = seq.min(total - 1);
    let start = center.saturating_sub(half);
    let end = (center + half + 1).min(total);
    let mut out = Vec::with_capacity(end - start + 1);
    out.push(SessionMessage {
        role: "system".to_string(),
        text: format!(
            "… сообщения {start}–{end} из {total}; совпадение — №{center} …"
        ),
        ts: None,
    });
    out.extend(msgs.into_iter().skip(start).take(end - start));
    Ok(cap_transcript(out))
}

/// Keep the most-recent messages within the message/byte caps. If anything was
/// dropped, prepend a `system` marker so the reader knows the head is truncated.
fn cap_transcript(msgs: Vec<SessionMessage>) -> Vec<SessionMessage> {
    let total = msgs.len();
    // Walk from the end, accumulating until either cap is reached.
    let mut bytes = 0usize;
    let mut keep = 0usize;
    for m in msgs.iter().rev() {
        let next = bytes + m.text.len();
        if keep >= MAX_TRANSCRIPT_MESSAGES || (keep > 0 && next > MAX_TRANSCRIPT_BYTES) {
            break;
        }
        bytes = next;
        keep += 1;
    }
    if keep >= total {
        return msgs;
    }
    let dropped = total - keep;
    let mut out = Vec::with_capacity(keep + 1);
    out.push(SessionMessage {
        role: "system".to_string(),
        text: format!(
            "… показаны последние {keep} из {total} сообщений ({dropped} более ранних опущены) …"
        ),
        ts: None,
    });
    out.extend(msgs.into_iter().skip(total - keep));
    out
}

/// Session ids currently live (a running `opencode -s …` / `claude --resume …`).
pub fn active_sessions() -> Vec<String> {
    parse_active_ids(&ps_args())
}

/// Terminate a live session by killing its process (SIGTERM).
pub fn terminate(id: &str) -> Result<()> {
    let pid = pid_for_session(id, &ps_args()).context("no live session for that id")?;
    let ok = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if ok {
        Ok(())
    } else {
        bail!("failed to terminate pid {pid}")
    }
}

// --- claude (filesystem JSONL) ---------------------------------------------

fn claude_root() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".claude").join("projects"))
}

fn claude_sessions(limit: usize) -> Vec<SessionMeta> {
    let Some(root) = claude_root() else {
        return vec![];
    };
    let mut files: Vec<(PathBuf, u64)> = Vec::new();
    let Ok(projects) = std::fs::read_dir(&root) else {
        return vec![];
    };
    for proj in projects.flatten() {
        if !proj.path().is_dir() {
            continue;
        }
        if let Ok(sessions) = std::fs::read_dir(proj.path()) {
            for s in sessions.flatten() {
                let p = s.path();
                if p.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                    files.push((p.clone(), mtime_ms(&p)));
                }
            }
        }
    }
    files.sort_by_key(|f| std::cmp::Reverse(f.1));
    files.truncate(limit);
    files
        .into_iter()
        .filter_map(|(p, mtime)| {
            let id = p.file_stem()?.to_string_lossy().to_string();
            let cwd = p
                .parent()
                .and_then(|d| d.file_name())
                .map(|n| decode_claude_dir(&n.to_string_lossy()));
            let title = claude_title(&p).unwrap_or_else(|| id.clone());
            Some(SessionMeta {
                provider: "claude".to_string(),
                id,
                title,
                updated: mtime,
                cwd,
                resumable: true,
            })
        })
        .collect()
}

/// Title from a claude JSONL head: prefer an `ai-title` record, else the first
/// user message. Reads only the head of the file.
fn claude_title(path: &Path) -> Option<String> {
    let content = read_head(path, 64 * 1024)?;
    claude_title_from(&content)
}

fn claude_title_from(content: &str) -> Option<String> {
    let mut first_user: Option<String> = None;
    for line in content.lines() {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        match v.get("type").and_then(|t| t.as_str()) {
            Some("ai-title") => {
                if let Some(t) = v.get("aiTitle").and_then(|x| x.as_str()) {
                    if !t.is_empty() {
                        return Some(t.to_string());
                    }
                }
            }
            Some("user") if first_user.is_none() => {
                first_user = message_text(v.get("message")).map(|s| truncate(&s, 80));
            }
            _ => {}
        }
    }
    first_user
}

/// Decode claude's project dir name (`-home-ojo-dev-x`) back to a path. Lossy
/// for paths containing dashes — this is a display hint only.
fn decode_claude_dir(name: &str) -> String {
    name.replace('-', "/")
}

/// The working directory a claude session was recorded in.
///
/// `claude --resume <id>` resolves a session id only within the project that
/// maps to the *current* cwd, so a non-interactive resume must chdir there
/// first — otherwise claude looks in the wrong project store and reports
/// "No conversation found with session ID". The exact path lives in the JSONL
/// `cwd` field; the project *dir name* is lossy (it can't tell `-` from `_`/`/`,
/// see [`decode_claude_dir`]) so we only fall back to decoding it if no record
/// carries a cwd.
pub fn claude_session_cwd(id: &str) -> Option<PathBuf> {
    let root = claude_root()?;
    let file = std::fs::read_dir(&root)
        .ok()?
        .flatten()
        .filter(|e| e.path().is_dir())
        .map(|e| e.path().join(format!("{id}.jsonl")))
        .find(|p| p.is_file())?;
    // The first records hold a `cwd`; the head is enough (no full read).
    if let Some(cwd) = read_head(&file, 256 * 1024).and_then(|c| cwd_from_jsonl(&c)) {
        return Some(PathBuf::from(cwd));
    }
    // Fallback: lossy decode of the project dir name (best effort).
    file.parent()
        .and_then(|d| d.file_name())
        .map(|n| PathBuf::from(decode_claude_dir(&n.to_string_lossy())))
}

/// First non-empty `cwd` field across a claude JSONL head, if any.
fn cwd_from_jsonl(content: &str) -> Option<String> {
    for line in content.lines() {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if let Some(cwd) = v.get("cwd").and_then(|c| c.as_str()) {
            if !cwd.is_empty() {
                return Some(cwd.to_string());
            }
        }
    }
    None
}

fn claude_transcript(id: &str) -> Result<Vec<SessionMessage>> {
    let root = claude_root().context("no home dir")?;
    // Find <id>.jsonl under any project dir.
    let file = std::fs::read_dir(&root)
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().is_dir())
        .map(|e| e.path().join(format!("{id}.jsonl")))
        .find(|p| p.is_file())
        .with_context(|| format!("claude session '{id}' not found"))?;
    let content = std::fs::read_to_string(&file)?;
    let mut out = Vec::new();
    for line in content.lines() {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let role = match v.get("type").and_then(|t| t.as_str()) {
            Some(r @ ("user" | "assistant" | "system")) => r,
            _ => continue,
        };
        if let Some(text) = message_text(v.get("message")) {
            if !text.trim().is_empty() {
                out.push(SessionMessage {
                    role: role.to_string(),
                    text,
                    ts: v
                        .get("timestamp")
                        .and_then(|t| t.as_str())
                        .and_then(parse_iso_ms),
                });
            }
        }
    }
    Ok(out)
}

// --- opencode (CLI) ---------------------------------------------------------

fn opencode_sessions(limit: usize) -> Vec<SessionMeta> {
    // `opencode session list` is scoped to the CWD's project, so run it from
    // HOME to get the global session list (the agent's own working directory
    // would otherwise expose only that one project's handful of sessions).
    let limit = limit.to_string();
    let Some(out) = run_cli_home(
        "opencode",
        &["session", "list", "--format", "json", "-n", &limit],
    ) else {
        return vec![];
    };
    let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(&out) else {
        return vec![];
    };
    arr.into_iter()
        .filter_map(|s| {
            let id = s.get("id")?.as_str()?.to_string();
            Some(SessionMeta {
                provider: "opencode".to_string(),
                id,
                title: s
                    .get("title")
                    .and_then(|t| t.as_str())
                    .unwrap_or("(без названия)")
                    .to_string(),
                updated: s.get("updated").and_then(|u| u.as_u64()).unwrap_or(0),
                cwd: s
                    .get("directory")
                    .and_then(|d| d.as_str())
                    .map(String::from),
                resumable: true,
            })
        })
        .collect()
}

fn opencode_transcript(id: &str) -> Result<Vec<SessionMessage>> {
    let out = run_cli_home("opencode", &["export", id]).context("opencode export failed")?;
    let v: serde_json::Value = serde_json::from_str(&out)?;
    let msgs = v.get("messages").and_then(|m| m.as_array());
    Ok(msgs
        .into_iter()
        .flatten()
        .filter_map(opencode_message)
        .collect())
}

/// One opencode export message → a transcript turn. Current opencode (1.x)
/// nests `{info:{role,time:{created}}, parts:[{type:"text",text}, …]}`; older
/// exports used a flat `{role, content}`. Handle both; drop non-text parts
/// (tool / step-start / step-finish) and empty turns.
fn opencode_message(m: &serde_json::Value) -> Option<SessionMessage> {
    // Current shape: role/time under `info`, text under `parts`.
    if let Some(info) = m.get("info") {
        let role = info.get("role").and_then(|r| r.as_str())?.to_string();
        let text = m
            .get("parts")
            .and_then(|p| p.as_array())
            .map(|parts| {
                parts
                    .iter()
                    .filter(|p| p.get("type").and_then(|t| t.as_str()) == Some("text"))
                    .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        if text.trim().is_empty() {
            return None;
        }
        let ts = info
            .get("time")
            .and_then(|t| t.get("created"))
            .and_then(|c| c.as_u64());
        return Some(SessionMessage { role, text, ts });
    }
    // Legacy flat shape: {role, content, time}.
    let role = m.get("role").and_then(|r| r.as_str())?.to_string();
    let text = message_text(Some(m))
        .or_else(|| m.get("content").and_then(|c| c.as_str()).map(String::from))?;
    if text.trim().is_empty() {
        return None;
    }
    Some(SessionMessage { role, text, ts: m.get("time").and_then(|t| t.as_u64()) })
}

// --- VS Code agents: Cline / Roo / Kilo (filesystem JSON) -------------------
//
// All three are forks of the same base and share an identical on-disk layout:
//   <globalStorage>/<ext-id>/tasks/<task-id>/ui_messages.json
// `ui_messages.json` is an array of ClineMessage `{ ts, type:"ask"|"say",
// say?, ask?, text? }`. History is read-only — these are VS Code-extension
// tasks with no headless `--resume`, so the panel surfaces them view-only.

/// (provider label, globalStorage extension-id) for each supported agent.
const VSCODE_AGENTS: &[(&str, &str)] = &[
    ("cline", "saoudrizwan.claude-dev"),
    ("roo", "rooveterinaryinc.roo-cline"),
    ("kilo", "kilocode.kilo-code"),
];

/// VS Code `globalStorage` roots across the common editor variants (incl. forks
/// the agents can also be installed into) and the remote/server layout.
fn vscode_global_storage_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(cfg) = dirs::config_dir() {
        // dirs::config_dir() = ~/.config (linux), ~/Library/Application Support
        // (macOS), %APPDATA% (windows) — the VS Code `User` parent on every OS.
        for app in ["Code", "Code - OSS", "VSCodium", "Cursor", "Windsurf"] {
            roots.push(cfg.join(app).join("User").join("globalStorage"));
        }
    }
    if let Some(home) = dirs::home_dir() {
        roots.push(
            home.join(".vscode-server")
                .join("data")
                .join("User")
                .join("globalStorage"),
        );
    }
    roots
}

/// All `tasks/` dirs for one extension id, across every globalStorage root.
fn vscode_task_dirs(ext_id: &str) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    for root in vscode_global_storage_roots() {
        let tasks = root.join(ext_id).join("tasks");
        let Ok(entries) = std::fs::read_dir(&tasks) else {
            continue;
        };
        for e in entries.flatten() {
            if e.path().is_dir() {
                dirs.push(e.path());
            }
        }
    }
    dirs
}

fn vscode_agent_sessions(limit: usize) -> Vec<SessionMeta> {
    let mut out = Vec::new();
    for (provider, ext_id) in VSCODE_AGENTS {
        let mut dirs: Vec<(PathBuf, u64)> = vscode_task_dirs(ext_id)
            .into_iter()
            .filter_map(|d| {
                let msgs = d.join("ui_messages.json");
                msgs.is_file().then(|| (d, mtime_ms(&msgs)))
            })
            .collect();
        dirs.sort_by_key(|f| std::cmp::Reverse(f.1));
        dirs.truncate(limit);
        for (dir, updated) in dirs {
            let Some(id) = dir.file_name().map(|n| n.to_string_lossy().to_string()) else {
                continue;
            };
            let title = vscode_title(&dir.join("ui_messages.json")).unwrap_or_else(|| id.clone());
            out.push(SessionMeta {
                provider: provider.to_string(),
                id,
                title,
                updated,
                cwd: None,
                resumable: false,
            });
        }
    }
    out
}

/// Title = the initial `say:"task"` text (truncated), else first user turn.
fn vscode_title(msgs_path: &Path) -> Option<String> {
    let content = read_head(msgs_path, 256 * 1024)?;
    let arr: Vec<serde_json::Value> = serde_json::from_str(&content).ok()?;
    for v in &arr {
        if v.get("say").and_then(|s| s.as_str()) == Some("task") {
            if let Some(t) = cline_text(v) {
                return Some(truncate(&t, 80));
            }
        }
    }
    arr.iter()
        .find_map(cline_message)
        .filter(|m| m.role == "user")
        .map(|m| truncate(&m.text, 80))
}

fn vscode_agent_transcript(provider: &str, id: &str) -> Result<Vec<SessionMessage>> {
    let ext_id = VSCODE_AGENTS
        .iter()
        .find(|(p, _)| *p == provider)
        .map(|(_, e)| *e)
        .with_context(|| format!("unknown vscode agent '{provider}'"))?;
    // Reject path-traversal in the id; it indexes a directory name.
    if id.contains('/') || id.contains("..") {
        bail!("invalid session id");
    }
    let path = vscode_global_storage_roots()
        .into_iter()
        .map(|r| r.join(ext_id).join("tasks").join(id).join("ui_messages.json"))
        .find(|p| p.is_file())
        .with_context(|| format!("{provider} session '{id}' not found"))?;
    let content = std::fs::read_to_string(&path)?;
    let arr: Vec<serde_json::Value> = serde_json::from_str(&content)?;
    Ok(arr.iter().filter_map(cline_message).collect())
}

/// Map one ClineMessage to a transcript turn, or None for tool/api noise.
fn cline_message(v: &serde_json::Value) -> Option<SessionMessage> {
    let say = v.get("say").and_then(|s| s.as_str());
    let ask = v.get("ask").and_then(|s| s.as_str());
    let role = match (say, ask) {
        // User's own turns: the initial task and feedback replies.
        (Some("task"), _) | (Some("user_feedback"), _) => "user",
        // Assistant prose / final answers / reasoning.
        (Some("text"), _) | (Some("completion_result"), _) | (Some("reasoning"), _) => "assistant",
        // Assistant asking the user something (followup / plan response).
        (_, Some("followup")) | (_, Some("plan_mode_respond")) | (_, Some("completion_result")) => {
            "assistant"
        }
        // api_req_started, tool, command, command_output, … → skip.
        _ => return None,
    };
    let text = cline_text(v)?;
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    Some(SessionMessage {
        role: role.to_string(),
        text: text.to_string(),
        ts: v.get("ts").and_then(|t| t.as_u64()),
    })
}

/// Extract display text from a ClineMessage `text`. Most are plain strings; some
/// (followup/plan_mode_respond) wrap it as JSON `{question|response, …}`.
fn cline_text(v: &serde_json::Value) -> Option<String> {
    let raw = v.get("text").and_then(|t| t.as_str())?;
    if let Ok(obj) = serde_json::from_str::<serde_json::Value>(raw) {
        for key in ["question", "response", "result"] {
            if let Some(s) = obj.get(key).and_then(|x| x.as_str()) {
                return Some(s.to_string());
            }
        }
    }
    Some(raw.to_string())
}

// --- Zed (SQLite threads.db) ------------------------------------------------
//
// Zed stores agent threads in `<data-dir>/zed/threads/threads.db`:
//   threads(id, summary, updated_at TEXT/RFC3339, data_type TEXT, data BLOB, …)
// `data` is a serde_json `DbThread` — either raw (`data_type='json'`) or a zstd
// frame (`data_type='zstd'`); `maybe_decompress` tells them apart by magic.
// `messages` is an externally-tagged enum array: {"User":{content:[…]}} /
// {"Agent":{content:[…]}} / "Resume" / {"Compaction":…}, each content item a
// {"Text":"…"} (Agent also Thinking/ToolUse, which we drop). View-only.

use rusqlite::{Connection, OpenFlags, OptionalExtension};

/// Candidate `threads.db` paths (linux uses `zed`, macOS `Zed`).
fn zed_db_paths() -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Some(data) = dirs::data_dir() {
        for app in ["zed", "Zed"] {
            v.push(data.join(app).join("threads").join("threads.db"));
        }
    }
    v
}

/// A `file:` URI with the path percent-encoded (UTF-8 safe). `immutable=1`
/// snapshots a possibly-live DB without taking locks or creating -wal/-shm.
fn zed_db_uri(path: &Path) -> String {
    let mut s = String::from("file:");
    for &b in path.to_string_lossy().as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'-' | b'_' | b'~') {
            s.push(b as char);
        } else {
            s.push_str(&format!("%{b:02x}"));
        }
    }
    s.push_str("?immutable=1&mode=ro");
    s
}

fn open_zed_db(path: &Path) -> Result<Connection> {
    // Prefer a plain read-only open so WAL-mode writes are visible; if the DB is
    // locked or its -shm can't be created, fall back to an immutable snapshot
    // (no locks, but won't see rows still sitting in -wal).
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .or_else(|_| {
            Connection::open_with_flags(
                zed_db_uri(path),
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
            )
        })
        .with_context(|| format!("open zed db {}", path.display()))
}

fn zed_sessions(limit: usize) -> Vec<SessionMeta> {
    let mut out = Vec::new();
    for path in zed_db_paths() {
        if !path.is_file() {
            continue;
        }
        let Ok(conn) = open_zed_db(&path) else {
            continue;
        };
        let Ok(mut stmt) =
            conn.prepare("SELECT id, summary, updated_at FROM threads ORDER BY updated_at DESC LIMIT ?1")
        else {
            continue;
        };
        let rows = stmt.query_map([limit as i64], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        });
        let Ok(rows) = rows else {
            continue;
        };
        for (id, summary, updated_at) in rows.flatten() {
            let title = if summary.trim().is_empty() {
                id.clone()
            } else {
                truncate(&summary, 80)
            };
            out.push(SessionMeta {
                provider: "zed".to_string(),
                id,
                title,
                updated: parse_iso_ms(&updated_at).unwrap_or(0),
                cwd: None,
                resumable: false,
            });
        }
    }
    out
}

fn zed_transcript(id: &str) -> Result<Vec<SessionMessage>> {
    for path in zed_db_paths() {
        if !path.is_file() {
            continue;
        }
        let conn = open_zed_db(&path)?;
        let row: Option<Vec<u8>> = conn
            .query_row(
                "SELECT data FROM threads WHERE id = ?1",
                [id],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        if let Some(data) = row {
            let json = remote_agents_shared::compress::maybe_decompress(&data)?;
            return parse_zed_thread(&json);
        }
    }
    bail!("zed session '{id}' not found")
}

fn parse_zed_thread(json: &[u8]) -> Result<Vec<SessionMessage>> {
    let v: serde_json::Value = serde_json::from_slice(json)?;
    let msgs = v.get("messages").and_then(|m| m.as_array());
    Ok(msgs
        .into_iter()
        .flatten()
        .filter_map(zed_message)
        .collect())
}

/// One externally-tagged Zed `Message` → a transcript turn, or None for the
/// non-conversational variants (Resume / Compaction / empty content).
fn zed_message(m: &serde_json::Value) -> Option<SessionMessage> {
    let obj = m.as_object()?;
    // User wins if present; otherwise Agent. `?` yields None for the
    // non-conversational variants ("Resume" string / Compaction / unknown).
    let (role, body) = match obj.get("User") {
        Some(u) => ("user", u),
        None => ("assistant", obj.get("Agent")?),
    };
    let parts: Vec<String> = body
        .get("content")?
        .as_array()?
        .iter()
        // Each content item is externally tagged; keep only {"Text": "…"}.
        .filter_map(|c| c.get("Text").and_then(|x| x.as_str()).map(String::from))
        .collect();
    let text = parts.join("\n");
    if text.trim().is_empty() {
        return None;
    }
    Some(SessionMessage {
        role: role.to_string(),
        text,
        ts: None,
    })
}

// --- Cursor (agent-transcripts JSONL tree) -----------------------------------
//
// Cursor (1.x) writes agent-mode transcripts as a JSONL tree under
// `~/.cursor/projects/<project>/agent-transcripts/<session>/<session>.jsonl`
// (ctx calls this `cursor_agent_transcript_jsonl_tree`). One JSON object per
// line; the conversational record carries the role at the TOP level (a
// nested `message.role`, when present, must agree):
//   {"timestamp":"…","role":"user","message":{"content":[{"type":"text","text":"…"}]}}
// User prompts wrap the query in tags:
//   "<timestamp>Thursday, Jul 30, 2026…</timestamp>\n<user_query>…</user_query>"
// Everything else — `{"event":"summary"|"turn_ended",…}`, `type`≠"message",
// records with `status`, tool_use/tool_result blocks — is non-conversational
// noise and skipped. View-only: no headless resume CLI.

/// Candidate transcript files: `projects/*/agent-transcripts/*/<id>.jsonl`.
fn cursor_files() -> Vec<(PathBuf, u64)> {
    let mut files = Vec::new();
    let Some(root) = dirs::home_dir().map(|h| h.join(".cursor").join("projects")) else {
        return files;
    };
    let Ok(projects) = std::fs::read_dir(&root) else {
        return files;
    };
    for proj in projects.flatten() {
        let transcripts = proj.path().join("agent-transcripts");
        let Ok(sessions) = std::fs::read_dir(&transcripts) else {
            continue;
        };
        for s in sessions.flatten() {
            let p = s
                .path()
                .join(format!("{}.jsonl", s.file_name().to_string_lossy()));
            if p.is_file() {
                let mtime = mtime_ms(&p);
                files.push((p, mtime));
            }
        }
    }
    files
}

fn cursor_sessions(limit: usize) -> Vec<SessionMeta> {
    let mut files = cursor_files();
    files.sort_by_key(|f| std::cmp::Reverse(f.1));
    files.truncate(limit);
    files
        .into_iter()
        .filter_map(|(p, mtime)| {
            let id = p.file_stem()?.to_string_lossy().to_string();
            // session/agent-transcripts/<project> — the project dir is a
            // lossy display hint (dash-encoded path or a tmp window id).
            let cwd = p
                .ancestors()
                .nth(2)
                .and_then(|d| d.file_name())
                .map(|n| n.to_string_lossy().to_string());
            let title = cursor_title(&p).unwrap_or_else(|| id.clone());
            Some(SessionMeta {
                provider: "cursor".to_string(),
                id,
                title,
                updated: mtime,
                cwd,
                resumable: false,
            })
        })
        .collect()
}

/// Title = the first user prompt, with Cursor's `<user_query>` wrapper
/// unwrapped; read from the file head only.
fn cursor_title(path: &Path) -> Option<String> {
    let head = read_head(path, 64 * 1024)?;
    for line in head.lines() {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else { continue };
        if cursor_role(&v) != Some("user") {
            continue;
        }
        let text = cursor_message_text(&v)?;
        return Some(truncate(&unwrap_user_query(&text), 80));
    }
    None
}

/// Strip Cursor's prompt wrapper: keep the `<user_query>`…`</user_query>`
/// body when present, else the raw text.
fn unwrap_user_query(s: &str) -> String {
    const OPEN: &str = "<user_query>";
    const CLOSE: &str = "</user_query>";
    if let (Some(a), Some(b)) = (s.find(OPEN), s.find(CLOSE)) {
        if b > a {
            return s[a + OPEN.len()..b].trim().to_string();
        }
    }
    s.trim().to_string()
}

fn cursor_transcript(id: &str) -> Result<Vec<SessionMessage>> {
    if id.contains('/') || id.contains("..") {
        bail!("invalid cursor session id");
    }
    for (p, _) in cursor_files() {
        if p.file_stem().and_then(|s| s.to_str()) == Some(id) {
            let content =
                std::fs::read_to_string(&p).with_context(|| format!("read {}", p.display()))?;
            return Ok(content.lines().filter_map(cursor_message).collect());
        }
    }
    bail!("cursor session '{id}' not found")
}

/// The authoritative top-level role, validated as conversational.
fn cursor_role(v: &serde_json::Value) -> Option<&str> {
    let role = v.get("role").and_then(|r| r.as_str())?;
    // A nested message role, when present, must agree with the top level.
    if let Some(nested) = v
        .get("message")
        .and_then(|m| m.get("role"))
        .and_then(|r| r.as_str())
    {
        if nested != role {
            return None;
        }
    }
    matches!(role, "user" | "assistant").then_some(role)
}

/// One JSONL line → a transcript turn, or None for noise.
fn cursor_message(line: &str) -> Option<SessionMessage> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
        return None;
    };
    // Summary / turn markers and status-carrying records are not turns.
    if v.get("event").is_some() || v.get("status").is_some() {
        return None;
    }
    let rtype = v.get("type").and_then(|t| t.as_str());
    if !matches!(rtype, None | Some("message")) {
        return None;
    }
    let role = cursor_role(&v)?;
    let text = cursor_message_text(&v)?;
    if text.trim().is_empty() {
        return None;
    }
    let ts = v
        .get("timestamp")
        .and_then(|t| t.as_str())
        .and_then(parse_iso_ms);
    Some(SessionMessage {
        role: role.to_string(),
        text,
        ts,
    })
}

/// Concatenate the `type:"text"` blocks of `message.content` (top-level
/// `content` as a legacy fallback); tool_use/tool_result blocks are dropped.
fn cursor_message_text(v: &serde_json::Value) -> Option<String> {
    let content = v
        .get("message")
        .and_then(|m| m.get("content"))
        .or_else(|| v.get("content"))?;
    let parts: Vec<String> = content
        .as_array()?
        .iter()
        .filter_map(|b| {
            (b.get("type").and_then(|t| t.as_str()) == Some("text"))
                .then(|| b.get("text").and_then(|t| t.as_str()).map(String::from))
                .flatten()
        })
        .collect();
    (!parts.is_empty()).then(|| parts.join("\n"))
}

// --- Codex (rollout JSONL trees) ----------------------------------------------
//
// Codex CLI writes session "rollouts" as JSONL trees under
// `~/.codex/sessions/YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl` (and the same
// layout under `~/.codex/archived_sessions/`; ctx calls these
// `codex_session_jsonl_tree`). One object per line:
//   {"timestamp":"…","type":"session_meta","payload":{"id":"<uuid>","cwd":"…"}}
//   {"timestamp":"…","type":"response_item","payload":{"type":"message",
//    "role":"user"|"assistant","content":[{"type":"input_text","text":"…"}]}}
// The first user turns are injected context (`# AGENTS.md…`,
// `<environment_context>…`, `<user_instructions>…`) and the `developer` role
// carries permissions instructions — all filtered out. Other envelope types
// (turn_context / event_msg / reasoning / tool calls) are noise.

fn codex_root() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".codex"))
}

/// Every rollout file under `sessions/` and `archived_sessions/` (recursive,
/// bounded depth — the tree is YYYY/MM/DD).
fn codex_files() -> Vec<(PathBuf, u64)> {
    let mut files = Vec::new();
    let Some(root) = codex_root() else { return files };
    for tree in ["sessions", "archived_sessions"] {
        walk_jsonl_tree(&root.join(tree), 0, &mut files);
    }
    files
}

fn walk_jsonl_tree(dir: &Path, depth: usize, out: &mut Vec<(PathBuf, u64)>) {
    if depth > 5 {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            walk_jsonl_tree(&p, depth + 1, out);
        } else if p.extension().and_then(|x| x.to_str()) == Some("jsonl") {
            let mtime = mtime_ms(&p);
            out.push((p, mtime));
        }
    }
}

/// Id + cwd from the first record (`session_meta`); falls back to the uuid
/// embedded in the file name.
fn codex_session_id_and_cwd(path: &Path) -> (String, Option<String>) {
    if let Some(head) = read_head(path, 64 * 1024) {
        if let Some(line) = head.lines().next() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                if v.get("type").and_then(|t| t.as_str()) == Some("session_meta") {
                    let id = v
                        .pointer("/payload/id")
                        .and_then(|i| i.as_str())
                        .filter(|i| !i.is_empty());
                    let cwd = v
                        .pointer("/payload/cwd")
                        .and_then(|c| c.as_str())
                        .map(String::from);
                    if let Some(id) = id {
                        return (id.to_string(), cwd);
                    }
                }
            }
        }
    }
    // rollout-<19-char ts>-<uuid>.jsonl → uuid after position 20.
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let after_prefix = stem.strip_prefix("rollout-").unwrap_or(&stem);
    let id = if after_prefix.len() > 20 {
        after_prefix[20..].to_string()
    } else {
        after_prefix.to_string()
    };
    (id, None)
}

/// User turns that are Codex context injections, not real prompts.
fn is_codex_injected_user_text(text: &str) -> bool {
    let t = text.trim_start();
    t.starts_with("# AGENTS.md")
        || t.starts_with("<environment_context>")
        || t.starts_with("<user_instructions>")
        || t.starts_with("<permissions")
        || t.starts_with("<ENVIRONMENT")
}

fn codex_sessions(limit: usize) -> Vec<SessionMeta> {
    let mut files = codex_files();
    files.sort_by_key(|f| std::cmp::Reverse(f.1));
    files.truncate(limit);
    files
        .into_iter()
        .filter_map(|(p, mtime)| {
            let (id, cwd) = codex_session_id_and_cwd(&p);
            if id.is_empty() {
                return None;
            }
            let title = codex_title(&p).unwrap_or_else(|| id.clone());
            Some(SessionMeta {
                provider: "codex".to_string(),
                id,
                title,
                updated: mtime,
                cwd,
                resumable: true,
            })
        })
        .collect()
}

/// Title = the first *real* user prompt (injections filtered).
fn codex_title(path: &Path) -> Option<String> {
    let head = read_head(path, 256 * 1024)?;
    for line in head.lines() {
        if let Some(m) = codex_message(line) {
            if m.role == "user" {
                return Some(truncate(&m.text, 80));
            }
        }
    }
    None
}

fn codex_transcript(id: &str) -> Result<Vec<SessionMessage>> {
    if id.contains('/') || id.contains("..") {
        bail!("invalid codex session id");
    }
    for (p, _) in codex_files() {
        if codex_session_id_and_cwd(&p).0 == id {
            let content =
                std::fs::read_to_string(&p).with_context(|| format!("read {}", p.display()))?;
            return Ok(content.lines().filter_map(codex_message).collect());
        }
    }
    bail!("codex session '{id}' not found")
}

/// One rollout line → a transcript turn, or None for noise.
fn codex_message(line: &str) -> Option<SessionMessage> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
        return None;
    };
    if v.get("type").and_then(|t| t.as_str()) != Some("response_item") {
        return None;
    }
    let payload = v.get("payload")?;
    if payload.get("type").and_then(|t| t.as_str()) != Some("message") {
        return None;
    }
    let role = payload.get("role").and_then(|r| r.as_str())?;
    if !matches!(role, "user" | "assistant") {
        return None; // developer / system carry instructions noise
    }
    let blocks = payload.get("content")?.as_array()?;
    let parts: Vec<String> = blocks
        .iter()
        .filter_map(|b| {
            for key in ["text", "input_text", "output_text", "summary_text"] {
                if let Some(t) = b.get(key).and_then(|t| t.as_str()) {
                    return Some(t.to_string());
                }
            }
            None
        })
        .collect();
    let text = parts.join("\n");
    if text.trim().is_empty() {
        return None;
    }
    if role == "user" && is_codex_injected_user_text(&text) {
        return None;
    }
    let ts = v.get("timestamp").and_then(|t| t.as_str()).and_then(parse_iso_ms);
    Some(SessionMessage {
        role: role.to_string(),
        text,
        ts,
    })
}

// --- Gemini CLI & Qwen Code (chat recordings) ----------------------------------
//
// Gemini CLI records chats as JSONL under `~/.gemini/tmp/<hash>/*.jsonl`
// (ctx: `gemini_cli_chat_recording_jsonl`). The first line is a header:
//   {"sessionId":"…","startTime":"…","directories":["<cwd>",…]}
// and the chat lines are:
//   {"id":"…","timestamp":"…","type":"user"|"gemini","content":"…"}
// (`type:"gemini"` is the assistant). Everything else — `toolCalls`,
// `result`, `$set`/`$rewindTo` notices — is noise. Qwen Code is a Gemini-CLI
// fork writing the same shapes under `~/.qwen/` (`tmp/` trees and flat
// `chat/` files), with `type:"qwen"` as its assistant tag.

/// Candidate recording files across the gemini-style roots.
fn gemini_family_files(root_name: &str) -> Vec<(PathBuf, u64)> {
    let mut files = Vec::new();
    let Some(home) = dirs::home_dir() else { return files };
    let root = home.join(root_name);
    // Gemini layout: tmp/<hash>/<file>.jsonl
    if let Ok(dirs_) = std::fs::read_dir(root.join("tmp")) {
        for d in dirs_.flatten() {
            if d.path().is_dir() {
                if let Ok(fs) = std::fs::read_dir(d.path()) {
                    for f in fs.flatten() {
                        let p = f.path();
                        if p.extension().and_then(|x| x.to_str()) == Some("jsonl") {
                            let mtime = mtime_ms(&p);
                            files.push((p, mtime));
                        }
                    }
                }
            }
        }
    }
    // Qwen layout: chat/<file>.jsonl (flat)
    if let Ok(fs) = std::fs::read_dir(root.join("chat")) {
        for f in fs.flatten() {
            let p = f.path();
            if p.extension().and_then(|x| x.to_str()) == Some("jsonl") {
                let mtime = mtime_ms(&p);
                files.push((p, mtime));
            }
        }
    }
    files
}

/// Header of a recording: `(session_id, cwd)` from the first line.
fn gemini_family_header(path: &Path) -> (Option<String>, Option<String>) {
    let Some(head) = read_head(path, 16 * 1024) else {
        return (None, None);
    };
    let Some(line) = head.lines().next() else { return (None, None) };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
        return (None, None);
    };
    let id = v
        .get("sessionId")
        .and_then(|s| s.as_str())
        .filter(|s| !s.trim().is_empty())
        .map(String::from);
    let cwd = v
        .get("directories")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .and_then(|d| d.as_str())
        .map(String::from);
    (id, cwd)
}

fn gemini_family_sessions(provider: &str, root_name: &str, limit: usize) -> Vec<SessionMeta> {
    let mut files = gemini_family_files(root_name);
    files.sort_by_key(|f| std::cmp::Reverse(f.1));
    files.truncate(limit);
    files
        .into_iter()
        .filter_map(|(p, mtime)| {
            let (header_id, cwd) = gemini_family_header(&p);
            let id = header_id.unwrap_or_else(|| {
                p.file_stem()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default()
            });
            if id.is_empty() {
                return None;
            }
            let title = gemini_family_title(&p).unwrap_or_else(|| id.clone());
            Some(SessionMeta {
                provider: provider.to_string(),
                id,
                title,
                updated: mtime,
                cwd,
                resumable: false,
            })
        })
        .collect()
}

/// Title = the first `type:"user"` line's content.
fn gemini_family_title(path: &Path) -> Option<String> {
    let head = read_head(path, 64 * 1024)?;
    for line in head.lines() {
        if let Some(m) = gemini_family_message(line) {
            if m.role == "user" {
                return Some(truncate(&m.text, 80));
            }
        }
    }
    None
}

fn gemini_family_transcript(provider: &str, root_name: &str, id: &str) -> Result<Vec<SessionMessage>> {
    if id.contains('/') || id.contains("..") {
        bail!("invalid {provider} session id");
    }
    for (p, _) in gemini_family_files(root_name) {
        let (header_id, _) = gemini_family_header(&p);
        let file_id = header_id.unwrap_or_else(|| {
            p.file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default()
        });
        if file_id == id {
            let content =
                std::fs::read_to_string(&p).with_context(|| format!("read {}", p.display()))?;
            return Ok(content
                .lines()
                .filter_map(gemini_family_message)
                .collect());
        }
    }
    bail!("{provider} session '{id}' not found")
}

/// One recording line → a transcript turn, or None for noise. The assistant
/// tag differs by writer (`gemini` for Gemini CLI, `qwen` for Qwen Code);
/// both are accepted.
fn gemini_family_message(line: &str) -> Option<SessionMessage> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
        return None;
    };
    // Only plain chat lines: no sessionId (header), no toolCalls/result
    // (tool activity), no $set/$rewindTo (notices).
    if v.get("sessionId").is_some()
        || v.get("toolCalls").is_some()
        || v.get("result").is_some()
        || v.get("$set").is_some()
        || v.get("$rewindTo").is_some()
    {
        return None;
    }
    let role = match v.get("type").and_then(|t| t.as_str())? {
        "user" => "user",
        "gemini" | "qwen" | "assistant" => "assistant",
        _ => return None,
    };
    let text = v.get("content").and_then(|c| c.as_str())?.to_string();
    if text.trim().is_empty() {
        return None;
    }
    let ts = v.get("timestamp").and_then(|t| t.as_str()).and_then(parse_iso_ms);
    Some(SessionMessage {
        role: role.to_string(),
        text,
        ts,
    })
}

// --- Goose (sessions.db SQLite) --------------------------------------------------
//
// Goose writes `~/.local/share/goose/sessions/sessions.db` (Windows:
// `%APPDATA%\Block\goose\data\sessions\sessions.db`; ctx:
// `goose_sessions_sqlite`) with tables `sessions` (id + optional metadata
// columns) and `messages` (id, session_id, role, content_json). `content_json`
// is a goose `Message` blob; we collect its text recursively (keys
// `text`/`content`/`message`), skipping `toolResponse` subtrees. Best-effort:
// any column may be missing (versions differ) — everything is optional
// except the ids.

fn goose_db_paths() -> Vec<PathBuf> {
    let mut v = Vec::new();
    if cfg!(windows) {
        if let Some(roaming) = dirs::data_dir() {
            v.push(roaming.join("Block").join("goose").join("data").join("sessions").join("sessions.db"));
        }
    } else if let Some(data) = dirs::data_dir() {
        v.push(data.join("goose").join("sessions").join("sessions.db"));
    }
    v
}

/// Read-only open with an immutable-snapshot fallback (shared with zed).
fn open_ro_sqlite(path: &Path) -> Result<rusqlite::Connection> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .or_else(|_| {
            Connection::open_with_flags(
                zed_db_uri(path),
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
            )
        })
        .map_err(anyhow::Error::from)
}

fn sqlite_columns(conn: &Connection, table: &str) -> Vec<String> {
    conn.prepare(&format!("PRAGMA table_info({table})"))
        .map(|mut stmt| {
            let rows = stmt.query_map([], |r| r.get::<_, String>(1));
            rows.map(|it| it.flatten().collect()).unwrap_or_default()
        })
        .unwrap_or_default()
}

fn goose_sessions(limit: usize) -> Vec<SessionMeta> {
    let mut out = Vec::new();
    for path in goose_db_paths() {
        if !path.is_file() {
            continue;
        }
        let Ok(conn) = open_ro_sqlite(&path) else { continue };
        let cols = sqlite_columns(&conn, "sessions");
        if !cols.iter().any(|c| c == "id") {
            continue;
        }
        // Pick the best available session timestamp / title / cwd columns.
        let ts_col = ["updated_at", "timestamp", "created_at", "started_at"]
            .iter()
            .find(|c| cols.iter().any(|x| x == *c));
        let title_col = ["description", "name", "metadata"]
            .iter()
            .find(|c| cols.iter().any(|x| x == *c));
        let cwd_col = ["working_dir", "work_dir", "cwd"]
            .iter()
            .find(|c| cols.iter().any(|x| x == *c));
        let mut sql = String::from("SELECT id");
        if let Some(t) = title_col {
            sql.push_str(&format!(", {t}"));
        } else {
            sql.push_str(", NULL");
        }
        if let Some(c) = cwd_col {
            sql.push_str(&format!(", {c}"));
        } else {
            sql.push_str(", NULL");
        }
        if let Some(t) = ts_col {
            sql.push_str(&format!(", {t}"));
        } else {
            sql.push_str(", NULL");
        }
        // Newest first (rowid ≈ insertion order) when no timestamp column.
        sql.push_str(" FROM sessions ORDER BY ");
        match ts_col {
            Some(t) => sql.push_str(&format!("{t} DESC")),
            None => sql.push_str("rowid DESC"),
        }
        sql.push_str(" LIMIT ?1");
        let Ok(mut stmt) = conn.prepare(&sql) else { continue };
        let rows = stmt.query_map([limit as i64], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        });
        let Ok(rows) = rows else { continue };
        let db_mtime = mtime_ms(&path);
        for (id, title, cwd, ts) in rows.flatten() {
            let updated = ts
                .as_deref()
                .and_then(parse_iso_ms)
                .or_else(|| ts.as_deref().and_then(|t| t.parse::<u64>().ok()))
                .filter(|t| *t > 0)
                .unwrap_or(db_mtime);
            out.push(SessionMeta {
                provider: "goose".to_string(),
                title: title
                    .as_deref()
                    .map(|t| truncate(t, 80))
                    .filter(|t| !t.is_empty())
                    .unwrap_or_else(|| id.clone()),
                id,
                updated,
                cwd,
                resumable: false,
            });
        }
    }
    out
}

fn goose_transcript(id: &str) -> Result<Vec<SessionMessage>> {
    if id.is_empty() || id.chars().any(|c| !c.is_ascii_alphanumeric() && c != '-' && c != '_') {
        bail!("invalid goose session id");
    }
    for path in goose_db_paths() {
        if !path.is_file() {
            continue;
        }
        let conn = open_ro_sqlite(&path)?;
        let cols = sqlite_columns(&conn, "messages");
        if !cols.iter().any(|c| c == "session_id") || !cols.iter().any(|c| c == "content_json") {
            continue;
        }
        let has_role = cols.iter().any(|c| c == "role");
        let has_ts = cols.iter().any(|c| c == "timestamp");
        let has_id = cols.iter().any(|c| c == "id");
        let mut sql = String::from("SELECT content_json");
        if has_role {
            sql.push_str(", role");
        }
        if has_ts {
            sql.push_str(", timestamp");
        }
        sql.push_str(" FROM messages WHERE session_id = ?1 ORDER BY ");
        sql.push_str(if has_id { "id" } else { "rowid" });
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([id], |r| {
            let content: String = r.get(0)?;
            let role: Option<String> = if has_role { r.get(1)? } else { None };
            let ts: Option<String> = if has_ts {
                let idx = if has_role { 2 } else { 1 };
                r.get(idx)?
            } else {
                None
            };
            Ok((content, role, ts))
        })?;
        let mut msgs = Vec::new();
        for (content, role, ts) in rows.flatten() {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&content) else {
                continue;
            };
            // The message blob carries its own role; the column wins when set.
            let blob_role = v
                .pointer("/message/role")
                .or_else(|| v.get("role"))
                .and_then(|r| r.as_str());
            let role = role
                .or_else(|| blob_role.map(String::from))
                .unwrap_or_else(|| "assistant".to_string());
            if !matches!(role.as_str(), "user" | "assistant") {
                continue;
            }
            let text = collect_goose_text(&v);
            if text.trim().is_empty() {
                continue;
            }
            let ts = ts.as_deref().and_then(parse_iso_ms);
            msgs.push(SessionMessage { role, text, ts });
        }
        return Ok(msgs);
    }
    bail!("goose session '{id}' not found")
}

/// Recursively collect conversational text from a goose message blob:
/// string values under `text`/`content`/`message` keys; `toolResponse`
/// subtrees are skipped (they are tool output, not turns).
fn collect_goose_text(v: &serde_json::Value) -> String {
    let mut parts = Vec::new();
    collect_goose_text_into(v, &mut parts);
    parts.join("\n")
}

fn collect_goose_text_into(v: &serde_json::Value, parts: &mut Vec<String>) {
    match v {
        serde_json::Value::Array(a) => {
            for item in a {
                collect_goose_text_into(item, parts);
            }
        }
        serde_json::Value::Object(o) => {
            if o.get("type").and_then(|t| t.as_str()) == Some("toolResponse") {
                return;
            }
            for key in ["text", "content", "message"] {
                if let Some(val) = o.get(key) {
                    match val {
                        serde_json::Value::String(s) => parts.push(s.clone()),
                        other => collect_goose_text_into(other, parts),
                    }
                }
            }
        }
        _ => {}
    }
}

// --- Continue (CLI sessions JSON) --------------------------------------------------
//
// Continue writes one JSON file per session under `~/.continue/sessions/`
// (ctx: `continue_cli_sessions_json`; the `sessions.json` index is skipped):
//   {"sessionId":"…","title":"…","createdAt":"…","workspaceDirectory":"…",
//    "history":[{"id":…,"timestamp":…,"message":{"role":"user","content":["…"]}}]}
// `content` may be a plain string (older files). View-only.

fn continue_sessions(limit: usize) -> Vec<SessionMeta> {
    let mut out = Vec::new();
    let Some(dir) = dirs::home_dir().map(|h| h.join(".continue").join("sessions")) else {
        return out;
    };
    let Ok(fs) = std::fs::read_dir(&dir) else { return out };
    let mut files: Vec<(PathBuf, u64)> = fs
        .flatten()
        .map(|f| {
            let p = f.path();
            (p.clone(), mtime_ms(&p))
        })
        .filter(|(p, _)| {
            p.extension().and_then(|x| x.to_str()) == Some("json")
                && p.file_name().and_then(|n| n.to_str()) != Some("sessions.json")
        })
        .collect();
    files.sort_by_key(|f| std::cmp::Reverse(f.1));
    files.truncate(limit);
    for (p, mtime) in files {
        if let Ok(v) = read_json_file(&p) {
            let id = v
                .get("sessionId")
                .and_then(|s| s.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from)
                .unwrap_or_else(|| {
                    p.file_stem()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_default()
                });
            if id.is_empty() {
                continue;
            }
            let title = v
                .get("title")
                .or_else(|| v.get("chatModelTitle"))
                .and_then(|t| t.as_str())
                .map(|t| truncate(t, 80))
                .filter(|t| !t.is_empty())
                .unwrap_or_else(|| {
                    continue_first_user(&v).unwrap_or_else(|| id.clone())
                });
            let cwd = v
                .get("workspaceDirectory")
                .or_else(|| v.get("workspace_directory"))
                .and_then(|c| c.as_str())
                .map(String::from);
            let updated = v
                .get("createdAt")
                .or_else(|| v.get("startedAt"))
                .and_then(|t| t.as_str())
                .and_then(parse_iso_ms)
                .unwrap_or(mtime);
            out.push(SessionMeta {
                provider: "continue".to_string(),
                id,
                title,
                updated,
                cwd,
                resumable: false,
            });
        }
    }
    out
}

fn continue_first_user(v: &serde_json::Value) -> Option<String> {
    for m in continue_messages(v) {
        if m.role == "user" {
            return Some(truncate(&m.text, 80));
        }
    }
    None
}

fn continue_transcript(id: &str) -> Result<Vec<SessionMessage>> {
    if id.contains('/') || id.contains("..") {
        bail!("invalid continue session id");
    }
    let Some(dir) = dirs::home_dir().map(|h| h.join(".continue").join("sessions")) else {
        bail!("continue history not found");
    };
    let Ok(fs) = std::fs::read_dir(&dir) else {
        bail!("continue history not found");
    };
    for f in fs.flatten() {
        let p = f.path();
        if p.extension().and_then(|x| x.to_str()) != Some("json") {
            continue;
        }
        let Ok(v) = read_json_file(&p) else { continue };
        let sid = v
            .get("sessionId")
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from)
            .unwrap_or_else(|| {
                p.file_stem()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default()
            });
        if sid == id {
            return Ok(continue_messages(&v));
        }
    }
    bail!("continue session '{id}' not found")
}

/// history[] → transcript turns (message.role + content, both string and
/// string-array shapes).
fn continue_messages(v: &serde_json::Value) -> Vec<SessionMessage> {
    let Some(history) = v.get("history").and_then(|h| h.as_array()) else {
        return vec![];
    };
    history
        .iter()
        .filter_map(|item| {
            let msg = item.get("message")?;
            let role = msg.get("role").and_then(|r| r.as_str())?;
            if !matches!(role, "user" | "assistant") {
                return None;
            }
            let content = msg.get("content")?;
            let text = match content {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Array(a) => a
                    .iter()
                    .filter_map(|c| c.as_str().map(String::from))
                    .collect::<Vec<_>>()
                    .join("\n"),
                _ => return None,
            };
            if text.trim().is_empty() {
                return None;
            }
            let ts = item
                .get("timestamp")
                .or_else(|| msg.get("timestamp"))
                .and_then(flexible_ts_ms);
            Some(SessionMessage {
                role: role.to_string(),
                text,
                ts,
            })
        })
        .collect()
}

/// Timestamps appear as ISO strings, epoch seconds, or epoch millis.
fn flexible_ts_ms(v: &serde_json::Value) -> Option<u64> {
    match v {
        serde_json::Value::String(s) => parse_iso_ms(s).or_else(|| s.parse::<u64>().ok()),
        serde_json::Value::Number(n) => n.as_f64().map(|f| {
            let ms = if f.abs() < 1e12 { f * 1000.0 } else { f };
            ms.max(0.0) as u64
        }),
        _ => None,
    }
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&content).with_context(|| format!("parse {}", path.display()))
}

// --- process scanning -------------------------------------------------------

/// Full command lines of running processes (`ps -eo args=`). One per line.
fn ps_args() -> String {
    run_cli("ps", &["-eo", "args="]).unwrap_or_default()
}

/// Session ids that appear in a running provider process's argv.
fn parse_active_ids(ps_output: &str) -> Vec<String> {
    let mut ids = Vec::new();
    for line in ps_output.lines() {
        let l = line.to_lowercase();
        let is_provider = l.contains("opencode") || l.contains("claude") || l.contains("codex");
        if !is_provider {
            continue;
        }
        let toks: Vec<&str> = line.split_whitespace().collect();
        for (i, t) in toks.iter().enumerate() {
            // opencode session id (`-s ses_…` / `--session ses_…`), a value
            // that itself is `ses_…`, or a codex uuid after `resume`.
            if t.starts_with("ses_") {
                ids.push(t.to_string());
            } else if *t == "-s" || *t == "--session" || *t == "--resume" || *t == "resume" {
                if let Some(next) = toks.get(i + 1) {
                    ids.push(next.to_string());
                }
            }
        }
    }
    ids.sort();
    ids.dedup();
    ids
}

/// PID of the process whose argv references `id`.
fn pid_for_session(id: &str, _ps: &str) -> Option<u32> {
    let out = run_cli("ps", &["-eo", "pid=,args="])?;
    for line in out.lines() {
        let line = line.trim_start();
        let (pid_str, rest) = line.split_once(char::is_whitespace)?;
        if rest.split_whitespace().any(|t| t == id) {
            return pid_str.parse().ok();
        }
    }
    None
}

// --- helpers ----------------------------------------------------------------

/// Run a CLI, capturing stdout, killed after `CLI_TIMEOUT`. None on any failure.
///
/// Captures into a temp FILE, not a pipe, on purpose: some CLIs (notably
/// `opencode`, on Bun) exit without flushing buffered stdout when it's a pipe,
/// truncating large output non-deterministically at the OS pipe buffer (64/128
/// KiB) — which corrupted `opencode export` of long transcripts so the dialog
/// wouldn't load. A regular file gets the complete output every time, and also
/// avoids the pipe-buffer deadlock (a child blocking mid-write while we wait).
fn run_cli(program: &str, args: &[&str]) -> Option<String> {
    run_cli_in(program, args, CLI_TIMEOUT, None)
}

/// Run a CLI from the user's HOME. Directory-scoped tools — notably
/// `opencode session list`, which returns only the CWD project's sessions —
/// then yield their GLOBAL view instead of whatever project directory the agent
/// happens to be running in (which otherwise hid most sessions).
fn run_cli_home(program: &str, args: &[&str]) -> Option<String> {
    run_cli_in(program, args, CLI_TIMEOUT, dirs::home_dir().as_deref())
}

#[cfg(test)]
fn run_cli_with_timeout(program: &str, args: &[&str], timeout: Duration) -> Option<String> {
    run_cli_in(program, args, timeout, None)
}

fn run_cli_in(program: &str, args: &[&str], timeout: Duration, cwd: Option<&Path>) -> Option<String> {
    let tmp = std::env::temp_dir().join(format!("ra-cli-{}.out", uuid::Uuid::new_v4()));
    let status = (|| {
        let file = std::fs::File::create(&tmp).ok()?;
        let mut cmd = Command::new(program);
        cmd.args(args)
            .stdout(file)
            .stderr(std::process::Stdio::null());
        if let Some(d) = cwd {
            cmd.current_dir(d);
        }
        // Own process group so a timeout SIGKILLs the whole subtree, not just
        // the leader: provider CLIs fork heavy children (`opencode` on Bun,
        // `claude` on node + MCP servers) that would otherwise reparent to init
        // and leak on every timed-out call. Mirrors the executor's group-kill
        // (iter145/146); `kill()` below still reaps the leader, killpg the rest.
        #[cfg(unix)]
        cmd.process_group(0);
        let mut child = cmd.spawn().ok()?;
        let pid = child.id();
        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait() {
                Ok(Some(s)) => break Some(s),
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                _ => {
                    // Timeout/error: SIGKILL the whole group (descendants that
                    // didn't start their own group), then kill+reap the leader —
                    // `kill()` alone reaches only the leader and, without `wait()`,
                    // leaves it a zombie (std Child doesn't reap on drop).
                    crate::executor::shell::kill_process_group(Some(pid));
                    let _ = child.kill();
                    let _ = child.wait();
                    break None;
                }
            }
        }
    })();
    let out = std::fs::read(&tmp)
        .ok()
        .map(|b| String::from_utf8_lossy(&b).to_string());
    let _ = std::fs::remove_file(&tmp);
    if status?.success() {
        out
    } else {
        None
    }
}

fn mtime_ms(p: &Path) -> u64 {
    std::fs::metadata(p)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn read_head(path: &Path, max_bytes: usize) -> Option<String> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).ok()?;
    let mut buf = vec![0u8; max_bytes];
    let n = f.read(&mut buf).ok()?;
    buf.truncate(n);
    Some(String::from_utf8_lossy(&buf).to_string())
}

/// Flatten a message's `content` (string or array of `{type:text,text}`) to text.
fn message_text(message: Option<&serde_json::Value>) -> Option<String> {
    let content = message?.get("content")?;
    if let Some(s) = content.as_str() {
        return Some(s.to_string());
    }
    if let Some(arr) = content.as_array() {
        let mut parts = Vec::new();
        for it in arr {
            if it.get("type").and_then(|t| t.as_str()) == Some("text") {
                if let Some(t) = it.get("text").and_then(|x| x.as_str()) {
                    parts.push(t.to_string());
                }
            }
        }
        if !parts.is_empty() {
            return Some(parts.join("\n"));
        }
    }
    None
}

fn parse_iso_ms(s: &str) -> Option<u64> {
    // Best-effort: rely on chrono if the string parses, else None.
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis() as u64)
}

fn truncate(s: &str, n: usize) -> String {
    let s = s.trim();
    if s.chars().count() <= n {
        s.to_string()
    } else {
        s.chars().take(n).collect::<String>() + "…"
    }
}

/// The resume runner for continuing a provider session non-interactively.
/// The task prompt is appended by the autonomous runner.
pub fn resume_runner(provider: &str, id: &str) -> Result<Vec<String>> {
    match provider {
        "claude" => Ok(vec![
            "claude".into(),
            "-p".into(),
            "--resume".into(),
            id.into(),
        ]),
        "opencode" => Ok(vec![
            "opencode".into(),
            "run".into(),
            "-s".into(),
            id.into(),
        ]),
        "codex" => Ok(vec![
            "codex".into(),
            "exec".into(),
            "resume".into(),
            id.into(),
        ]),
        "cline" | "roo" | "kilo" | "zed" | "cursor" | "gemini" | "qwen" | "goose"
        | "continue" => {
            bail!("'{provider}' sessions are view-only (no headless resume)")
        }
        other => bail!("unknown provider '{other}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_claude_project_dir() {
        assert_eq!(decode_claude_dir("-home-ojo-dev-x"), "/home/ojo/dev/x");
    }

    #[test]
    fn cwd_from_jsonl_picks_first_nonempty() {
        // The leading `mode` record has no cwd; the first user record does. The
        // exact path (underscores intact) must come from the field, never from a
        // lossy decode of the dash-encoded project dir name.
        let jsonl = concat!(
            r#"{"type":"mode","sessionId":"a"}"#,
            "\n",
            r#"{"type":"user","cwd":"/home/ojo/dev/tunshell_mcp_agents","message":{"role":"user","content":"hi"}}"#,
            "\n",
        );
        assert_eq!(
            cwd_from_jsonl(jsonl).as_deref(),
            Some("/home/ojo/dev/tunshell_mcp_agents")
        );
        // No cwd anywhere → None (caller falls back to the lossy dir decode).
        assert_eq!(cwd_from_jsonl(r#"{"type":"mode"}"#), None);
        // Empty cwd is ignored.
        assert_eq!(cwd_from_jsonl(r#"{"type":"user","cwd":""}"#), None);
    }

    #[test]
    fn claude_title_prefers_ai_title_then_user() {
        let jsonl = concat!(
            r#"{"type":"mode","sessionId":"a"}"#,
            "\n",
            r#"{"type":"user","message":{"role":"user","content":"первый вопрос"}}"#,
            "\n",
            r#"{"type":"ai-title","aiTitle":"Сгенерированный заголовок","sessionId":"a"}"#,
            "\n",
        );
        assert_eq!(
            claude_title_from(jsonl).as_deref(),
            Some("Сгенерированный заголовок")
        );

        // No ai-title → first user message (content array form).
        let jsonl2 = concat!(
            r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hello world"}]}}"#,
            "\n",
        );
        assert_eq!(claude_title_from(jsonl2).as_deref(), Some("hello world"));
    }

    #[test]
    fn parses_active_session_ids_from_argv() {
        let ps = concat!(
            "/usr/bin/opencode -s ses_abc123 run\n",
            "node /x/claude --resume 11112222-3333\n",
            "/usr/bin/some-editor file.txt\n",
            "opencode session list\n",
        );
        let ids = parse_active_ids(ps);
        assert!(ids.contains(&"ses_abc123".to_string()));
        assert!(ids.contains(&"11112222-3333".to_string()));
        assert_eq!(ids.len(), 2); // editor + plain list ignored
    }

    #[test]
    fn resume_runner_builds_provider_command() {
        assert_eq!(
            resume_runner("claude", "u1").unwrap(),
            vec!["claude", "-p", "--resume", "u1"]
        );
        assert_eq!(
            resume_runner("opencode", "ses_x").unwrap(),
            vec!["opencode", "run", "-s", "ses_x"]
        );
        assert!(resume_runner("bogus", "x").is_err());
    }

    fn msg(text: &str) -> SessionMessage {
        SessionMessage { role: "assistant".into(), text: text.into(), ts: None }
    }

    #[test]
    fn cap_transcript_keeps_short_history_untouched() {
        let v = vec![msg("a"), msg("b"), msg("c")];
        let out = cap_transcript(v.clone());
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].text, "a"); // no marker prepended
    }

    #[test]
    fn cap_transcript_truncates_by_message_count_keeping_the_tail() {
        let v: Vec<_> = (0..MAX_TRANSCRIPT_MESSAGES + 50).map(|i| msg(&i.to_string())).collect();
        let out = cap_transcript(v);
        assert_eq!(out.len(), MAX_TRANSCRIPT_MESSAGES + 1); // +1 marker
        assert_eq!(out[0].role, "system");
        assert!(out[0].text.contains("опущены"));
        // The tail is preserved; the last original message is still last.
        assert_eq!(out.last().unwrap().text, (MAX_TRANSCRIPT_MESSAGES + 49).to_string());
    }

    #[test]
    fn cap_transcript_truncates_by_bytes() {
        // Each message ~10 KiB; well past the byte cap in far fewer than the
        // message-count cap, so the byte limit is what triggers truncation.
        let big = "x".repeat(10_000);
        let v: Vec<_> = (0..200).map(|_| msg(&big)).collect();
        let out = cap_transcript(v);
        assert_eq!(out[0].role, "system"); // truncated → marker present
        let kept = out.len() - 1;
        assert!(kept < 200, "should have dropped some (kept {kept})");
        // Kept text stays within the byte budget.
        let bytes: usize = out.iter().skip(1).map(|m| m.text.len()).sum();
        assert!(bytes <= MAX_TRANSCRIPT_BYTES, "kept {bytes} bytes");
    }

    #[cfg(unix)]
    #[test]
    fn run_cli_captures_output_larger_than_pipe_buffer() {
        // `seq 1 50000` emits ~250 KiB — well past the 64 KiB OS pipe buffer
        // that truncated pipe-captured output. Temp-file capture gets it whole.
        let out = run_cli("seq", &["1", "50000"]).expect("seq ran");
        assert!(out.len() > 64 * 1024, "truncated to {} bytes", out.len());
        assert_eq!(out.lines().next_back(), Some("50000")); // got the full tail
    }

    #[cfg(unix)]
    #[test]
    fn run_cli_timeout_kills_and_reaps_child() {
        // `exec sleep <unique>` so the spawned child IS the sleeper. Run it on a
        // thread, grab its pid while alive, then check it's fully gone (reaped,
        // not a zombie) after the timeout — `kill -0` succeeds on a zombie.
        let marker = "sleep 88.123";
        let h = std::thread::spawn(|| {
            run_cli_with_timeout("sh", &["-c", "exec sleep 88.123"], Duration::from_millis(700))
        });
        std::thread::sleep(Duration::from_millis(250));
        let pid = Command::new("pgrep")
            .args(["-f", marker])
            .output()
            .ok()
            .and_then(|o| String::from_utf8_lossy(&o.stdout).split_whitespace().next().map(String::from))
            .expect("sleep should be running before the timeout");

        assert!(h.join().unwrap().is_none(), "timed-out CLI returns None");
        std::thread::sleep(Duration::from_millis(200));
        let alive = Command::new("kill")
            .args(["-0", &pid])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        assert!(!alive, "timed-out child pid {pid} was not killed+reaped");
    }

    #[test]
    fn opencode_message_parses_current_and_legacy_shapes() {
        // Current opencode 1.x: role/time under `info`, text under `parts`;
        // tool/step-start/step-finish parts are dropped.
        let cur = serde_json::json!({
            "info": {"role": "assistant", "time": {"created": 1781625315855u64}},
            "parts": [
                {"type": "step-start"},
                {"type": "text", "text": "Проверю список"},
                {"type": "tool", "tool": "exec"},
                {"type": "text", "text": "готово"},
                {"type": "step-finish"}
            ]
        });
        let m = opencode_message(&cur).unwrap();
        assert_eq!(m.role, "assistant");
        assert_eq!(m.text, "Проверю список\nготово");
        assert_eq!(m.ts, Some(1781625315855));

        // A message with only non-text parts yields nothing.
        let toolonly = serde_json::json!({
            "info": {"role": "assistant"},
            "parts": [{"type": "tool"}, {"type": "step-finish"}]
        });
        assert!(opencode_message(&toolonly).is_none());

        // Legacy flat shape still works.
        let legacy = serde_json::json!({"role": "user", "content": "привет", "time": 123u64});
        let lm = opencode_message(&legacy).unwrap();
        assert_eq!(lm.role, "user");
        assert_eq!(lm.text, "привет");
        assert_eq!(lm.ts, Some(123));
    }

    #[test]
    fn zed_thread_parses_externally_tagged_messages() {
        // DbThread JSON (v0.3.0): messages is an externally-tagged enum array.
        let thread = serde_json::json!({
            "version": "0.3.0",
            "title": "t",
            "messages": [
                {"User": {"id": "u1", "content": [{"Text": "привет"}, {"Image": {}}]}},
                {"Agent": {"content": [
                    {"Thinking": {"text": "hmm", "signature": null}},
                    {"Text": "ответ"},
                    {"ToolUse": {}}
                ]}},
                "Resume",
                {"Compaction": {"Summary": "…"}},
                {"Agent": {"content": [{"ToolUse": {}}]}}
            ]
        });
        let msgs = parse_zed_thread(&serde_json::to_vec(&thread).unwrap()).unwrap();
        // Resume/Compaction skipped; the tool-only Agent message (no Text) skipped.
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].role, "user");
        assert_eq!(msgs[0].text, "привет"); // Image content dropped
        assert_eq!(msgs[1].role, "assistant");
        assert_eq!(msgs[1].text, "ответ"); // Thinking/ToolUse dropped
    }

    #[test]
    fn zed_db_uri_percent_encodes_path() {
        let uri = zed_db_uri(Path::new("/home/u/Application Support/Zed/threads.db"));
        assert!(uri.starts_with("file:/home/u/Application%20Support/Zed/threads.db"));
        assert!(uri.ends_with("?immutable=1&mode=ro"));
    }

    #[test]
    fn vscode_agents_are_view_only() {
        for p in ["cline", "roo", "kilo", "zed", "cursor"] {
            assert!(
                resume_runner(p, "task-1").is_err(),
                "{p} must not be resumable"
            );
        }
    }

    #[test]
    fn cursor_message_parses_roles_and_skips_noise() {
        // Top-level role is authoritative; content blocks under message.content.
        let user = r#"{"timestamp":"2026-07-30T18:38:00.000Z","role":"user","message":{"content":[{"type":"text","text":"fix the build"}]}}"#;
        let m = cursor_message(user).unwrap();
        assert_eq!(m.role, "user");
        assert_eq!(m.text, "fix the build");
        assert_eq!(m.ts, Some(parse_iso_ms("2026-07-30T18:38:00.000Z").unwrap()));

        // Multi-block assistant message: text blocks joined, tool_use dropped.
        let assistant = r#"{"role":"assistant","message":{"content":[{"type":"text","text":"looking"},{"type":"tool_use","name":"edit","input":{}},{"type":"text","text":"now"}]}}"#;
        let m = cursor_message(assistant).unwrap();
        assert_eq!(m.role, "assistant");
        assert_eq!(m.text, "looking\nnow");

        // Legacy nested role that agrees is retained; disagreeing is dropped.
        let nested = r#"{"role":"user","message":{"role":"user","content":[{"type":"text","text":"ok"}]}}"#;
        assert_eq!(cursor_message(nested).unwrap().text, "ok");
        let disagree = r#"{"role":"user","message":{"role":"assistant","content":[{"type":"text","text":"MUST_NOT_EMIT"}]}}"#;
        assert!(cursor_message(disagree).is_none());

        // Top-level content (no message wrapper) is the legacy fallback.
        let flat = r#"{"role":"assistant","content":[{"type":"text","text":"flat"}]}"#;
        assert_eq!(cursor_message(flat).unwrap().text, "flat");

        // Noise: summary / turn_ended / status carriers / unknown roles /
        // tool-only messages / broken JSON.
        for noise in [
            r#"{"event":"summary","message":{"content":[{"type":"text","text":"x"}]}}"#,
            r#"{"type":"turn_ended","status":"completed"}"#,
            r#"{"role":"system","message":{"content":[{"type":"text","text":"x"}]}}"#,
            r#"{"role":"user","message":{"content":[{"type":"tool_result","content":"…"}]}}"#,
            "not json at all",
        ] {
            assert!(cursor_message(noise).is_none(), "must skip: {noise}");
        }
    }

    #[test]
    fn cursor_title_unwraps_user_query_tags() {
        // Cursor wraps user prompts: <timestamp>…</timestamp>\n<user_query>…</user_query>
        let wrapped = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":""#,
            r#"<timestamp>Thursday, Jul 30, 2026, 11:38 AM (UTC-7)</timestamp>"#,
            r#"\n<user_query>починить миграцию</user_query>"#,
            r#""}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"text","text":"answer"}]}}"#,
        );
        let dir = std::env::temp_dir().join(format!("ra-cursor-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("session.jsonl");
        std::fs::write(&p, wrapped).unwrap();
        assert_eq!(cursor_title(&p).as_deref(), Some("починить миграцию"));
        std::fs::remove_dir_all(&dir).ok();

        // No wrapper → the raw text is the title.
        assert_eq!(
            unwrap_user_query("  plain prompt  "),
            "plain prompt"
        );
        assert_eq!(unwrap_user_query("<user_query>tagged</user_query>"), "tagged");
    }

    #[test]
    fn cursor_session_id_rejects_path_traversal() {
        assert!(cursor_transcript("../etc/passwd").is_err());
        assert!(cursor_transcript("a/b").is_err());
        assert!(cursor_transcript("normal-id").is_err()); // not found, not traversed
    }

    #[test]
    fn codex_message_parses_roles_and_filters_injections() {
        // session_meta / turn_context / event_msg / reasoning / tool records
        // are not turns.
        for noise in [
            r#"{"type":"session_meta","payload":{"id":"x"}}"#,
            r#"{"type":"turn_context","payload":{}}"#,
            r#"{"type":"event_msg","payload":{"type":"task_started"}}"#,
            r#"{"type":"response_item","payload":{"type":"reasoning","summary":[]}}"#,
            r#"{"type":"response_item","payload":{"type":"function_call","name":"sh"}}"#,
            r#"{"type":"response_item","payload":{"type":"message","role":"developer","content":[{"type":"input_text","text":"<permissions instructions>"}]}}"#,
        ] {
            assert!(codex_message(noise).is_none(), "must skip: {noise}");
        }

        // Injected context in user turns is dropped…
        for injected in [
            "# AGENTS.md instructions for /repo",
            "<environment_context>\n <cwd>/repo</cwd>",
            "<user_instructions>extra</user_instructions>",
            "<permissions instructions>read-only",
        ] {
            let v = serde_json::json!({
                "type": "response_item",
                "payload": {"type": "message", "role": "user",
                            "content": [{"type": "input_text", "text": injected}]}
            });
            assert!(
                codex_message(&v.to_string()).is_none(),
                "must drop injection: {injected}"
            );
        }

        // …while a real prompt passes with the envelope timestamp.
        let real = r#"{"timestamp":"2026-03-25T00:54:35.208Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"починить сборку"}]}}"#;
        let m = codex_message(real).unwrap();
        assert_eq!(m.role, "user");
        assert_eq!(m.text, "починить сборку");
        assert_eq!(m.ts, Some(parse_iso_ms("2026-03-25T00:54:35.208Z").unwrap()));

        // Assistant output_text blocks (several shapes) are joined.
        let a = r#"{"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"},{"type":"summary_text","text":"summary"},{"type":"text","text":"tails"}]}}"#;
        assert_eq!(codex_message(a).unwrap().text, "done\nsummary\ntails");
    }

    #[test]
    fn codex_session_id_and_cwd_from_meta_or_filename() {
        let dir = std::env::temp_dir().join(format!("ra-codex-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("rollout-2026-03-25T00-54-35-019d21d7-aa46-7593-bdd0-b04ca0def3e1.jsonl");
        std::fs::write(&p, concat!(
            r#"{"timestamp":"…","type":"session_meta","payload":{"id":"019d21d7-aa46-7593-bdd0-b04ca0def3e1","cwd":"/repo"}}"#,
            "\n",
            r#"{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"привет"}]}}"#,
        )).unwrap();
        let (id, cwd) = codex_session_id_and_cwd(&p);
        assert_eq!(id, "019d21d7-aa46-7593-bdd0-b04ca0def3e1");
        assert_eq!(cwd.as_deref(), Some("/repo"));
        assert_eq!(codex_title(&p).as_deref(), Some("привет"));
        std::fs::remove_dir_all(&dir).ok();

        // No header → the uuid after the filename timestamp.
        let (id, cwd) = codex_session_id_and_cwd(Path::new(
            "/x/rollout-2026-03-25T00-54-35-019d21d7-aa46-7593-bdd0-b04ca0def3e1.jsonl",
        ));
        assert_eq!(id, "019d21d7-aa46-7593-bdd0-b04ca0def3e1");
        assert_eq!(cwd, None);
    }

    #[test]
    fn gemini_family_message_parses_user_and_assistant_tags() {
        for (tag, role) in [("gemini", "assistant"), ("qwen", "assistant"), ("assistant", "assistant")] {
            let line = format!(
                r#"{{"id":"r1","timestamp":"2026-04-01T10:00:00Z","type":"{tag}","content":"ответ"}}"#
            );
            let m = gemini_family_message(&line).unwrap();
            assert_eq!(m.role, role, "tag {tag}");
            assert_eq!(m.text, "ответ");
            assert!(m.ts.is_some());
        }

        let user = r#"{"id":"r0","type":"user","content":"вопрос"}"#;
        assert_eq!(gemini_family_message(user).unwrap().role, "user");

        // Header / tool / notice / unknown lines are noise.
        for noise in [
            r#"{"sessionId":"s","directories":["/r"]}"#,
            r#"{"id":"r2","type":"user","content":"x","toolCalls":[{"name":"sh"}]}"#,
            r#"{"id":"r3","type":"user","content":"x","result":{"ok":true}}"#,
            r#"{"id":"r4","$set":{"summary":"…"}}"#,
            r#"{"id":"r5","$rewindTo":"r2"}"#,
            r#"{"id":"r6","type":"system","content":"x"}"#,
            "garbage",
        ] {
            assert!(gemini_family_message(noise).is_none(), "must skip: {noise}");
        }
    }

    #[test]
    fn gemini_family_header_reads_session_and_cwd() {
        let dir = std::env::temp_dir().join(format!("ra-gem-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("chat.jsonl");
        std::fs::write(&p, concat!(
            r#"{"sessionId":"abc-123","startTime":"…","directories":["/repo","/other"]}"#,
            "\n",
            r#"{"id":"r0","type":"user","content":"первый вопрос"}"#,
            "\n",
            r#"{"id":"r1","type":"gemini","content":"ответ"}"#,
        )).unwrap();
        let (id, cwd) = gemini_family_header(&p);
        assert_eq!(id.as_deref(), Some("abc-123"));
        assert_eq!(cwd.as_deref(), Some("/repo"));
        assert_eq!(gemini_family_title(&p).as_deref(), Some("первый вопрос"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn goose_text_collector_walks_and_skips_tool_responses() {
        let v = serde_json::json!({
            "type": "message",
            "message": {
                "role": "user",
                "content": [
                    {"type": "text", "text": "погнали"},
                    {"type": "toolResponse", "content": "huge tool output"},
                ]
            }
        });
        assert_eq!(collect_goose_text(&v), "погнали");
        // Flatter shapes (text/content/message keys at any depth) also work.
        assert_eq!(
            collect_goose_text(&serde_json::json!({"content": [{"text": "a"}, {"message": "b"}]})),
            "a\nb"
        );
    }

    #[test]
    fn continue_messages_parses_history_items() {
        let v = serde_json::json!({
            "sessionId": "cs-1",
            "title": "T",
            "createdAt": "2026-02-01T00:00:00Z",
            "workspaceDirectory": "/repo",
            "history": [
                {"id": 1, "message": {"role": "user", "content": ["первое", "второе"]}},
                {"id": 2, "message": {"role": "assistant", "content": "готово"}, "timestamp": 1767000000},
                {"id": 3, "message": {"role": "tool", "content": "x"}},
                {"id": 4, "editorText": "no message here"},
            ]
        });
        let msgs = continue_messages(&v);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].role, "user");
        assert_eq!(msgs[0].text, "первое\nвторое");
        assert_eq!(msgs[1].role, "assistant");
        assert_eq!(msgs[1].text, "готово");
        // epoch seconds → ms
        assert_eq!(msgs[1].ts, Some(1_767_000_000_000));
        assert_eq!(continue_first_user(&v).as_deref(), Some("первое\nвторое"));
    }

    #[test]
    fn cline_message_maps_roles_and_skips_noise() {
        let task = serde_json::json!({"ts":1,"type":"say","say":"task","text":"做 X"});
        let m = cline_message(&task).unwrap();
        assert_eq!(m.role, "user");
        assert_eq!(m.text, "做 X");
        assert_eq!(m.ts, Some(1));

        let say_text = serde_json::json!({"ts":2,"type":"say","say":"text","text":"done"});
        assert_eq!(cline_message(&say_text).unwrap().role, "assistant");

        let feedback = serde_json::json!({"type":"say","say":"user_feedback","text":"no, retry"});
        assert_eq!(cline_message(&feedback).unwrap().role, "user");

        // Tool / api noise is dropped from the transcript.
        for noise in ["api_req_started", "tool", "command", "command_output"] {
            let v = serde_json::json!({"type":"say","say":noise,"text":"{}"});
            assert!(cline_message(&v).is_none(), "{noise} should be skipped");
        }

        // Empty text is dropped even for a conversational role.
        let empty = serde_json::json!({"type":"say","say":"text","text":"   "});
        assert!(cline_message(&empty).is_none());
    }

    #[test]
    fn cline_text_unwraps_json_followup() {
        // followup asks wrap the prompt as JSON {question, options}.
        let v = serde_json::json!({
            "type":"ask","ask":"followup",
            "text":"{\"question\":\"which env?\",\"options\":[\"dev\",\"prod\"]}"
        });
        let m = cline_message(&v).unwrap();
        assert_eq!(m.role, "assistant");
        assert_eq!(m.text, "which env?");

        // A plain (non-JSON) text passes through untouched.
        let plain = serde_json::json!({"type":"say","say":"text","text":"plain answer"});
        assert_eq!(cline_message(&plain).unwrap().text, "plain answer");
    }

    #[test]
    fn vscode_title_prefers_task_then_first_user() {
        let dir = std::env::temp_dir().join(format!("ra-vsc-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("ui_messages.json");

        let msgs = serde_json::json!([
            {"type":"say","say":"api_req_started","text":"{}"},
            {"type":"say","say":"task","text":"Fix the build on CI"},
            {"type":"say","say":"text","text":"Sure, looking now"},
        ]);
        std::fs::write(&p, serde_json::to_vec(&msgs).unwrap()).unwrap();
        assert_eq!(vscode_title(&p).as_deref(), Some("Fix the build on CI"));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A timed-out provider CLI must take its whole subtree down, not just the
    /// leader. We run `sh -c "<sleep> & wait"`: the leader is `sh`, the long
    /// `sleep` is a backgrounded grandchild. Without the group-kill, SIGKILLing
    /// only `sh` reparents the `sleep` to init and leaks it; `process_group(0)` +
    /// killpg reaches it. The sleep duration is a unique marker so a stray from a
    /// broken run is found by pgrep and reaped by PID (not `pkill -f`, which would
    /// also match this test's own shell).
    #[cfg(unix)]
    #[test]
    fn timed_out_cli_kills_backgrounded_grandchild() {
        let marker = "92731"; // distinctive sleep length, in seconds
        let script = format!("sleep {marker} & wait");
        let start = Instant::now();
        let out = run_cli_with_timeout("sh", &["-c", &script], Duration::from_millis(300));
        assert!(out.is_none(), "a timed-out CLI must yield None");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "must not block waiting on the grandchild"
        );
        // Let the SIGKILL propagate, then assert no `sleep <marker>` survives.
        std::thread::sleep(Duration::from_millis(200));
        let pgrep = Command::new("pgrep")
            .args(["-f", &format!("sleep {marker}")])
            .output()
            .expect("pgrep");
        let stdout = String::from_utf8_lossy(&pgrep.stdout);
        let survivors: Vec<&str> = stdout.split_whitespace().collect();
        // Reap any leak by PID before asserting, so a failure doesn't poison the
        // next run with a stray process.
        for pid in &survivors {
            if let Ok(p) = pid.parse::<i32>() {
                unsafe {
                    libc::kill(p, libc::SIGKILL);
                }
            }
        }
        assert!(
            survivors.is_empty(),
            "leaked a backgrounded grandchild: {survivors:?}"
        );
    }
}
