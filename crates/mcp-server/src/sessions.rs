//! AI-provider session discovery: list/read/terminate the host's local
//! `claude` and `opencode` conversations so the web panel can surface them as
//! dialogs (with host + provider labels), continue them, and flag live ones.
//!
//! - claude: JSONL transcripts under `~/.claude/projects/<proj>/<uuid>.jsonl`.
//! - opencode: queried via its CLI (`opencode session list/export`), so the
//!   1.3 GB SQLite store is never touched directly.

use anyhow::{bail, Context, Result};
use remote_agents_shared::{SessionMessage, SessionMeta};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::{Duration, Instant, UNIX_EPOCH};

/// Cap per provider so a huge history doesn't blow up the list.
const MAX_PER_PROVIDER: usize = 60;
/// Re-scan no more often than this (the list is polled by the panel).
const CACHE_TTL: Duration = Duration::from_secs(30);
/// Bound on a provider CLI call.
const CLI_TIMEOUT: Duration = Duration::from_secs(8);

static CACHE: Mutex<Option<(Instant, Vec<SessionMeta>)>> = Mutex::new(None);

/// All provider sessions on this host (metadata only), newest first. Cached.
pub fn list_sessions() -> Vec<SessionMeta> {
    if let Ok(guard) = CACHE.lock() {
        if let Some((at, v)) = guard.as_ref() {
            if at.elapsed() < CACHE_TTL {
                return v.clone();
            }
        }
    }
    let mut all = claude_sessions();
    all.extend(opencode_sessions());
    all.extend(vscode_agent_sessions());
    all.sort_by_key(|s| std::cmp::Reverse(s.updated));
    if let Ok(mut g) = CACHE.lock() {
        *g = Some((Instant::now(), all.clone()));
    }
    all
}

/// Drop the cached session list so the next `list_sessions` re-scans. Called
/// when an autonomous task finishes: a chat turn (`claude -p` / `opencode run`)
/// just created or extended a provider session, and the web wants to adopt it
/// immediately (to resume it for context), not after the 30s TTL.
pub fn invalidate_cache() {
    if let Ok(mut g) = CACHE.lock() {
        *g = None;
    }
}

/// Full transcript of one session.
pub fn get_transcript(provider: &str, id: &str) -> Result<Vec<SessionMessage>> {
    match provider {
        "claude" => claude_transcript(id),
        "opencode" => opencode_transcript(id),
        "cline" | "roo" | "kilo" => vscode_agent_transcript(provider, id),
        other => bail!("unknown provider '{other}'"),
    }
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

fn claude_sessions() -> Vec<SessionMeta> {
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
    files.truncate(MAX_PER_PROVIDER);
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

fn opencode_sessions() -> Vec<SessionMeta> {
    let Some(out) = run_cli(
        "opencode",
        &["session", "list", "--format", "json", "-n", "60"],
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
    let out = run_cli("opencode", &["export", id]).context("opencode export failed")?;
    let v: serde_json::Value = serde_json::from_str(&out)?;
    // opencode export shape can vary; pull a `messages` array of {role, ...}.
    let msgs = v
        .get("messages")
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or_default();
    Ok(msgs
        .iter()
        .filter_map(|m| {
            let role = m.get("role").and_then(|r| r.as_str())?.to_string();
            let text = message_text(Some(m)).or_else(|| {
                m.get("content").and_then(|c| c.as_str()).map(String::from)
            })?;
            Some(SessionMessage { role, text, ts: m.get("time").and_then(|t| t.as_u64()) })
        })
        .collect())
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

fn vscode_agent_sessions() -> Vec<SessionMeta> {
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
        dirs.truncate(MAX_PER_PROVIDER);
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
        let is_provider = l.contains("opencode") || l.contains("claude");
        if !is_provider {
            continue;
        }
        let toks: Vec<&str> = line.split_whitespace().collect();
        for (i, t) in toks.iter().enumerate() {
            // opencode session id (`-s ses_…` / `--session ses_…`) or a value
            // that itself is `ses_…`.
            if t.starts_with("ses_") {
                ids.push(t.to_string());
            } else if *t == "-s" || *t == "--session" || *t == "--resume" {
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
fn run_cli(program: &str, args: &[&str]) -> Option<String> {
    let mut child = Command::new(program)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + CLI_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            _ => {
                let _ = child.kill();
                return None;
            }
        }
    }
    let out = child.wait_with_output().ok()?;
    if out.status.success() {
        Some(String::from_utf8_lossy(&out.stdout).to_string())
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
        "cline" | "roo" | "kilo" => {
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

    #[test]
    fn vscode_agents_are_view_only() {
        for p in ["cline", "roo", "kilo"] {
            assert!(
                resume_runner(p, "task-1").is_err(),
                "{p} must not be resumable"
            );
        }
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
}
