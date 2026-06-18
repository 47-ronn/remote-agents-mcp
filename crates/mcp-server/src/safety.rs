//! Security policy enforcement.
//!
//! Centralizes all gating decisions so they cannot diverge between call sites:
//!   * command policy per [`AgentMode`] (Plan read-only whitelist + always-on
//!     catastrophic denylist),
//!   * path allow/deny with lexical normalization (so `..` traversal can't
//!     escape the configured lists), and
//!   * file size limits.

use crate::config::SecurityConfig;
use anyhow::{bail, Result};
use remote_agents_shared::AgentMode;
use std::path::{Component, Path, PathBuf};

/// Catastrophic command patterns that are ALWAYS denied, regardless of mode
/// (even `Bypass`). This is the last-resort safety net and is not configurable.
const HARD_DENIED: &[&str] = &[
    "rm -rf /",
    "rm -rf /*",
    ":(){:|:&};:",
    "mkfs",
    "dd if=/dev/zero",
    "> /dev/sda",
    "> /dev/sdb",
];

/// Read-only `git` subcommands permitted in Plan mode.
const GIT_RO: &[&str] = &[
    "status", "log", "diff", "show", "branch", "remote", "ls-files", "ls-tree",
    "rev-parse", "describe", "blame", "tag", "shortlog", "reflog", "cat-file",
    "config", "whatchanged", "grep",
];

/// Read-only `cargo` subcommands permitted in Plan mode.
const CARGO_RO: &[&str] = &["check", "tree", "metadata", "search", "fmt"];

/// Read-only `npm` subcommands permitted in Plan mode.
const NPM_RO: &[&str] = &["ls", "list", "view", "outdated", "audit", "ping"];

/// Default commands permitted in Plan (read-only) mode.
pub fn default_readonly_commands() -> Vec<String> {
    [
        "ls", "cat", "pwd", "echo", "printf", "grep", "egrep", "fgrep", "rg",
        "find", "head", "tail", "wc", "sort", "uniq", "cut", "awk", "sed", "tr",
        "column", "nl", "diff", "ps", "df", "du", "whoami", "hostname", "uname",
        "date", "uptime", "free", "id", "env", "printenv", "which", "type",
        "stat", "file", "tree", "realpath", "dirname", "basename", "readlink",
        "ip", "ifconfig", "netstat", "ss", "dig", "nslookup", "ping", "top",
        "htop", "lsblk", "lscpu", "lsof", "jq", "yq", "md5sum", "sha256sum",
        "git", "cargo", "npm",
        // More read-only text/inspection/system tools (iter125):
        "tac", "rev", "comm", "paste", "join", "fold", "fmt", "expand",
        "unexpand", "numfmt", "seq", "factor", "look", "xxd", "hexdump", "od",
        "strings", "base64", "base32", "cksum", "sha1sum", "sha512sum", "b2sum",
        "pgrep", "pidof", "nproc", "arch", "getconf", "locale", "tty", "groups",
        "who", "w", "host", "whois", "lsmod", "lspci", "lsusb", "findmnt",
        "getent", "ldd", "readelf", "nm", "objdump", "cal", "whatis", "apropos",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// Check whether a shell command is permitted in the current mode.
pub fn check_command_allowed(command: &str, mode: AgentMode, sec: &SecurityConfig) -> Result<()> {
    if mode == AgentMode::Disabled {
        bail!("Agent is disabled");
    }

    // Catastrophic + user denylist always apply, even in Bypass.
    let lc = command.to_lowercase();
    let user_denied = sec.denied_commands.iter().map(String::as_str);
    for pattern in HARD_DENIED.iter().copied().chain(user_denied) {
        if !pattern.is_empty() && lc.contains(&pattern.to_lowercase()) {
            bail!("Command blocked by denylist (matched '{}')", pattern);
        }
    }

    match mode {
        AgentMode::Bypass | AgentMode::Edit => Ok(()),
        AgentMode::Plan => {
            // Redirection writes to files, which is not read-only.
            if command.contains('>') {
                bail!("Plan mode: output redirection (`>`) is not allowed");
            }
            // Command / process substitution runs nested commands that the
            // per-segment whitelist never sees (the shell evaluates them before
            // the outer program). `>(...)` is already caught by the `>` rule
            // above; block `$(...)`, backticks, and `<(...)` too.
            if command.contains("$(") || command.contains('`') || command.contains("<(") {
                bail!("Plan mode: command substitution is not allowed");
            }
            for segment in split_segments(command) {
                let seg = segment.trim();
                if seg.is_empty() {
                    continue;
                }
                if !is_readonly_segment(seg, sec) {
                    bail!(
                        "Plan mode: '{}' is not an allowed read-only command",
                        first_program(seg).unwrap_or("?")
                    );
                }
            }
            Ok(())
        }
        AgentMode::Disabled => unreachable!("handled above"),
    }
}

/// Split a command into segments on shell control operators. Naive (does not
/// honour quoting) but conservative: it can only ever produce *more* segments
/// to check, never fewer, so it cannot be used to smuggle a command past the
/// Plan-mode whitelist.
fn split_segments(command: &str) -> impl Iterator<Item = &str> {
    command.split(['|', ';', '&', '\n'])
}

/// First token of a segment that is an actual program name (skipping leading
/// `VAR=value` environment assignments), with any directory prefix stripped.
fn first_program(segment: &str) -> Option<&str> {
    for tok in segment.split_whitespace() {
        if tok.contains('=') && !tok.starts_with('-') {
            continue; // env assignment prefix, e.g. `LANG=C grep ...`
        }
        return Some(basename(tok));
    }
    None
}

fn basename(token: &str) -> &str {
    token.rsplit('/').next().unwrap_or(token)
}

fn is_readonly_segment(segment: &str, sec: &SecurityConfig) -> bool {
    let mut tokens = segment.split_whitespace().peekable();

    // Skip leading `VAR=value` assignments.
    let prog = loop {
        match tokens.next() {
            Some(t) if t.contains('=') && !t.starts_with('-') => continue,
            Some(t) => break basename(t),
            None => return true, // assignments only / empty
        }
    };

    if !sec.readonly_commands.iter().any(|c| c == prog) {
        return false;
    }

    // Tools that can both read and write require a read-only subcommand.
    // The subcommand is the first non-flag token after the program.
    let sub = tokens.find(|t| !t.starts_with('-')).map(basename);
    match prog {
        "git" => matches!(sub, Some(s) if GIT_RO.contains(&s)),
        "cargo" => sub.is_none_or(|s| CARGO_RO.contains(&s)),
        "npm" => matches!(sub, Some(s) if NPM_RO.contains(&s)),
        // Whitelisted, but each can write or run other commands via specific
        // flags/builtins — allow only when used purely for reading.
        "find" => find_is_readonly(segment),
        "sed" => !sed_in_place(segment),
        "awk" | "gawk" | "mawk" => !segment.contains("system("),
        _ => true,
    }
}

/// `find` action predicates that write files or run commands — disallowed in
/// read-only mode (`-printf`/`-print` to stdout stay allowed).
const FIND_WRITE_ACTIONS: &[&str] = &[
    "-exec", "-execdir", "-ok", "-okdir", "-delete", "-fprintf", "-fprint", "-fls",
];

fn find_is_readonly(segment: &str) -> bool {
    !segment
        .split_whitespace()
        .any(|t| FIND_WRITE_ACTIONS.contains(&t))
}

/// `sed -i` / `--in-place` edits files in place — a write.
fn sed_in_place(segment: &str) -> bool {
    segment.split_whitespace().any(|t| {
        t == "-i" || t.starts_with("-i") || t == "--in-place" || t.starts_with("--in-place=")
    })
}

/// Check whether a path may be read or written under the current policy.
pub fn check_path_allowed(path: &str, sec: &SecurityConfig) -> Result<()> {
    let normalized = normalize(path);

    // Denylist takes precedence and always applies.
    for denied in &sec.denied_paths {
        if normalized.starts_with(normalize(denied)) {
            bail!("Path denied by policy: {}", path);
        }
    }

    // Empty allow list means everything (not already denied) is permitted.
    if sec.allowed_paths.is_empty() {
        return Ok(());
    }
    for allowed in &sec.allowed_paths {
        if normalized.starts_with(normalize(allowed)) {
            return Ok(());
        }
    }
    bail!("Path not in allowed list: {}", path)
}

/// Lexically normalize a path: resolve `.`/`..` and make it absolute against
/// the current working directory WITHOUT touching the filesystem (so it works
/// for write targets that don't exist yet, and `..` can't escape the lists).
fn normalize(path: &str) -> PathBuf {
    let p = Path::new(path);
    let mut out = if p.is_absolute() {
        PathBuf::new()
    } else {
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"))
    };

    for comp in p.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::RootDir => out = PathBuf::from("/"),
            Component::Prefix(prefix) => out = PathBuf::from(prefix.as_os_str()),
            Component::Normal(c) => out.push(c),
        }
    }
    out
}

/// Enforce the configured maximum file size (0 = unlimited).
pub fn check_size(bytes: u64, sec: &SecurityConfig) -> Result<()> {
    if sec.max_file_size > 0 && bytes > sec.max_file_size {
        bail!(
            "File size {} bytes exceeds limit of {} bytes",
            bytes,
            sec.max_file_size
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sec() -> SecurityConfig {
        SecurityConfig::default()
    }

    #[test]
    fn plan_allows_readonly() {
        let s = sec();
        assert!(check_command_allowed("ls -la /tmp", AgentMode::Plan, &s).is_ok());
        assert!(check_command_allowed("cat foo.txt | grep bar", AgentMode::Plan, &s).is_ok());
        assert!(check_command_allowed("git status", AgentMode::Plan, &s).is_ok());
        assert!(check_command_allowed("LANG=C grep x f", AgentMode::Plan, &s).is_ok());
    }

    #[test]
    fn plan_blocks_writes() {
        let s = sec();
        assert!(check_command_allowed("rm file", AgentMode::Plan, &s).is_err());
        assert!(check_command_allowed("git commit -m x", AgentMode::Plan, &s).is_err());
        assert!(check_command_allowed("echo hi > f", AgentMode::Plan, &s).is_err());
        assert!(check_command_allowed("ls | rm -rf foo", AgentMode::Plan, &s).is_err());
    }

    #[test]
    fn plan_blocks_command_substitution() {
        // The outer program is whitelisted, but the shell evaluates the nested
        // command first — so these must be rejected in read-only mode.
        let s = sec();
        assert!(check_command_allowed("echo $(touch f)", AgentMode::Plan, &s).is_err());
        assert!(check_command_allowed("echo `touch f`", AgentMode::Plan, &s).is_err());
        assert!(check_command_allowed("cat <(touch f)", AgentMode::Plan, &s).is_err());
        // A normal variable expansion is still fine (not a command substitution).
        assert!(check_command_allowed("echo ${HOME}", AgentMode::Plan, &s).is_ok());
        // Edit/Bypass are unaffected by this read-only-only restriction.
        assert!(check_command_allowed("echo $(date)", AgentMode::Edit, &s).is_ok());
    }

    #[test]
    fn plan_blocks_write_capable_whitelisted_tools() {
        let s = sec();
        // find: read-only traversal is fine; write/exec actions are not.
        assert!(check_command_allowed("find . -name '*.rs'", AgentMode::Plan, &s).is_ok());
        assert!(check_command_allowed("find . -printf '%p\\n'", AgentMode::Plan, &s).is_ok());
        assert!(check_command_allowed("find /tmp -exec touch {} ;", AgentMode::Plan, &s).is_err());
        assert!(check_command_allowed("find /tmp -delete", AgentMode::Plan, &s).is_err());
        assert!(check_command_allowed("find . -fprint out", AgentMode::Plan, &s).is_err());

        // sed: stream edit to stdout ok; in-place is a write.
        assert!(check_command_allowed("sed 's/a/b/' f", AgentMode::Plan, &s).is_ok());
        assert!(check_command_allowed("sed -i 's/a/b/' f", AgentMode::Plan, &s).is_err());
        assert!(check_command_allowed("sed -i.bak 's/a/b/' f", AgentMode::Plan, &s).is_err());

        // awk: text processing ok; system() runs a command.
        assert!(check_command_allowed("awk '{print $1}' f", AgentMode::Plan, &s).is_ok());
        assert!(check_command_allowed("awk 'BEGIN{system(\"touch f\")}'", AgentMode::Plan, &s).is_err());

        // Edit mode is unaffected.
        assert!(check_command_allowed("find /tmp -delete", AgentMode::Edit, &s).is_ok());
    }

    #[test]
    fn hard_denylist_applies_even_in_bypass() {
        let s = sec();
        assert!(check_command_allowed("rm -rf /", AgentMode::Bypass, &s).is_err());
        assert!(check_command_allowed("anything goes", AgentMode::Bypass, &s).is_ok());
    }

    #[test]
    fn edit_allows_writes_but_not_catastrophe() {
        let s = sec();
        assert!(check_command_allowed("rm file", AgentMode::Edit, &s).is_ok());
        assert!(check_command_allowed("mkfs.ext4 /dev/sdb", AgentMode::Edit, &s).is_err());
    }

    #[test]
    fn path_traversal_cannot_escape_denylist() {
        let mut s = sec();
        s.denied_paths = vec!["/etc".to_string()];
        assert!(check_path_allowed("/etc/shadow", &s).is_err());
        assert!(check_path_allowed("/var/../etc/passwd", &s).is_err());
        assert!(check_path_allowed("/var/log/syslog", &s).is_ok());
    }

    #[test]
    fn allowlist_restricts() {
        let mut s = sec();
        s.denied_paths.clear();
        s.allowed_paths = vec!["/home/me/project".to_string()];
        assert!(check_path_allowed("/home/me/project/src/main.rs", &s).is_ok());
        assert!(check_path_allowed("/home/me/project/../secret", &s).is_err());
        assert!(check_path_allowed("/etc/hosts", &s).is_err());
    }

    #[test]
    fn size_limit() {
        let mut s = sec();
        s.max_file_size = 100;
        assert!(check_size(50, &s).is_ok());
        assert!(check_size(200, &s).is_err());
        s.max_file_size = 0;
        assert!(check_size(u64::MAX, &s).is_ok());
    }
}
