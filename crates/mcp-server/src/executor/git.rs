//! Git operations, implemented by shelling out to the `git` CLI.
//!
//! Using the CLI (rather than a native libgit2 binding) keeps the dependency
//! tree light and matches whatever git configuration / credentials the host
//! already has set up for the user running the agent.

use anyhow::{bail, Context, Result};
use remote_agents_shared::GitStatus;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

#[cfg(windows)]
use std::sync::OnceLock;
#[cfg(windows)]
use tokio::sync::Mutex;

/// Upper bound on any single `git` invocation. Local ops (status/commit) finish
/// in milliseconds; this only backstops *network* subcommands (`pull`/`push`)
/// that would otherwise stall indefinitely — a wedged TLS/SSH connection, or an
/// SSH host-key prompt with no terminal to answer it. Generous on purpose: a
/// legitimately slow transfer over a thin link must not be cut off, only a true
/// hang. The credential-prompt hang is killed instantly by `GIT_TERMINAL_PROMPT`
/// below; this is the safety net for everything else.
const GIT_TIMEOUT: Duration = Duration::from_secs(300);

/// Non-interactive `ssh` for git, so an SSH `pull`/`push` fails fast instead of
/// stalling until [`GIT_TIMEOUT`]. `BatchMode=yes` refuses every prompt (password,
/// key passphrase, unknown-host confirmation) and errors instead — a headless
/// agent could never answer them anyway. `ConnectTimeout` bounds the TCP connect
/// so an unreachable host doesn't eat the full backstop.
///
/// Pure so the policy is unit-testable: returns `None` when the user already
/// configured `GIT_SSH_COMMAND`, so we never clobber a custom ssh/key setup;
/// otherwise the default to apply. `GIT_TERMINAL_PROMPT=0` already covers the
/// HTTPS credential prompt; this is the SSH counterpart.
fn git_ssh_command(existing: Option<&str>) -> Option<&'static str> {
    match existing {
        Some(s) if !s.trim().is_empty() => None,
        _ => Some("ssh -o BatchMode=yes -o ConnectTimeout=10"),
    }
}

/// Track which paths we've already added to safe.directory to avoid repeating.
#[cfg(windows)]
static SAFE_DIRS_ADDED: OnceLock<Mutex<std::collections::HashSet<String>>> = OnceLock::new();

/// On Windows, detect if `repo` is on a network/mapped drive and auto-add it to
/// git's `safe.directory` config to avoid the "dubious ownership" error. Git
/// refuses to operate on network drives by default because the ownership check
/// is unreliable — but for our headless agent this is a reasonable default.
///
/// This is idempotent: each path is only added once per process lifetime.
#[cfg(windows)]
async fn ensure_safe_directory(repo: &str) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use std::ffi::OsStr;

    // Normalize the path so git config matching works.
    let path = match std::fs::canonicalize(repo) {
        Ok(p) => p,
        Err(_) => return Ok(()), // Path doesn't exist, git will fail anyway
    };
    let path_str = path.to_string_lossy().to_string();

    // Check if this is a network/UNC path or a mapped network drive.
    // UNC paths start with `\\` and mapped drives like `N:\` might be network drives.
    let is_network = path_str.starts_with(r"\\")
        || is_network_drive(&path_str);

    if !is_network {
        return Ok(());
    }

    // Check if we've already added this path.
    let added = SAFE_DIRS_ADDED.get_or_init(|| Mutex::new(std::collections::HashSet::new()));
    {
        let mut guard = added.lock().await;
        if guard.contains(&path_str) {
            return Ok(());
        }
        guard.insert(path_str.clone());
    }

    // Add to safe.directory. Use forward slashes as git prefers.
    let safe_path = path_str.replace('\\', "/");
    tracing::info!("Adding network path to git safe.directory: {}", safe_path);
    
    let mut cmd = Command::new("git");
    cmd.args(["config", "--global", "--add", "safe.directory", &safe_path])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let _ = cmd.spawn()?.wait().await;
    Ok(())
}

/// Check if a drive letter path is a network drive (GetDriveTypeW returns DRIVE_REMOTE).
#[cfg(windows)]
fn is_network_drive(path: &str) -> bool {
    use std::os::windows::ffi::OsStrExt;
    use std::ffi::OsStr;

    // Extract drive letter (e.g., "N:" from "N:\foo\bar")
    if path.len() < 2 || !path.chars().next().map(|c| c.is_ascii_alphabetic()).unwrap_or(false) {
        return false;
    }
    let root: Vec<u16> = OsStr::new(&format!("{}\\", &path[..2]))
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();

    // DRIVE_REMOTE = 4
    // Safety: GetDriveTypeW is a simple Windows API that reads a null-terminated string.
    const DRIVE_REMOTE: u32 = 4;
    unsafe {
        windows_sys::Win32::Storage::FileSystem::GetDriveTypeW(root.as_ptr()) == DRIVE_REMOTE
    }
}

#[cfg(not(windows))]
async fn ensure_safe_directory(_repo: &str) -> Result<()> {
    Ok(())
}

/// Run a git subcommand in `repo` and capture its output.
async fn git(repo: &str, args: &[&str]) -> Result<(String, String, i32)> {
    // On Windows, ensure network paths are in safe.directory.
    ensure_safe_directory(repo).await?;
    let mut cmd = Command::new("git");
    cmd.arg("-C")
        .arg(repo)
        .args(args)
        // Headless agent: there is no terminal, so a private-repo `pull`/`push`
        // would otherwise block *forever* waiting on a username/password prompt.
        // `0` makes git fail fast with a clear error instead of hanging.
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SSH counterpart of the prompt guard — only when the user hasn't set their own.
    if let Some(ssh) = git_ssh_command(std::env::var("GIT_SSH_COMMAND").ok().as_deref()) {
        cmd.env("GIT_SSH_COMMAND", ssh);
    }
    let output = run_with_timeout(cmd, GIT_TIMEOUT).await?;
    Ok((
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
        output.status.code().unwrap_or(-1),
    ))
}

/// Spawn `cmd`, capture its output, and abort it if it outruns `timeout`.
///
/// The child leads its own process group (`process_group(0)`, unix) so a hung
/// network subcommand takes its whole subtree down on timeout — `git pull`/`push`
/// fork `git-remote-https`/`ssh` children that would otherwise reparent to init
/// and leak (same class as the executor's group-kill, iter145–147). On timeout we
/// SIGKILL the group; `kill_on_drop` reaps the leader as the future is dropped.
async fn run_with_timeout(mut cmd: Command, timeout: Duration) -> Result<std::process::Output> {
    #[cfg(unix)]
    cmd.process_group(0);
    cmd.kill_on_drop(true);
    let child = cmd.spawn().context("failed to spawn git")?;
    let pid = child.id();
    match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(res) => res.context("git i/o failed"),
        Err(_elapsed) => {
            // The future (owning `child`) is dropped at end of scope → kill_on_drop
            // SIGKILLs the leader; this reaches the rest of the group.
            crate::executor::shell::kill_process_group(pid);
            bail!("git timed out after {}s", timeout.as_secs());
        }
    }
}

/// Collect a structured status of the repository at `repo`.
pub async fn status(repo: &str) -> Result<GitStatus> {
    // Current branch (or short SHA when detached).
    let (branch_out, _, _) = git(repo, &["rev-parse", "--abbrev-ref", "HEAD"]).await?;
    let branch = branch_out.trim().to_string();

    // Ahead/behind vs upstream, if an upstream is configured.
    let (ab_out, _, ab_code) =
        git(repo, &["rev-list", "--left-right", "--count", "HEAD...@{upstream}"]).await?;
    let (ahead, behind) = if ab_code == 0 {
        let mut parts = ab_out.split_whitespace();
        let a = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let b = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        (a, b)
    } else {
        (0, 0)
    };

    // Porcelain v1 for file states.
    let (porcelain, stderr, code) = git(repo, &["status", "--porcelain"]).await?;
    if code != 0 {
        anyhow::bail!("git status failed: {}", stderr.trim());
    }

    let mut staged = Vec::new();
    let mut modified = Vec::new();
    let mut untracked = Vec::new();

    for line in porcelain.lines() {
        if line.len() < 3 {
            continue;
        }
        let x = line.as_bytes()[0] as char; // index (staged) status
        let y = line.as_bytes()[1] as char; // worktree status
        let path = line[3..].to_string();

        if x == '?' && y == '?' {
            untracked.push(path);
            continue;
        }
        if x != ' ' && x != '?' {
            staged.push(path.clone());
        }
        if y != ' ' && y != '?' {
            modified.push(path);
        }
    }

    let clean = staged.is_empty() && modified.is_empty() && untracked.is_empty();

    Ok(GitStatus {
        branch,
        clean,
        ahead,
        behind,
        staged,
        modified,
        untracked,
    })
}

/// Render combined stdout/stderr and a success flag from a git invocation.
fn combine(stdout: String, stderr: String, code: i32) -> (String, bool) {
    let mut out = stdout;
    if !stderr.trim().is_empty() {
        if !out.is_empty() && !out.ends_with('\n') {
            out.push('\n');
        }
        out.push_str(&stderr);
    }
    (out.trim_end().to_string(), code == 0)
}

/// `git pull <remote> [branch]`.
pub async fn pull(repo: &str, remote: &str, branch: Option<&str>) -> Result<(String, bool)> {
    let mut args = vec!["pull", remote];
    if let Some(b) = branch {
        args.push(b);
    }
    let (o, e, c) = git(repo, &args).await?;
    Ok(combine(o, e, c))
}

/// Stage `files` (or everything when empty) and `git commit -m <message>`.
pub async fn commit(repo: &str, message: &str, files: &[String]) -> Result<(String, bool)> {
    // Stage requested files (or all changes).
    let mut add_args = vec!["add"];
    if files.is_empty() {
        add_args.push("-A");
    } else {
        for f in files {
            add_args.push(f.as_str());
        }
    }
    let (ao, ae, ac) = git(repo, &add_args).await?;
    if ac != 0 {
        return Ok(combine(ao, ae, ac));
    }

    let (o, e, c) = git(repo, &["commit", "-m", message]).await?;
    Ok(combine(o, e, c))
}

/// `git push <remote> [branch]`.
pub async fn push(repo: &str, remote: &str, branch: Option<&str>) -> Result<(String, bool)> {
    let mut args = vec!["push", remote];
    if let Some(b) = branch {
        args.push(b);
    }
    let (o, e, c) = git(repo, &args).await?;
    Ok(combine(o, e, c))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Create a temp dir, `git init` it with a deterministic identity, and
    /// return the dir handle plus its path string. The handle must be kept
    /// alive for the duration of the test (drop removes the directory).
    async fn init_repo() -> (TempDir, String) {
        let dir = TempDir::new().expect("tempdir");
        let repo = dir.path().to_str().unwrap().to_string();

        // `git init` defaults the initial branch name to whatever the host
        // config says; pin it so assertions are deterministic.
        let (_, e, c) = git(&repo, &["init", "-b", "main"]).await.unwrap();
        assert_eq!(c, 0, "git init failed: {e}");
        git(&repo, &["config", "user.email", "test@example.com"])
            .await
            .unwrap();
        git(&repo, &["config", "user.name", "Test"])
            .await
            .unwrap();
        (dir, repo)
    }

    /// A timed-out git invocation must take its whole subtree down, not just the
    /// leader. We run `sh -c "<sleep> & wait"` through the same helper git uses:
    /// the leader is `sh`, the long `sleep` is a backgrounded grandchild standing
    /// in for the `git-remote-https`/`ssh` children a real `pull`/`push` forks.
    /// Without the group-kill, SIGKILLing only `sh` reparents the `sleep` to init
    /// and leaks it. Unique sleep length so a stray from a broken run is found by
    /// pgrep and reaped by PID (not `pkill -f`, which would match this test too).
    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_git_kills_backgrounded_grandchild() {
        let marker = "84517"; // distinctive sleep length, in seconds
        let mut cmd = Command::new("sh");
        cmd.args(["-c", &format!("sleep {marker} & wait")])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let res = run_with_timeout(cmd, Duration::from_millis(300)).await;
        assert!(res.is_err(), "a timed-out command must error");

        // Let the SIGKILL propagate, then assert no `sleep <marker>` survives.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let pgrep = std::process::Command::new("pgrep")
            .args(["-f", &format!("sleep {marker}")])
            .output()
            .expect("pgrep");
        let stdout = String::from_utf8_lossy(&pgrep.stdout);
        let survivors: Vec<&str> = stdout.split_whitespace().collect();
        // Reap any leak by PID before asserting, so a failure can't poison the
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

    #[test]
    fn git_ssh_command_defaults_only_when_user_has_none() {
        // No user setting → our non-interactive default (BatchMode so SSH never
        // hangs on a prompt; ConnectTimeout bounds the connect).
        let def = git_ssh_command(None).expect("a default when unset");
        assert!(def.contains("BatchMode=yes"));
        assert!(def.contains("ConnectTimeout="));
        // Blank/whitespace counts as unset.
        assert!(git_ssh_command(Some("")).is_some());
        assert!(git_ssh_command(Some("   ")).is_some());
        // A real user setting is respected — never clobbered.
        assert_eq!(git_ssh_command(Some("ssh -i ~/.ssh/deploy_key")), None);
    }

    #[test]
    fn combine_appends_stderr_to_stdout() {
        let (out, ok) = combine("done".into(), "warning: x".into(), 0);
        assert_eq!(out, "done\nwarning: x");
        assert!(ok);
    }

    #[test]
    fn combine_reports_failure_and_trims() {
        let (out, ok) = combine("oops\n".into(), String::new(), 1);
        assert_eq!(out, "oops");
        assert!(!ok);
    }

    #[test]
    fn combine_empty_stdout_keeps_stderr_only() {
        let (out, ok) = combine(String::new(), "fatal: boom\n".into(), 128);
        assert_eq!(out, "fatal: boom");
        assert!(!ok);
    }

    #[tokio::test]
    async fn status_reports_untracked_then_clean_after_commit() {
        let (_dir, repo) = init_repo().await;

        // Fresh repo with one untracked file. Note: before the first commit
        // HEAD points at an unborn branch, so `rev-parse --abbrev-ref HEAD`
        // reports "HEAD" rather than the branch name — branch is asserted
        // after the commit below.
        fs::write(format!("{repo}/a.txt"), "hello").unwrap();
        let st = status(&repo).await.unwrap();
        assert!(!st.clean);
        assert_eq!(st.untracked, vec!["a.txt".to_string()]);
        assert!(st.staged.is_empty());
        assert!(st.modified.is_empty());
        // No upstream configured -> ahead/behind default to 0.
        assert_eq!((st.ahead, st.behind), (0, 0));

        // Commit everything; the tree should then be clean and on `main`.
        let (out, ok) = commit(&repo, "initial", &[]).await.unwrap();
        assert!(ok, "commit failed: {out}");
        let st = status(&repo).await.unwrap();
        assert_eq!(st.branch, "main");
        assert!(st.clean, "expected clean tree, got {st:?}");
        assert!(st.untracked.is_empty());
    }

    #[tokio::test]
    async fn status_distinguishes_staged_and_modified() {
        let (_dir, repo) = init_repo().await;

        // Commit an initial version of the file.
        fs::write(format!("{repo}/f.txt"), "v1").unwrap();
        let (out, ok) = commit(&repo, "add f", &[]).await.unwrap();
        assert!(ok, "commit failed: {out}");

        // Stage a brand-new file (index status) ...
        fs::write(format!("{repo}/staged.txt"), "new").unwrap();
        git(&repo, &["add", "staged.txt"]).await.unwrap();
        // ... and modify the committed file without staging (worktree status).
        fs::write(format!("{repo}/f.txt"), "v2").unwrap();

        let st = status(&repo).await.unwrap();
        assert!(!st.clean);
        assert!(st.staged.contains(&"staged.txt".to_string()));
        assert!(st.modified.contains(&"f.txt".to_string()));
    }

    #[tokio::test]
    async fn commit_with_explicit_file_list_stages_only_named_files() {
        let (_dir, repo) = init_repo().await;

        fs::write(format!("{repo}/included.txt"), "in").unwrap();
        fs::write(format!("{repo}/excluded.txt"), "out").unwrap();

        let (out, ok) = commit(&repo, "partial", &["included.txt".to_string()])
            .await
            .unwrap();
        assert!(ok, "commit failed: {out}");

        // Only the named file was committed; the other stays untracked.
        let st = status(&repo).await.unwrap();
        assert_eq!(st.untracked, vec!["excluded.txt".to_string()]);
    }
}
