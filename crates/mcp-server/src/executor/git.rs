//! Git operations, implemented by shelling out to the `git` CLI.
//!
//! Using the CLI (rather than a native libgit2 binding) keeps the dependency
//! tree light and matches whatever git configuration / credentials the host
//! already has set up for the user running the agent.

use anyhow::{Context, Result};
use remote_agents_shared::GitStatus;
use std::process::Stdio;
use tokio::process::Command;

/// Run a git subcommand in `repo` and capture its output.
async fn git(repo: &str, args: &[&str]) -> Result<(String, String, i32)> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("failed to spawn git")?;

    Ok((
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
        output.status.code().unwrap_or(-1),
    ))
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
