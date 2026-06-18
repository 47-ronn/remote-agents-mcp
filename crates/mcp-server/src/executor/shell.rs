//! Shell command execution

use anyhow::Result;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

/// Execute a shell command with timeout
pub async fn exec(command: &str, cwd: Option<&str>, timeout_ms: u64) -> Result<ExecResult> {
    let shell = if cfg!(windows) { "cmd" } else { "sh" };
    let shell_arg = if cfg!(windows) { "/C" } else { "-c" };

    let mut cmd = Command::new(shell);
    cmd.arg(shell_arg)
        .arg(command)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // On timeout the `cmd.output()` future is dropped; tokio defaults to
        // *not* killing the child on drop, which would orphan the `sh -c`
        // process (and any tree under it). Reap it instead of leaking.
        .kill_on_drop(true);

    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }

    // Set environment to ensure consistent output
    cmd.env("LANG", "en_US.UTF-8");
    cmd.env("LC_ALL", "en_US.UTF-8");

    let duration = Duration::from_millis(timeout_ms);

    let output = timeout(duration, cmd.output()).await??;

    Ok(ExecResult {
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        exit_code: output.status.code().unwrap_or(-1),
    })
}

/// Execute a shell command, feeding `stdin_data` to its standard input, with a
/// timeout. The stdin write runs concurrently with output collection so a child
/// that streams large output while we are still writing input cannot deadlock
/// on a full pipe.
pub async fn exec_with_stdin(
    command: &str,
    stdin_data: &str,
    timeout_ms: u64,
) -> Result<ExecResult> {
    let shell = if cfg!(windows) { "cmd" } else { "sh" };
    let shell_arg = if cfg!(windows) { "/C" } else { "-c" };

    let mut child = Command::new(shell)
        .arg(shell_arg)
        .arg(command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("LANG", "en_US.UTF-8")
        .env("LC_ALL", "en_US.UTF-8")
        // Reap the child if the timeout drops `wait_with_output` (see `exec`).
        .kill_on_drop(true)
        .spawn()?;

    // Write (and close) stdin from a separate task to avoid pipe deadlock.
    let stdin = child.stdin.take();
    let data = stdin_data.to_string();
    let writer = tokio::spawn(async move {
        if let Some(mut si) = stdin {
            let _ = si.write_all(data.as_bytes()).await;
            let _ = si.shutdown().await; // EOF for the child
        }
    });

    let duration = Duration::from_millis(timeout_ms);
    let output = timeout(duration, child.wait_with_output()).await??;
    let _ = writer.await;

    Ok(ExecResult {
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        exit_code: output.status.code().unwrap_or(-1),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_exec_simple() {
        let result = exec("echo hello", None, 5000).await.unwrap();
        assert!(result.stdout.trim() == "hello");
        assert_eq!(result.exit_code, 0);
    }

    #[tokio::test]
    async fn test_exec_with_cwd() {
        let result = exec("pwd", Some("/tmp"), 5000).await.unwrap();
        assert!(result.stdout.contains("/tmp"));
    }

    #[tokio::test]
    async fn test_exec_error() {
        let result = exec("exit 42", None, 5000).await.unwrap();
        assert_eq!(result.exit_code, 42);
    }

    #[tokio::test]
    async fn test_exec_with_stdin_pipes_data() {
        // Classic map-style transform: uppercase stdin.
        let result = exec_with_stdin("tr a-z A-Z", "hello", 5000).await.unwrap();
        assert_eq!(result.stdout.trim(), "HELLO");
        assert_eq!(result.exit_code, 0);
    }

    #[tokio::test]
    async fn test_exec_with_stdin_large_input_no_deadlock() {
        // 256 KiB through stdin while the child streams it back on stdout.
        let big = "x".repeat(256 * 1024);
        let result = exec_with_stdin("cat", &big, 10_000).await.unwrap();
        assert_eq!(result.stdout.len(), big.len());
        assert_eq!(result.exit_code, 0);
    }

    // A unique sleep duration (integer — BSD `sleep` rejects floats) so a
    // concurrent test's child can't be mistaken for ours by pgrep.
    #[cfg(unix)]
    fn pgrep_alive(marker: &str) -> bool {
        std::process::Command::new("pgrep")
            .arg("-f")
            .arg(marker)
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Poll until a `pgrep`-visible process matching `marker` reaches the wanted
    /// liveness, or the window elapses. Windows are generous on purpose: under a
    /// saturated `cargo test --workspace` the runtime can be starved for seconds,
    /// and a wrongly-reaped orphan stays alive forever (so the no-fix case is
    /// still reliably red — it never flips to "gone").
    #[cfg(unix)]
    async fn poll_liveness(marker: &str, want_alive: bool, max: Duration) -> bool {
        use std::time::Instant;
        let start = Instant::now();
        loop {
            if pgrep_alive(marker) == want_alive {
                return true;
            }
            if start.elapsed() >= max {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// A command that outlives its timeout must be killed, not orphaned. We
    /// `exec` the sleep so the spawned `sh` is *replaced* by it — the child PID
    /// the runtime tracks is the sleep itself, so pgrep sees exactly our victim.
    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_exec_kills_orphan_child() {
        let marker = "sleep 87654";

        // Generous timeout so the child is reliably observable before the kill,
        // even when the runtime is starved under a full workspace test run.
        let handle = tokio::spawn(async move { exec(&format!("exec {marker}"), None, 1000).await });

        assert!(
            poll_liveness(marker, true, Duration::from_secs(8)).await,
            "the sleep child never started"
        );

        let res = handle.await.unwrap();
        assert!(res.is_err(), "exec should have timed out");

        // After the timed-out future is dropped, kill_on_drop must reap the child.
        assert!(
            poll_liveness(marker, false, Duration::from_secs(12)).await,
            "timed-out exec left an orphan '{marker}' running"
        );
    }

    /// Same guarantee for the stdin-feeding path.
    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_exec_with_stdin_kills_orphan_child() {
        let marker = "sleep 76543";

        let handle = tokio::spawn(async move {
            exec_with_stdin(&format!("exec {marker}"), "", 1000).await
        });

        assert!(
            poll_liveness(marker, true, Duration::from_secs(8)).await,
            "the sleep child never started"
        );

        let res = handle.await.unwrap();
        assert!(res.is_err(), "exec_with_stdin should have timed out");

        assert!(
            poll_liveness(marker, false, Duration::from_secs(12)).await,
            "timed-out exec_with_stdin left an orphan '{marker}' running"
        );
    }
}
