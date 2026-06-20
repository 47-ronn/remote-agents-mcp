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
        // On timeout the wait future is dropped; tokio defaults to *not* killing
        // the child on drop, which would orphan the `sh -c` leader. Reap it.
        // (`kill_on_drop` only signals the leader — the whole-tree kill below
        // handles backgrounded grandchildren.)
        .kill_on_drop(true);

    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }

    // Set environment to ensure consistent output
    cmd.env("LANG", "en_US.UTF-8");
    cmd.env("LC_ALL", "en_US.UTF-8");

    // Run the child as its own process-group leader so a timeout can SIGKILL the
    // entire tree, not just `sh -c`. Without this a command that backgrounds work
    // (`some_daemon &`) leaks the grandchild: `kill_on_drop` reaps only the
    // leader and the orphan is reparented to init.
    #[cfg(unix)]
    cmd.process_group(0);

    let child = cmd.spawn()?;
    let pid = child.id();
    let duration = Duration::from_millis(timeout_ms);

    let output = match timeout(duration, child.wait_with_output()).await {
        Ok(res) => res?,
        Err(elapsed) => {
            kill_process_group(pid);
            return Err(elapsed.into());
        }
    };

    Ok(ExecResult {
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        exit_code: output.status.code().unwrap_or(-1),
    })
}

/// SIGKILL the whole process group led by `pid` (no-op if the child already
/// exited and so reports no pid). `process_group(0)` made the child its own
/// group leader, so `-pid` targets it and every descendant that didn't start a
/// new group. `kill_on_drop` still reaps the leader; this reaches the rest.
#[cfg(unix)]
pub(crate) fn kill_process_group(pid: Option<u32>) {
    if let Some(pid) = pid {
        // Safe: a bare `kill(2)`, no memory is dereferenced. A negative pid
        // signals the entire process group.
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
}

/// On Windows, use `taskkill /F /T /PID` to forcibly terminate the process tree.
/// `/F` = force, `/T` = tree (kill child processes too).
#[cfg(windows)]
pub(crate) fn kill_process_group(pid: Option<u32>) {
    if let Some(pid) = pid {
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn kill_process_group(_pid: Option<u32>) {}

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

    let mut cmd = Command::new(shell);
    cmd.arg(shell_arg)
        .arg(command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("LANG", "en_US.UTF-8")
        .env("LC_ALL", "en_US.UTF-8")
        // Reap the leader if the timeout drops `wait_with_output` (see `exec`).
        .kill_on_drop(true);
    // Own process group so a timeout kills the whole tree (see `exec`).
    #[cfg(unix)]
    cmd.process_group(0);
    let mut child = cmd.spawn()?;
    let pid = child.id();

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
    let output = match timeout(duration, child.wait_with_output()).await {
        Ok(res) => res?,
        Err(elapsed) => {
            kill_process_group(pid);
            // The writer may be parked on a full stdin pipe; the kill closes it,
            // but abort so we never block on a child that's gone.
            writer.abort();
            return Err(elapsed.into());
        }
    };
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

    /// The tree case: a command that BACKGROUNDS a child keeps `sh` as the
    /// tracked leader, so `kill_on_drop` (which signals only the leader) would
    /// reap `sh` and leave the backgrounded `sleep` reparented to init. The
    /// process-group kill must take the whole tree down. With the group kill
    /// removed this test stays red — the grandchild never goes away.
    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_exec_kills_backgrounded_grandchild() {
        let marker = "sleep 87655";

        // `<marker> & wait`: sh forks the sleep into the background and blocks in
        // `wait`, so the leader is sh and the victim is a *grandchild*.
        let handle =
            tokio::spawn(async move { exec(&format!("{marker} & wait"), None, 1000).await });

        assert!(
            poll_liveness(marker, true, Duration::from_secs(8)).await,
            "the backgrounded child never started"
        );

        let res = handle.await.unwrap();
        assert!(res.is_err(), "exec should have timed out");

        assert!(
            poll_liveness(marker, false, Duration::from_secs(12)).await,
            "timed-out exec leaked a backgrounded grandchild '{marker}'"
        );
    }

    /// Same whole-tree guarantee for the stdin-feeding path.
    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_exec_with_stdin_kills_backgrounded_grandchild() {
        let marker = "sleep 76544";

        let handle = tokio::spawn(async move {
            exec_with_stdin(&format!("{marker} & wait"), "", 1000).await
        });

        assert!(
            poll_liveness(marker, true, Duration::from_secs(8)).await,
            "the backgrounded child never started"
        );

        let res = handle.await.unwrap();
        assert!(res.is_err(), "exec_with_stdin should have timed out");

        assert!(
            poll_liveness(marker, false, Duration::from_secs(12)).await,
            "timed-out exec_with_stdin leaked a backgrounded grandchild '{marker}'"
        );
    }
}
