//! Shell command execution

use anyhow::Result;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time::timeout;

pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    /// Wall-clock execution time in milliseconds.
    pub duration_ms: u64,
    /// `true` if the command was killed due to timeout.
    pub timed_out: bool,
}

/// Maximum partial output to capture on timeout (128 KB per stream).
const MAX_PARTIAL_OUTPUT: usize = 128 * 1024;

/// Windows command line length limit (safe threshold to avoid "filename too long").
/// Actual limit is ~8191 for cmd.exe, but we use a conservative 4KB threshold.
#[cfg(windows)]
const MAX_CMD_LINE_LENGTH: usize = 4096;

/// Execute a shell command with timeout. On timeout, returns any partial
/// stdout/stderr captured so far instead of empty strings.
pub async fn exec(command: &str, cwd: Option<&str>, timeout_ms: u64) -> Result<ExecResult> {
    // On Windows, if the command is too long, use a temporary script file to avoid
    // "The filename or extension is too long" error (cmd.exe has ~8KB limit).
    #[cfg(windows)]
    if command.len() > MAX_CMD_LINE_LENGTH {
        return exec_via_tempfile(command, cwd, timeout_ms).await;
    }

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

    let mut child = cmd.spawn()?;
    let pid = child.id();
    let deadline = Duration::from_millis(timeout_ms);
    let start = Instant::now();

    // Take stdout/stderr handles for incremental capture.
    let stdout_handle = child.stdout.take();
    let stderr_handle = child.stderr.take();

    // Shared buffers for partial output capture.
    let stdout_buf = Arc::new(Mutex::new(String::new()));
    let stderr_buf = Arc::new(Mutex::new(String::new()));

    // Spawn tasks to read stdout/stderr line by line.
    let stdout_buf_clone = stdout_buf.clone();
    let stdout_task = tokio::spawn(async move {
        if let Some(stdout) = stdout_handle {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let mut buf = stdout_buf_clone.lock().await;
                if buf.len() < MAX_PARTIAL_OUTPUT {
                    if !buf.is_empty() {
                        buf.push('\n');
                    }
                    buf.push_str(&line);
                }
            }
        }
    });
    let stdout_abort = stdout_task.abort_handle();

    let stderr_buf_clone = stderr_buf.clone();
    let stderr_task = tokio::spawn(async move {
        if let Some(stderr) = stderr_handle {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let mut buf = stderr_buf_clone.lock().await;
                if buf.len() < MAX_PARTIAL_OUTPUT {
                    if !buf.is_empty() {
                        buf.push('\n');
                    }
                    buf.push_str(&line);
                }
            }
        }
    });
    let stderr_abort = stderr_task.abort_handle();

    // Wait for child with timeout, collecting output in parallel.
    let wait_result = timeout(deadline, async {
        let status = child.wait().await;
        // Wait for readers to finish after child exits.
        let _ = stdout_task.await;
        let _ = stderr_task.await;
        status
    })
    .await;

    let elapsed_ms = start.elapsed().as_millis() as u64;

    match wait_result {
        Ok(Ok(status)) => {
            let stdout = stdout_buf.lock().await.clone();
            let stderr = stderr_buf.lock().await.clone();
            Ok(ExecResult {
                stdout,
                stderr,
                exit_code: status.code().unwrap_or(-1),
                duration_ms: elapsed_ms,
                timed_out: false,
            })
        }
        Ok(Err(e)) => Err(e.into()),
        Err(_timeout) => {
            // Kill the process tree first.
            kill_process_group(pid);
            // Abort the reader tasks (they may be blocked on the dead process).
            stdout_abort.abort();
            stderr_abort.abort();
            // Collect whatever partial output we captured.
            let partial_stdout = stdout_buf.lock().await.clone();
            let partial_stderr = stderr_buf.lock().await.clone();
            let mut stderr_out = partial_stderr;
            if !stderr_out.is_empty() {
                stderr_out.push('\n');
            }
            stderr_out.push_str(&format!("[command timed out after {}ms]", elapsed_ms));
            Ok(ExecResult {
                stdout: partial_stdout,
                stderr: stderr_out,
                exit_code: -1,
                duration_ms: elapsed_ms,
                timed_out: true,
            })
        }
    }
}

/// Execute a long command via a temporary batch/script file on Windows.
/// This avoids the "filename or extension is too long" error when the command
/// exceeds cmd.exe's ~8KB argument length limit.
#[cfg(windows)]
async fn exec_via_tempfile(command: &str, cwd: Option<&str>, timeout_ms: u64) -> Result<ExecResult> {
    use std::io::Write;
    
    // Create a temporary batch file
    let temp_dir = std::env::temp_dir();
    let script_name = format!("remote-agent-{}.bat", std::process::id());
    let script_path = temp_dir.join(script_name);
    
    // Write the command to the batch file
    let mut file = std::fs::File::create(&script_path)?;
    writeln!(file, "@echo off")?;
    writeln!(file, "{}", command)?;
    drop(file);
    
    // Execute the batch file
    let script_path_str = script_path.to_string_lossy().to_string();
    let mut cmd = Command::new("cmd");
    cmd.arg("/C")
        .arg(&script_path_str)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    
    cmd.env("LANG", "en_US.UTF-8");
    cmd.env("LC_ALL", "en_US.UTF-8");
    
    let mut child = cmd.spawn()?;
    let pid = child.id();
    let deadline = Duration::from_millis(timeout_ms);
    let start = Instant::now();
    
    let stdout_handle = child.stdout.take();
    let stderr_handle = child.stderr.take();
    
    let stdout_buf = Arc::new(Mutex::new(String::new()));
    let stderr_buf = Arc::new(Mutex::new(String::new()));
    
    let stdout_buf_clone = stdout_buf.clone();
    let stdout_task = tokio::spawn(async move {
        if let Some(stdout) = stdout_handle {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let mut buf = stdout_buf_clone.lock().await;
                if buf.len() < MAX_PARTIAL_OUTPUT {
                    if !buf.is_empty() {
                        buf.push('\n');
                    }
                    buf.push_str(&line);
                }
            }
        }
    });
    let stdout_abort = stdout_task.abort_handle();
    
    let stderr_buf_clone = stderr_buf.clone();
    let stderr_task = tokio::spawn(async move {
        if let Some(stderr) = stderr_handle {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let mut buf = stderr_buf_clone.lock().await;
                if buf.len() < MAX_PARTIAL_OUTPUT {
                    if !buf.is_empty() {
                        buf.push('\n');
                    }
                    buf.push_str(&line);
                }
            }
        }
    });
    let stderr_abort = stderr_task.abort_handle();
    
    let wait_result = timeout(deadline, async {
        let status = child.wait().await;
        let _ = stdout_task.await;
        let _ = stderr_task.await;
        status
    })
    .await;
    
    let elapsed_ms = start.elapsed().as_millis() as u64;
    
    // Clean up the temporary file
    let _ = std::fs::remove_file(&script_path);
    
    match wait_result {
        Ok(Ok(status)) => {
            let stdout = stdout_buf.lock().await.clone();
            let stderr = stderr_buf.lock().await.clone();
            Ok(ExecResult {
                stdout,
                stderr,
                exit_code: status.code().unwrap_or(-1),
                duration_ms: elapsed_ms,
                timed_out: false,
            })
        }
        Ok(Err(e)) => Err(e.into()),
        Err(_elapsed) => {
            kill_process_group(pid);
            stdout_abort.abort();
            stderr_abort.abort();
            let partial_stdout = stdout_buf.lock().await.clone();
            let partial_stderr = stderr_buf.lock().await.clone();
            let mut stderr_out = partial_stderr;
            if !stderr_out.is_empty() {
                stderr_out.push('\n');
            }
            stderr_out.push_str(&format!("[command timed out after {}ms]", elapsed_ms));
            Ok(ExecResult {
                stdout: partial_stdout,
                stderr: stderr_out,
                exit_code: -1,
                duration_ms: elapsed_ms,
                timed_out: true,
            })
        }
    }
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
/// on a full pipe. On timeout, returns any partial stdout/stderr captured so far.
pub async fn exec_with_stdin(
    command: &str,
    stdin_data: &str,
    timeout_ms: u64,
) -> Result<ExecResult> {
    // On Windows, use tempfile for long commands to avoid argument length limits
    #[cfg(windows)]
    if command.len() > MAX_CMD_LINE_LENGTH {
        return exec_with_stdin_via_tempfile(command, stdin_data, timeout_ms).await;
    }

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
    let stdout_handle = child.stdout.take();
    let stderr_handle = child.stderr.take();
    let data = stdin_data.to_string();
    let writer = tokio::spawn(async move {
        if let Some(mut si) = stdin {
            let _ = si.write_all(data.as_bytes()).await;
            let _ = si.shutdown().await; // EOF for the child
        }
    });
    let writer_abort = writer.abort_handle();

    // Shared buffers for partial output capture.
    let stdout_buf = Arc::new(Mutex::new(String::new()));
    let stderr_buf = Arc::new(Mutex::new(String::new()));

    // Spawn tasks to read stdout/stderr line by line.
    let stdout_buf_clone = stdout_buf.clone();
    let stdout_task = tokio::spawn(async move {
        if let Some(stdout) = stdout_handle {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let mut buf = stdout_buf_clone.lock().await;
                if buf.len() < MAX_PARTIAL_OUTPUT {
                    if !buf.is_empty() {
                        buf.push('\n');
                    }
                    buf.push_str(&line);
                }
            }
        }
    });
    let stdout_abort = stdout_task.abort_handle();

    let stderr_buf_clone = stderr_buf.clone();
    let stderr_task = tokio::spawn(async move {
        if let Some(stderr) = stderr_handle {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let mut buf = stderr_buf_clone.lock().await;
                if buf.len() < MAX_PARTIAL_OUTPUT {
                    if !buf.is_empty() {
                        buf.push('\n');
                    }
                    buf.push_str(&line);
                }
            }
        }
    });
    let stderr_abort = stderr_task.abort_handle();

    let deadline = Duration::from_millis(timeout_ms);
    let start = Instant::now();

    // Wait for child with timeout, collecting output in parallel.
    let wait_result = timeout(deadline, async {
        let status = child.wait().await;
        // Wait for readers to finish after child exits.
        let _ = stdout_task.await;
        let _ = stderr_task.await;
        let _ = writer.await;
        status
    })
    .await;

    let elapsed_ms = start.elapsed().as_millis() as u64;

    match wait_result {
        Ok(Ok(status)) => {
            let stdout = stdout_buf.lock().await.clone();
            let stderr = stderr_buf.lock().await.clone();
            Ok(ExecResult {
                stdout,
                stderr,
                exit_code: status.code().unwrap_or(-1),
                duration_ms: elapsed_ms,
                timed_out: false,
            })
        }
        Ok(Err(e)) => Err(e.into()),
        Err(_timeout) => {
            // Kill the process tree first.
            kill_process_group(pid);
            // Abort the reader/writer tasks (they may be blocked).
            stdout_abort.abort();
            stderr_abort.abort();
            writer_abort.abort();
            // Collect whatever partial output we captured.
            let partial_stdout = stdout_buf.lock().await.clone();
            let partial_stderr = stderr_buf.lock().await.clone();
            let mut stderr_out = partial_stderr;
            if !stderr_out.is_empty() {
                stderr_out.push('\n');
            }
            stderr_out.push_str(&format!("[command timed out after {}ms]", elapsed_ms));
            Ok(ExecResult {
                stdout: partial_stdout,
                stderr: stderr_out,
                exit_code: -1,
                duration_ms: elapsed_ms,
                timed_out: true,
            })
        }
    }
}

/// Execute a command with stdin via tempfile (Windows long command workaround).
#[cfg(windows)]
async fn exec_with_stdin_via_tempfile(
    command: &str,
    stdin_data: &str,
    timeout_ms: u64,
) -> Result<ExecResult> {
    use std::io::Write;
    
    let temp_dir = std::env::temp_dir();
    let script_name = format!("remote-agent-stdin-{}.bat", std::process::id());
    let script_path = temp_dir.join(script_name);
    
    let mut file = std::fs::File::create(&script_path)?;
    writeln!(file, "@echo off")?;
    writeln!(file, "{}", command)?;
    drop(file);
    
    let script_path_str = script_path.to_string_lossy().to_string();
    let mut cmd = Command::new("cmd");
    cmd.arg("/C")
        .arg(&script_path_str)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("LANG", "en_US.UTF-8")
        .env("LC_ALL", "en_US.UTF-8")
        .kill_on_drop(true);
    
    let mut child = cmd.spawn()?;
    let pid = child.id();
    
    let stdin = child.stdin.take();
    let stdout_handle = child.stdout.take();
    let stderr_handle = child.stderr.take();
    let data = stdin_data.to_string();
    let writer = tokio::spawn(async move {
        if let Some(mut si) = stdin {
            let _ = si.write_all(data.as_bytes()).await;
        }
    });
    let writer_abort = writer.abort_handle();
    
    let stdout_buf = Arc::new(Mutex::new(String::new()));
    let stderr_buf = Arc::new(Mutex::new(String::new()));
    
    let stdout_buf_clone = stdout_buf.clone();
    let stdout_task = tokio::spawn(async move {
        if let Some(stdout) = stdout_handle {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let mut buf = stdout_buf_clone.lock().await;
                if buf.len() < MAX_PARTIAL_OUTPUT {
                    if !buf.is_empty() {
                        buf.push('\n');
                    }
                    buf.push_str(&line);
                }
            }
        }
    });
    let stdout_abort = stdout_task.abort_handle();
    
    let stderr_buf_clone = stderr_buf.clone();
    let stderr_task = tokio::spawn(async move {
        if let Some(stderr) = stderr_handle {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let mut buf = stderr_buf_clone.lock().await;
                if buf.len() < MAX_PARTIAL_OUTPUT {
                    if !buf.is_empty() {
                        buf.push('\n');
                    }
                    buf.push_str(&line);
                }
            }
        }
    });
    let stderr_abort = stderr_task.abort_handle();
    
    let deadline = Duration::from_millis(timeout_ms);
    let start = Instant::now();
    
    let wait_result = timeout(deadline, async {
        let status = child.wait().await;
        let _ = writer.await;
        let _ = stdout_task.await;
        let _ = stderr_task.await;
        status
    })
    .await;
    
    let elapsed_ms = start.elapsed().as_millis() as u64;
    let _ = std::fs::remove_file(&script_path);
    
    match wait_result {
        Ok(Ok(status)) => {
            let stdout = stdout_buf.lock().await.clone();
            let stderr = stderr_buf.lock().await.clone();
            Ok(ExecResult {
                stdout,
                stderr,
                exit_code: status.code().unwrap_or(-1),
                duration_ms: elapsed_ms,
                timed_out: false,
            })
        }
        Ok(Err(e)) => Err(e.into()),
        Err(_elapsed) => {
            kill_process_group(pid);
            stdout_abort.abort();
            stderr_abort.abort();
            writer_abort.abort();
            let partial_stdout = stdout_buf.lock().await.clone();
            let partial_stderr = stderr_buf.lock().await.clone();
            let mut stderr_out = partial_stderr;
            if !stderr_out.is_empty() {
                stderr_out.push('\n');
            }
            stderr_out.push_str(&format!("[command timed out after {}ms]", elapsed_ms));
            Ok(ExecResult {
                stdout: partial_stdout,
                stderr: stderr_out,
                exit_code: -1,
                duration_ms: elapsed_ms,
                timed_out: true,
            })
        }
    }
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
        #[cfg(unix)]
        {
            let result = exec("pwd", Some("/tmp"), 5000).await.unwrap();
            assert!(result.stdout.contains("/tmp"));
        }
        #[cfg(windows)]
        {
            // Use cd on Windows to show current directory
            let result = exec("cd", Some("C:\\Windows"), 5000).await.unwrap();
            assert!(result.stdout.contains("Windows"));
        }
    }

    #[tokio::test]
    async fn test_exec_error() {
        let result = exec("exit 42", None, 5000).await.unwrap();
        assert_eq!(result.exit_code, 42);
    }

    #[tokio::test]
    async fn test_exec_with_stdin_pipes_data() {
        #[cfg(unix)]
        {
            // Classic map-style transform: uppercase stdin.
            let result = exec_with_stdin("tr a-z A-Z", "hello", 5000).await.unwrap();
            assert_eq!(result.stdout.trim(), "HELLO");
            assert_eq!(result.exit_code, 0);
        }
        #[cfg(windows)]
        {
            // Windows: use PowerShell to uppercase
            let result = exec_with_stdin("findstr .*", "HELLO", 5000).await.unwrap();
            assert_eq!(result.stdout.trim(), "HELLO");
            assert_eq!(result.exit_code, 0);
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_exec_with_stdin_large_input_no_deadlock() {
        // 256 KiB through stdin while the child streams it back on stdout.
        // This test verifies that we don't deadlock when both stdin and stdout
        // are full. Unix 'cat' is reliable for this. Windows commands like 'more'
        // and 'sort' have different buffering behavior that makes this test flaky.
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

        let res = handle.await.unwrap().unwrap();
        assert!(res.timed_out, "exec should have timed out");

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

        let res = handle.await.unwrap().unwrap();
        assert!(res.timed_out, "exec_with_stdin should have timed out");

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

        let res = handle.await.unwrap().unwrap();
        assert!(res.timed_out, "exec should have timed out");

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

        let res = handle.await.unwrap().unwrap();
        assert!(res.timed_out, "exec_with_stdin should have timed out");

        assert!(
            poll_liveness(marker, false, Duration::from_secs(12)).await,
            "timed-out exec_with_stdin leaked a backgrounded grandchild '{marker}'"
        );
    }

    /// On timeout, any partial output captured before the kill is preserved.
    /// This tests that a command printing output before hanging returns that
    /// output even after being terminated.
    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_exec_captures_partial_output() {
        // Print some lines, then hang forever.
        let result = exec(
            "echo 'line 1'; echo 'line 2'; sleep 999",
            None,
            500, // short timeout
        )
        .await
        .unwrap();

        assert!(result.timed_out, "expected timeout");
        assert!(
            result.stdout.contains("line 1"),
            "partial stdout missing 'line 1', got: {:?}",
            result.stdout
        );
        assert!(
            result.stdout.contains("line 2"),
            "partial stdout missing 'line 2', got: {:?}",
            result.stdout
        );
        assert!(
            result.stderr.contains("timed out"),
            "stderr should mention timeout, got: {:?}",
            result.stderr
        );
    }

    /// Telemetry: duration_ms is populated even on success.
    #[tokio::test]
    async fn exec_populates_duration_ms() {
        let result = exec("echo quick", None, 5000).await.unwrap();
        assert!(!result.timed_out);
        // Duration should be non-zero (command took at least some time).
        // Just check it's reasonable — less than 5 seconds.
        assert!(result.duration_ms < 5000, "duration_ms too large: {}", result.duration_ms);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn long_command_uses_tempfile_automatically() {
        // Create a command longer than MAX_CMD_LINE_LENGTH (4096 bytes)
        // by echoing a long string. This should trigger the tempfile path.
        let long_str = "X".repeat(5000);
        let command = format!("echo {}", long_str);
        
        let result = exec(&command, None, 5000).await.unwrap();
        assert_eq!(result.exit_code, 0);
        // The output should contain our repeated X's
        assert!(result.stdout.contains("XXXX"));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn long_command_with_stdin_uses_tempfile() {
        // Create a long command that echoes stdin
        let long_comment = format!("REM {}", "Y".repeat(5000));
        let command = format!("{}\nfindstr .*", long_comment);
        
        let result = exec_with_stdin(&command, "HELLO", 5000).await.unwrap();
        // Command should execute without "filename too long" error
        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.contains("HELLO"));
    }
}
