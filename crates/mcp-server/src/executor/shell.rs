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
        .stderr(Stdio::piped());

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
}
