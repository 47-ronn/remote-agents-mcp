//! tmux pane tools: persistent interactive terminals on the host.
//!
//! One named window in a dedicated `ra` tmux session per pane. The pane
//! outlives every tool call — an interactive process (an AI CLI, a build
//! watch, a TUI) keeps running detached, and the agent drives it later with
//! PaneSend / reads the screen with PaneRead. This is the "live session"
//! complement to autonomous tasks (which are one-shot headless runs).
//!
//! Everything is a thin wrapper over the tmux CLI (`new-session`,
//! `new-window`, `send-keys`, `capture-pane`, `list-panes`, `kill-window`),
//! executed blocking-side because each tmux call is a short-lived process
//! over a unix socket.

use anyhow::{bail, Context, Result};
use remote_agents_shared::{Command, CommandResult, PaneInfo};
use std::process::Command as StdCommand;

/// All panes we create live in one dedicated session, so they never collide
/// with a user's own tmux sessions.
const SESSION: &str = "ra";

fn tmux(args: &[&str]) -> Result<String> {
    let out = StdCommand::new("tmux")
        .args(args)
        .output()
        .context("failed to run tmux — is it installed? (apt install tmux / brew install tmux)")?;
    if !out.status.success() {
        bail!(
            "tmux {}: {}",
            args.first().unwrap_or(&"<cmd>"),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn tmux_status(args: &[&str]) -> Result<bool> {
    StdCommand::new("tmux")
        .args(args)
        .output()
        .map(|o| o.status.success())
        .context("failed to run tmux — is it installed?")
}

/// Ensure the `ra` session exists (idempotent).
fn ensure_session() -> Result<()> {
    if !tmux_status(&["has-session", "-t", &format!("={SESSION}")])? {
        tmux(&["new-session", "-d", "-s", SESSION])?;
    }
    Ok(())
}

/// Sanitize a pane name into a tmux-safe window name (no `:`/`.` that would
/// confuse target parsing).
fn safe_name(name: &str) -> String {
    name.chars()
        .map(|c| if c == ':' || c == '.' || c.is_whitespace() { '-' } else { c })
        .collect()
}

pub(crate) fn execute(cmd: &Command) -> Result<CommandResult> {
    match cmd {
        Command::PaneStart { name, command, cwd } => {
            let name = safe_name(name);
            if name.is_empty() {
                bail!("pane name must not be empty");
            }
            let target = format!("{SESSION}:{name}");
            let existed = tmux_status(&["has-session", "-t", &format!("={SESSION}")])?
                && tmux(&["list-windows", "-t", &format!("={SESSION}"), "-F", "#{window_name}"])?
                    .lines()
                    .any(|w| w == name);

            if !existed {
                ensure_session()?;
                let sess_next = format!("={SESSION}:");
                let mut args: Vec<&str> = vec![
                    "new-window", "-d", "-t", &sess_next, "-n", &name, "-P", "-F", "#{pane_id}",
                ];
                if let Some(dir) = cwd.as_deref() {
                    args.extend(["-c", dir]);
                }
                tmux(&args)?;
            }

            // Resolve the pane id either way (a re-start of an existing pane
            // still reports its id).
            let pane_id = tmux(&[
                "display", "-p", "-t", &target, "#{pane_id}",
            ])?
            .trim()
            .to_string();

            // Fire the initial command as if typed: keeps a shell in the pane
            // after it exits (the agent can send more later).
            if let Some(cmd) = command.as_deref() {
                if !existed {
                    tmux(&["send-keys", "-t", &target, "-l", cmd])?;
                    tmux(&["send-keys", "-t", &target, "Enter"])?;
                }
            }

            Ok(CommandResult::PaneStarted { target, pane_id, existed })
        }

        Command::PaneSend { target, text, enter, keys } => {
            if !text.is_empty() {
                tmux(&["send-keys", "-t", target, "-l", text])?;
            }
            if let Some(k) = keys.as_deref() {
                for key in k.split(',').map(str::trim).filter(|k| !k.is_empty()) {
                    tmux(&["send-keys", "-t", target, key])?;
                }
            }
            if *enter {
                tmux(&["send-keys", "-t", target, "Enter"])?;
            }
            Ok(CommandResult::Ok)
        }

        Command::PaneRead { target, history } => {
            // `-S -<n>` reaches into scrollback; 0 history = visible screen only.
            let from = if *history == 0 { "-".to_string() } else { format!("-{}", *history) };
            let text = tmux(&[
                "capture-pane", "-p", "-J", "-t", target, "-S", &from,
            ])?;
            Ok(CommandResult::PaneText { text })
        }

        Command::PaneList => {
            let out = tmux(&[
                "list-panes", "-a", "-F",
                "#{session_name}:#{window_name}.#{pane_index}\t#{pane_id}\t#{session_name}\t#{window_name}\t#{pane_current_command}\t#{pane_dead}",
            ])?;
            let panes = out
                .lines()
                .filter_map(|l| {
                    let f: Vec<&str> = l.split('\t').collect();
                    if f.len() < 6 { return None; }
                    Some(PaneInfo {
                        target: f[0].to_string(),
                        pane_id: f[1].to_string(),
                        session: f[2].to_string(),
                        window: f[3].to_string(),
                        command: f[4].to_string(),
                        dead: f[5] == "1",
                    })
                })
                .collect();
            Ok(CommandResult::PaneList { panes })
        }

        Command::PaneKill { target } => {
            // Killing a window in our session kills the pane with it; for
            // targets inside other sessions, kill the pane only.
            tmux(&["kill-pane", "-t", target])?;
            Ok(CommandResult::Ok)
        }

        _ => bail!("not a pane command"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_sanitized_for_tmux_targets() {
        assert_eq!(safe_name("my chat: v2"), "my-chat--v2");
        assert_eq!(safe_name("a.b"), "a-b");
        assert_eq!(safe_name("ok_1-2"), "ok_1-2");
    }

    #[test]
    fn pane_list_parses_tab_format() {
        // One line per pane, 6 tab fields, dead flag 0/1 — mirrors the
        // list-panes -F format string above.
        let line = "ra:build.0\t%3\tra\tbuild\tnode\t0";
        let f: Vec<&str> = line.split('\t').collect();
        let p = PaneInfo {
            target: f[0].to_string(),
            pane_id: f[1].to_string(),
            session: f[2].to_string(),
            window: f[3].to_string(),
            command: f[4].to_string(),
            dead: f[5] == "1",
        };
        assert_eq!(p.target, "ra:build.0");
        assert_eq!(p.pane_id, "%3");
        assert!(!p.dead);
    }
}
