//! Service installation / removal for running the agent as a background daemon.
//!
//! Platform support:
//!   * Linux  — systemd **user** service (`~/.config/systemd/user`)
//!   * macOS  — launchd LaunchAgent (`~/Library/LaunchAgents`)
//!   * Windows — generates an NSSM command (printed for the user to run)
//!
//! The generated service simply invokes `remote-agent run`, which loads the
//! agent config file; any extra args passed to `install` are baked into the
//! service's command line.

use anyhow::{bail, Context, Result};
use std::path::PathBuf;
use std::process::Command;

const SERVICE_NAME: &str = "remote-agent";
const LAUNCHD_LABEL: &str = "com.anthropic.remote-agent";

/// Install the agent as an auto-starting service for the current user.
pub fn install(extra_args: &[String]) -> Result<()> {
    let exe = std::env::current_exe().context("cannot determine current executable path")?;
    let exe = exe.to_string_lossy().to_string();

    match std::env::consts::OS {
        "linux" => install_systemd(&exe, extra_args),
        "macos" => install_launchd(&exe, extra_args),
        "windows" => install_windows(&exe, extra_args),
        other => bail!("service install not supported on '{}'", other),
    }
}

/// Remove the previously installed service.
pub fn uninstall() -> Result<()> {
    match std::env::consts::OS {
        "linux" => uninstall_systemd(),
        "macos" => uninstall_launchd(),
        "windows" => uninstall_windows(),
        other => bail!("service uninstall not supported on '{}'", other),
    }
}

fn exec_start(exe: &str, extra_args: &[String]) -> String {
    if extra_args.is_empty() {
        format!("{} run", exe)
    } else {
        format!("{} run {}", exe, extra_args.join(" "))
    }
}

// ---------------------------------------------------------------------------
// Linux / systemd (user scope)
// ---------------------------------------------------------------------------

fn systemd_unit_path() -> Result<PathBuf> {
    let dir = dirs::config_dir()
        .context("no config dir")?
        .join("systemd")
        .join("user");
    Ok(dir.join(format!("{}.service", SERVICE_NAME)))
}

/// Build the systemd user-unit file content (pure; no I/O).
fn systemd_unit(exe: &str, extra_args: &[String]) -> String {
    format!(
        "[Unit]\n\
         Description=Remote Agent daemon\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={exec}\n\
         Restart=always\n\
         RestartSec=5\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        exec = exec_start(exe, extra_args)
    )
}

fn install_systemd(exe: &str, extra_args: &[String]) -> Result<()> {
    let unit = systemd_unit(exe, extra_args);

    let path = systemd_unit_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, unit).with_context(|| format!("writing {:?}", path))?;
    println!("Wrote systemd unit: {}", path.display());

    run("systemctl", &["--user", "daemon-reload"])?;
    run("systemctl", &["--user", "enable", "--now", SERVICE_NAME])?;
    println!("Service '{}' enabled and started.", SERVICE_NAME);
    println!("  Status: systemctl --user status {}", SERVICE_NAME);
    println!("  Logs:   journalctl --user -u {} -f", SERVICE_NAME);
    Ok(())
}

fn uninstall_systemd() -> Result<()> {
    let _ = run("systemctl", &["--user", "disable", "--now", SERVICE_NAME]);
    let path = systemd_unit_path()?;
    if path.exists() {
        std::fs::remove_file(&path)?;
        println!("Removed {}", path.display());
    }
    let _ = run("systemctl", &["--user", "daemon-reload"]);
    println!("Service '{}' uninstalled.", SERVICE_NAME);
    Ok(())
}

// ---------------------------------------------------------------------------
// macOS / launchd
// ---------------------------------------------------------------------------

fn launchd_plist_path() -> Result<PathBuf> {
    let home = dirs::home_dir().context("no home dir")?;
    Ok(home
        .join("Library")
        .join("LaunchAgents")
        .join(format!("{}.plist", LAUNCHD_LABEL)))
}

/// Build the launchd LaunchAgent plist content (pure; no I/O).
fn launchd_plist(exe: &str, extra_args: &[String]) -> String {
    let mut program_args = String::new();
    program_args.push_str(&format!("    <string>{}</string>\n", exe));
    program_args.push_str("    <string>run</string>\n");
    for arg in extra_args {
        program_args.push_str(&format!("    <string>{}</string>\n", arg));
    }

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n  \
           <key>Label</key>\n  <string>{label}</string>\n  \
           <key>ProgramArguments</key>\n  <array>\n{args}  </array>\n  \
           <key>RunAtLoad</key>\n  <true/>\n  \
           <key>KeepAlive</key>\n  <true/>\n\
         </dict>\n\
         </plist>\n",
        label = LAUNCHD_LABEL,
        args = program_args
    )
}

fn install_launchd(exe: &str, extra_args: &[String]) -> Result<()> {
    let plist = launchd_plist(exe, extra_args);

    let path = launchd_plist_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, plist).with_context(|| format!("writing {:?}", path))?;
    println!("Wrote launchd plist: {}", path.display());

    // Reload if already loaded, then load.
    let _ = run("launchctl", &["unload", &path.to_string_lossy()]);
    run("launchctl", &["load", &path.to_string_lossy()])?;
    println!("LaunchAgent '{}' loaded.", LAUNCHD_LABEL);
    Ok(())
}

fn uninstall_launchd() -> Result<()> {
    let path = launchd_plist_path()?;
    if path.exists() {
        let _ = run("launchctl", &["unload", &path.to_string_lossy()]);
        std::fs::remove_file(&path)?;
        println!("Removed {}", path.display());
    }
    println!("LaunchAgent '{}' uninstalled.", LAUNCHD_LABEL);
    Ok(())
}

// ---------------------------------------------------------------------------
// Windows / NSSM
// ---------------------------------------------------------------------------

fn install_windows(exe: &str, extra_args: &[String]) -> Result<()> {
    let args = exec_start(exe, extra_args);
    // We can't assume NSSM is installed; print the commands for the user.
    println!("Windows service install (requires NSSM: https://nssm.cc):");
    println!("  nssm install {} \"{}\"", SERVICE_NAME, exe);
    println!("  nssm set {} AppParameters \"run {}\"", SERVICE_NAME, extra_args.join(" "));
    println!("  nssm set {} Start SERVICE_AUTO_START", SERVICE_NAME);
    println!("  nssm start {}", SERVICE_NAME);
    println!("\nFull command line: {}", args);
    Ok(())
}

fn uninstall_windows() -> Result<()> {
    println!("Windows service uninstall:");
    println!("  nssm stop {}", SERVICE_NAME);
    println!("  nssm remove {} confirm", SERVICE_NAME);
    Ok(())
}

// ---------------------------------------------------------------------------

fn run(program: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(program)
        .args(args)
        .status()
        .with_context(|| format!("failed to run {} {:?}", program, args))?;
    if !status.success() {
        bail!("{} {:?} exited with {}", program, args, status);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn exec_start_with_and_without_args() {
        assert_eq!(exec_start("/bin/remote-agent", &[]), "/bin/remote-agent run");
        assert_eq!(
            exec_start("/bin/remote-agent", &args(&["--room", "dev"])),
            "/bin/remote-agent run --room dev"
        );
    }

    #[test]
    fn systemd_unit_has_execstart_and_restart_policy() {
        let unit = systemd_unit("/usr/bin/remote-agent", &args(&["--room", "gpu", "--token", "t"]));
        assert!(unit.contains("ExecStart=/usr/bin/remote-agent run --room gpu --token t"));
        assert!(unit.contains("Restart=always"));
        assert!(unit.contains("[Install]"));
        assert!(unit.contains("WantedBy=default.target"));
    }

    #[test]
    fn launchd_plist_lists_each_arg_and_keepalive() {
        let plist = launchd_plist("/opt/remote-agent", &args(&["--room", "gpu"]));
        assert!(plist.contains(&format!("<string>{}</string>", LAUNCHD_LABEL)));
        assert!(plist.contains("<string>/opt/remote-agent</string>"));
        assert!(plist.contains("<string>run</string>"));
        assert!(plist.contains("<string>--room</string>"));
        assert!(plist.contains("<string>gpu</string>"));
        assert!(plist.contains("<key>RunAtLoad</key>"));
        assert!(plist.contains("<key>KeepAlive</key>"));
        // Well-formed plist envelope.
        assert!(plist.starts_with("<?xml"));
        assert!(plist.trim_end().ends_with("</plist>"));
    }
}
