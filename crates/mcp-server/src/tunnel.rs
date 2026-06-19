//! Cloudflare quick tunnels: expose one of this host's local addresses at a
//! public `*.trycloudflare.com` URL via `cloudflared`, downloading the binary
//! on demand. A dev convenience (`cloudflared tunnel --url http://localhost:N`),
//! gated to edit/bypass mode by the executor.
//!
//! cloudflared keeps running for the life of the tunnel; we track each child in
//! an in-memory registry (mirrors `transfer`/`autonomous`) and reap exited ones.

use anyhow::{bail, Context, Result};
use remote_agents_shared::TunnelInfo;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long to wait for cloudflared to report its public URL before giving up.
const URL_TIMEOUT: Duration = Duration::from_secs(30);

/// In-memory registry of quick tunnels this node started.
#[derive(Default)]
pub struct TunnelStore {
    inner: Mutex<HashMap<String, Handle>>,
}

struct Handle {
    info: TunnelInfo,
    child: Child,
}

impl TunnelStore {
    /// Start a quick tunnel to `target` (a local address or bare port). Downloads
    /// `cloudflared` into `data_dir` if it isn't already available, spawns it,
    /// and returns once the public URL is known. `data_dir` is the node's data
    /// dir (where the binary is cached).
    pub fn start(&self, target: &str, data_dir: Option<PathBuf>) -> Result<TunnelInfo> {
        let target = validate_target(target)?;
        let bin = ensure_cloudflared(data_dir)?;
        let id = short_id();

        // cloudflared logs (incl. the URL) to stderr. Capture to a file, not a
        // pipe, so a full pipe can never block the long-lived child.
        let log = std::env::temp_dir().join(format!("ra-tunnel-{id}.log"));
        let logfile = std::fs::File::create(&log).context("create tunnel log")?;
        let mut command = Command::new(&bin);
        command
            .args(["tunnel", "--no-autoupdate", "--url", &target])
            .stdout(Stdio::null())
            .stderr(logfile);
        // Lead its own process group so we can tear down cloudflared AND any
        // helper children it spawns in one kill (see `kill_reap`).
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let mut child = command
            .spawn()
            .context("spawn cloudflared (is it executable?)")?;

        match wait_for_url(&mut child, &log) {
            Ok(public_url) => {
                let _ = std::fs::remove_file(&log); // captured; child keeps its fd
                let info = TunnelInfo {
                    id,
                    target,
                    public_url,
                    status: "running".into(),
                };
                self.inner
                    .lock()
                    .unwrap()
                    .insert(info.id.clone(), Handle { info: info.clone(), child });
                Ok(info)
            }
            Err(msg) => {
                kill_reap(&mut child);
                let _ = std::fs::remove_file(&log);
                bail!("{msg}");
            }
        }
    }

    /// Running tunnels (reaping any whose process has exited).
    pub fn list(&self) -> Vec<TunnelInfo> {
        let mut g = self.inner.lock().unwrap();
        g.retain(|_, h| !matches!(h.child.try_wait(), Ok(Some(_))));
        g.values().map(|h| h.info.clone()).collect()
    }

    /// Stop a tunnel by id (kills its `cloudflared` process).
    pub fn stop(&self, id: &str) -> Result<()> {
        match self.inner.lock().unwrap().remove(id) {
            Some(mut h) => {
                kill_reap(&mut h.child);
                Ok(())
            }
            None => bail!("no running tunnel with id '{id}'"),
        }
    }

    /// Kill every running tunnel. Called on agent shutdown so `cloudflared`
    /// children don't linger as orphans after the agent exits.
    pub fn shutdown(&self) {
        if let Ok(mut g) = self.inner.lock() {
            for (_, mut h) in g.drain() {
                kill_reap(&mut h.child);
            }
        }
    }
}

/// SIGKILL a child (and, on unix, its whole process group, since cloudflared can
/// spawn helper children) and reap it, so nothing lingers as a zombie/orphan.
fn kill_reap(child: &mut Child) {
    #[cfg(unix)]
    {
        // The child leads its own group (`process_group(0)` at spawn), so its
        // pid == pgid; `kill -<pid>` signals the whole group.
        let pgid = child.id();
        let _ = Command::new("kill")
            .arg("-KILL")
            .arg(format!("-{pgid}"))
            .stderr(Stdio::null())
            .status();
    }
    let _ = child.kill();
    let _ = child.wait();
}

impl Drop for TunnelStore {
    fn drop(&mut self) {
        // Belt-and-suspenders for the graceful path (the registry being dropped):
        // `std::process::Child` does NOT kill on drop, so do it explicitly.
        if let Ok(g) = self.inner.get_mut() {
            for h in g.values_mut() {
                kill_reap(&mut h.child);
            }
        }
    }
}

/// Wait for cloudflared to print its public URL. Returns the URL, or an error
/// message — surfacing an early process exit (with the log tail) right away so a
/// bad binary / edge-connection failure reports the real cause instead of a
/// generic timeout after the full window.
fn wait_for_url(child: &mut Child, log: &Path) -> std::result::Result<String, String> {
    let deadline = Instant::now() + URL_TIMEOUT;
    loop {
        if let Some(url) = std::fs::read_to_string(log).ok().and_then(|s| parse_tunnel_url(&s)) {
            return Ok(url);
        }
        // cloudflared exited before opening a tunnel → report its error now.
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!(
                "cloudflared exited ({status}) before opening a tunnel: {}",
                log_tail(log)
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "cloudflared did not report a tunnel URL within {URL_TIMEOUT:?}"
            ));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The last few log lines (char-safe, bounded) for error context.
fn log_tail(log: &Path) -> String {
    let s = std::fs::read_to_string(log).unwrap_or_default();
    let joined = s
        .lines()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join(" | ");
    let joined = joined.trim();
    // Keep the tail bounded without slicing through a UTF-8 boundary.
    joined
        .chars()
        .rev()
        .take(400)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

/// Extract the `https://<sub>.trycloudflare.com` URL cloudflared prints to its
/// log. Returns the first match, or None if not present yet.
fn parse_tunnel_url(log: &str) -> Option<String> {
    for line in log.lines() {
        let Some(start) = line.find("https://") else {
            continue;
        };
        let tail = &line[start..];
        // The URL ends at the first character that can't appear in it.
        let end = tail
            .find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '/' | ':')))
            .unwrap_or(tail.len());
        let url = &tail[..end];
        if url.contains(".trycloudflare.com") {
            return Some(url.to_string());
        }
    }
    None
}

/// Normalize/validate a tunnel target. Accepts a bare port (→ localhost) or an
/// `http(s)://` URL whose host is EXACTLY this machine's loopback — a dev tool to
/// expose LOCAL services, not arbitrary internal/remote hosts.
fn validate_target(target: &str) -> Result<String> {
    let t = target.trim();

    // Bare port → http://localhost:PORT (validate the range).
    if !t.is_empty() && t.chars().all(|c| c.is_ascii_digit()) {
        return match t.parse::<u32>() {
            Ok(p) if (1..=65535).contains(&p) => Ok(format!("http://localhost:{p}")),
            _ => bail!("tunnel port must be 1–65535, got '{target}'"),
        };
    }

    let lower = t.to_lowercase();
    let is_http = lower.starts_with("http://") || lower.starts_with("https://");
    // Parse the URL host PROPERLY (exact match) so a lookalike like
    // `http://localhost.evil.com` or `http://localhost@evil.com` can't sneak past
    // a substring check and tunnel to a remote host.
    let host_is_loopback = url_host(&lower)
        .map(|h| matches!(h.as_str(), "localhost" | "127.0.0.1" | "::1"))
        .unwrap_or(false);
    if is_http && host_is_loopback {
        Ok(t.to_string())
    } else {
        bail!("tunnel target must be a local address (http(s)://localhost[:port] or a bare port), got '{target}'")
    }
}

/// Extract the host from an `scheme://[userinfo@]host[:port][/path]` URL,
/// lowercased. Handles userinfo and bracketed IPv6 (`[::1]`).
fn url_host(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://")?.1;
    // Authority ends at the first '/', '?' or '#'.
    let authority = after_scheme.split(['/', '?', '#']).next().unwrap_or("");
    // Drop any userinfo before the last '@'.
    let hostport = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    // Bracketed IPv6 `[::1]:port` vs plain `host:port`.
    let host = if let Some(rest) = hostport.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        hostport.split(':').next().unwrap_or("")
    };
    Some(host.to_string())
}

/// Locate `cloudflared`: prefer one on `PATH`, then a cached download, else
/// fetch the right release asset for this platform into the cache.
fn ensure_cloudflared(data_dir: Option<PathBuf>) -> Result<PathBuf> {
    if on_path("cloudflared") {
        return Ok(PathBuf::from("cloudflared"));
    }
    let cache = cache_path(data_dir);
    if cache.is_file() {
        return Ok(cache);
    }
    download_cloudflared(&cache)?;
    Ok(cache)
}

fn on_path(program: &str) -> bool {
    Command::new(program)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn cache_path(data_dir: Option<PathBuf>) -> PathBuf {
    let base = data_dir
        .or_else(dirs::data_dir)
        .unwrap_or_else(|| PathBuf::from("."));
    let name = if cfg!(windows) { "cloudflared.exe" } else { "cloudflared" };
    base.join("remote-agents").join("bin").join(name)
}

/// GitHub release asset name for the current platform, or None if unsupported.
fn cloudflared_asset() -> Option<&'static str> {
    asset_for(std::env::consts::OS, std::env::consts::ARCH)
}

fn asset_for(os: &str, arch: &str) -> Option<&'static str> {
    match (os, arch) {
        ("linux", "x86_64") => Some("cloudflared-linux-amd64"),
        ("linux", "aarch64") => Some("cloudflared-linux-arm64"),
        ("linux", "arm") => Some("cloudflared-linux-arm"),
        ("linux", "x86") => Some("cloudflared-linux-386"),
        ("macos", "x86_64") => Some("cloudflared-darwin-amd64.tgz"),
        ("macos", "aarch64") => Some("cloudflared-darwin-arm64.tgz"),
        ("windows", "x86_64") => Some("cloudflared-windows-amd64.exe"),
        ("windows", "x86") => Some("cloudflared-windows-386.exe"),
        _ => None,
    }
}

fn download_cloudflared(dest: &Path) -> Result<()> {
    let asset = cloudflared_asset().context("no cloudflared build for this OS/arch")?;
    let url = format!(
        "https://github.com/cloudflare/cloudflared/releases/latest/download/{asset}"
    );
    let dir = dest.parent().context("cache path has no parent")?;
    std::fs::create_dir_all(dir).context("create cloudflared cache dir")?;

    if asset.ends_with(".tgz") {
        // macOS ships a tarball containing the `cloudflared` binary.
        let tgz = dir.join("cloudflared.tgz");
        fetch(&url, &tgz)?;
        let status = Command::new("tar")
            .args(["xzf", &tgz.to_string_lossy(), "-C", &dir.to_string_lossy()])
            .status()
            .context("run tar")?;
        let _ = std::fs::remove_file(&tgz);
        if !status.success() {
            bail!("failed to extract cloudflared tarball");
        }
    } else {
        fetch(&url, dest)?;
    }
    make_executable(dest)?;
    Ok(())
}

/// Download `url` to `dest` using whatever HTTP client is available (curl, then
/// wget) — avoids pulling a heavyweight HTTP dependency into the static binary.
fn fetch(url: &str, dest: &Path) -> Result<()> {
    let d = dest.to_string_lossy().to_string();
    let curl = Command::new("curl").args(curl_args(url, &d)).status();
    if matches!(curl, Ok(s) if s.success()) {
        return Ok(());
    }
    let wget = Command::new("wget").args(wget_args(url, &d)).status();
    if matches!(wget, Ok(s) if s.success()) {
        return Ok(());
    }
    bail!("could not download cloudflared (need `curl` or `wget`): {url}")
}

/// `curl` args for fetching `url` → `dest`, time-bounded so a stalled CDN
/// connection can't hang `tunnel start` forever: `--connect-timeout` caps the TCP
/// handshake and `--max-time` the whole transfer. `--max-time` is generous —
/// cloudflared is tens of MB and the link may be thin — so it kills a true hang,
/// not a slow-but-progressing download. `--retry` still rides transient failures.
fn curl_args(url: &str, dest: &str) -> Vec<String> {
    [
        "-fsSL",
        "--retry",
        "3",
        "--connect-timeout",
        "20",
        "--max-time",
        "300",
        "-o",
        dest,
        url,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// `wget` fallback args. Only `-T` (network timeout) is used for the bound: it's
/// understood by both GNU wget (alias of `--timeout`) and BusyBox wget (`-T SEC`),
/// so the fallback stays portable on minimal images where curl is absent.
fn wget_args(url: &str, dest: &str) -> Vec<String> {
    ["-q", "-T", "60", "-O", dest, url]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .context("chmod cloudflared")
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<()> {
    Ok(())
}

/// Short random-ish id from the current time (no extra deps needed for a label).
fn short_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn download_args_are_time_bounded() {
        // curl: connect + overall timeout present, file written to dest, url last.
        let c = curl_args("https://dl/cf", "/tmp/cf");
        assert!(
            c.windows(2).any(|w| w[0] == "--connect-timeout" && w[1] == "20"),
            "curl needs a connect timeout: {c:?}"
        );
        assert!(
            c.windows(2).any(|w| w[0] == "--max-time" && w[1] == "300"),
            "curl needs an overall timeout: {c:?}"
        );
        assert!(c.windows(2).any(|w| w[0] == "-o" && w[1] == "/tmp/cf"));
        assert_eq!(c.last().map(String::as_str), Some("https://dl/cf"));

        // wget: portable `-T` network timeout, dest via -O, url last.
        let w = wget_args("https://dl/cf", "/tmp/cf");
        assert!(
            w.windows(2).any(|x| x[0] == "-T" && x[1] == "60"),
            "wget needs a network timeout: {w:?}"
        );
        assert!(w.windows(2).any(|x| x[0] == "-O" && x[1] == "/tmp/cf"));
        assert_eq!(w.last().map(String::as_str), Some("https://dl/cf"));
    }

    #[test]
    fn asset_mapping_covers_common_platforms() {
        assert_eq!(asset_for("linux", "x86_64"), Some("cloudflared-linux-amd64"));
        assert_eq!(asset_for("linux", "aarch64"), Some("cloudflared-linux-arm64"));
        assert_eq!(asset_for("macos", "aarch64"), Some("cloudflared-darwin-arm64.tgz"));
        assert_eq!(asset_for("windows", "x86_64"), Some("cloudflared-windows-amd64.exe"));
        assert_eq!(asset_for("plan9", "mips"), None);
    }

    #[test]
    fn parses_url_from_cloudflared_box() {
        let log = "\
2026-06-18T10:00:00Z INF +-------------------------------------+
2026-06-18T10:00:00Z INF |  Your quick Tunnel has been created! |
2026-06-18T10:00:00Z INF |  https://happy-tree-cat.trycloudflare.com  |
2026-06-18T10:00:00Z INF +-------------------------------------+";
        assert_eq!(
            parse_tunnel_url(log).as_deref(),
            Some("https://happy-tree-cat.trycloudflare.com")
        );
        // No URL yet → None.
        assert_eq!(parse_tunnel_url("INF starting cloudflared\n"), None);
        // A non-trycloudflare https URL is ignored.
        assert_eq!(parse_tunnel_url("see https://example.com for docs"), None);
    }

    #[cfg(unix)]
    #[test]
    fn wait_for_url_surfaces_early_exit() {
        // cloudflared crashing before it prints a URL → fast, informative error
        // (with the log tail), not a 30s generic timeout.
        let log = std::env::temp_dir().join(format!("ra-tun-exit-{}.log", std::process::id()));
        let logfile = std::fs::File::create(&log).unwrap();
        let mut child = Command::new("sh")
            .args(["-c", "echo 'failed to connect to the edge' >&2; exit 1"])
            .stdout(Stdio::null())
            .stderr(logfile)
            .spawn()
            .expect("spawn sh");
        let res = wait_for_url(&mut child, &log);
        std::fs::remove_file(&log).ok();
        let err = res.expect_err("should error on early exit");
        assert!(err.contains("exited"), "got: {err}");
        assert!(err.contains("failed to connect to the edge"), "tail missing: {err}");
    }

    #[cfg(unix)]
    #[test]
    fn wait_for_url_returns_parsed_url_while_running() {
        let log = std::env::temp_dir().join(format!("ra-tun-url-{}.log", std::process::id()));
        std::fs::write(&log, "INF |  https://abc-def.trycloudflare.com  |\n").unwrap();
        let mut child = Command::new("sleep").arg("5").spawn().expect("spawn sleep");
        let res = wait_for_url(&mut child, &log);
        let _ = child.kill();
        let _ = child.wait();
        std::fs::remove_file(&log).ok();
        assert_eq!(res.unwrap(), "https://abc-def.trycloudflare.com");
    }

    #[cfg(unix)]
    #[test]
    fn shutdown_kills_running_children() {
        // Register a real long-running child, then shut down: it must be killed
        // and the registry emptied (so cloudflared can't orphan on agent exit).
        let store = TunnelStore::default();
        let child = Command::new("sleep").arg("60").spawn().expect("spawn sleep");
        let pid = child.id();
        let info = TunnelInfo {
            id: "t1".into(),
            target: "http://localhost:1".into(),
            public_url: "https://x.trycloudflare.com".into(),
            status: "running".into(),
        };
        store
            .inner
            .lock()
            .unwrap()
            .insert("t1".into(), Handle { info, child });
        assert_eq!(store.list().len(), 1);

        store.shutdown();
        assert!(store.list().is_empty(), "registry cleared");
        // The child process is gone (SIGKILL delivered).
        std::thread::sleep(Duration::from_millis(100));
        let alive = Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(!alive, "child pid {pid} should be dead");
    }

    #[test]
    fn validate_target_normalizes_and_restricts() {
        assert_eq!(validate_target("3000").unwrap(), "http://localhost:3000");
        assert_eq!(
            validate_target("http://localhost:8080").unwrap(),
            "http://localhost:8080"
        );
        assert_eq!(
            validate_target("http://127.0.0.1:5000/app").unwrap(),
            "http://127.0.0.1:5000/app"
        );
        assert!(validate_target("http://[::1]:8080").is_ok());
        // Non-local targets are rejected (no exposing arbitrary internal hosts).
        assert!(validate_target("http://10.0.0.5:80").is_err());
        assert!(validate_target("https://example.com").is_err());
        assert!(validate_target("ftp://localhost:21").is_err());
    }

    #[test]
    fn validate_target_rejects_loopback_lookalikes() {
        // A substring check would have let these tunnel to a REMOTE host.
        assert!(validate_target("http://localhost.evil.com").is_err());
        assert!(validate_target("http://127.0.0.1.evil.com/").is_err());
        assert!(validate_target("http://localhost@evil.com").is_err());
        assert!(validate_target("http://evil.com/?x=localhost").is_err());
        assert!(validate_target("http://evil-localhost.com").is_err());
    }

    #[test]
    fn validate_target_checks_port_range() {
        assert!(validate_target("0").is_err());
        assert!(validate_target("65536").is_err());
        assert!(validate_target("99999").is_err());
        assert_eq!(validate_target("65535").unwrap(), "http://localhost:65535");
        assert_eq!(validate_target("1").unwrap(), "http://localhost:1");
    }

    #[test]
    fn url_host_parses_authority() {
        assert_eq!(url_host("http://localhost:3000/x").as_deref(), Some("localhost"));
        assert_eq!(url_host("http://localhost@evil.com").as_deref(), Some("evil.com"));
        assert_eq!(url_host("http://[::1]:8080/p").as_deref(), Some("::1"));
        assert_eq!(url_host("http://127.0.0.1.evil.com").as_deref(), Some("127.0.0.1.evil.com"));
    }
}
