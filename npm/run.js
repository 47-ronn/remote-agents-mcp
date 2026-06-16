#!/usr/bin/env node
// Launcher: exec the downloaded native `remote-agent` binary, forwarding args,
// stdin/stdout/stderr, termination signals, and exit code.
// `remote-agents <cmd>` == `remote-agent <cmd>`.
//
// On startup of a long-running mode it also does a best-effort npm-registry
// version check and logs a notice if a newer release is published. This is
// NOTIFY-ONLY: the agent never self-updates (updating mid-task would interrupt
// it); the operator runs `npm i -g remote-agents@latest` when convenient.

const path = require("path");
const fs = require("fs");
const os = require("os");
const https = require("https");
const { spawn } = require("child_process");
const { version } = require("./package.json");

const REGISTRY_URL = "https://registry.npmjs.org/remote-agents/latest";
// Only the long-running modes are worth a version check; one-shot subcommands
// (init/config/install/...) start and exit, so a notice there is just noise.
const LONG_RUNNING = new Set(["run", "mcp", "hybrid"]);

// True if dotted version string `latest` is strictly greater than `current`.
// Non-numeric / missing segments are treated as 0 (best-effort, no semver dep).
function isNewer(latest, current) {
  const parse = (s) => String(s).split(".").map((n) => parseInt(n, 10) || 0);
  const a = parse(latest);
  const b = parse(current);
  for (let i = 0; i < Math.max(a.length, b.length); i++) {
    const x = a[i] || 0;
    const y = b[i] || 0;
    if (x > y) return true;
    if (x < y) return false;
  }
  return false;
}

// A human-readable upgrade notice if `latest` is newer than `current`, else null.
function updateNotice(current, latest) {
  if (latest && isNewer(latest, current)) {
    return (
      `remote-agents: a newer version is available (${current} -> ${latest}). ` +
      `Update with: npm i -g remote-agents@latest`
    );
  }
  return null;
}

// Best-effort GET of the registry's `latest` document. Resolves the parsed JSON
// or rejects; callers swallow errors (an update check must never break startup).
function httpGetJson(url, timeoutMs) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      { headers: { "User-Agent": "remote-agents-updatecheck" } },
      (res) => {
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`HTTP ${res.statusCode}`));
        }
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (c) => (data += c));
        res.on("end", () => {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            reject(e);
          }
        });
      }
    );
    req.on("error", reject);
    req.setTimeout(timeoutMs, () => req.destroy(new Error("timeout")));
  });
}

// Resolve the latest published version string, or null on any failure. `doGet`
// is injectable for tests.
function fetchLatestVersion(doGet = httpGetJson, timeoutMs = 3000) {
  return doGet(REGISTRY_URL, timeoutMs)
    .then((json) => (json && typeof json.version === "string" ? json.version : null))
    .catch(() => null);
}

// Platform data directory, mirroring Rust's `dirs::data_dir()` so the native
// agent reads the same cache file: Linux XDG, macOS Application Support,
// Windows Roaming AppData.
function dataDir() {
  if (process.platform === "win32") {
    return process.env.APPDATA || path.join(os.homedir(), "AppData", "Roaming");
  }
  if (process.platform === "darwin") {
    return path.join(os.homedir(), "Library", "Application Support");
  }
  return process.env.XDG_DATA_HOME || path.join(os.homedir(), ".local", "share");
}

// Where the latest-known version is cached for the native agent to read and
// surface as AgentInfo.update_available.
function latestVersionPath() {
  return path.join(dataDir(), "remote-agents", "latest-version");
}

// Best-effort write of the update-available cache for the native agent to read
// (surfaced as AgentInfo.update_available). The launcher owns the comparison —
// it knows the accurate INSTALLED version (the compiled-in Cargo version can lag
// the npm release) — so it writes the newer version when an upgrade exists and
// clears the file (empty) once up to date. Never throws. `writeFile`/`mkdir`
// are injectable for tests.
function cacheUpdateAvailable(
  latest,
  current = version,
  file = latestVersionPath(),
  writeFile = fs.writeFileSync,
  mkdir = fs.mkdirSync
) {
  const body = latest && isNewer(latest, current) ? String(latest) : "";
  try {
    mkdir(path.dirname(file), { recursive: true });
    writeFile(file, body);
  } catch {
    /* cache is best-effort; the notify-only log still fires */
  }
}

// Fire-and-forget: log an upgrade notice for long-running modes. Never throws,
// never blocks the agent. Disable with REMOTE_AGENTS_NO_UPDATE_CHECK=1.
async function maybeNotifyUpdate(subcommand, fetchLatest = fetchLatestVersion) {
  if (process.env.REMOTE_AGENTS_NO_UPDATE_CHECK) return;
  if (!LONG_RUNNING.has(subcommand)) return;
  const latest = await fetchLatest();
  // Cache the result so the native agent can surface it as
  // AgentInfo.update_available (visible in list_agents).
  cacheUpdateAvailable(latest);
  const notice = updateNotice(version, latest);
  if (notice) console.error(notice);
}

function main() {
  const exe = process.platform === "win32" ? ".exe" : "";
  const bin = path.join(__dirname, "bin", `remote-agent${exe}`);

  if (!fs.existsSync(bin)) {
    console.error(
      "remote-agents: native binary not found.\n" +
        "  Reinstall it with: npm install -g remote-agents  (re-runs the download)."
    );
    process.exit(1);
  }

  const child = spawn(bin, process.argv.slice(2), { stdio: "inherit" });

  // Best-effort, non-blocking update check (notify-only).
  maybeNotifyUpdate(process.argv[2]).catch(() => {});

  // Forward termination signals so stopping this launcher also stops the agent
  // (spawnSync used to orphan the native process).
  const SIGNALS = ["SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT"];
  for (const sig of SIGNALS) {
    process.on(sig, () => {
      if (!child.killed) {
        try {
          child.kill(sig);
        } catch {
          /* child already gone */
        }
      }
    });
  }

  child.on("error", (err) => {
    console.error(`remote-agents: failed to launch binary: ${err.message}`);
    process.exit(1);
  });

  child.on("exit", (code, signal) => {
    if (signal) {
      // Mirror the child's signal in our exit status (128 + signum).
      const num = (os.constants.signals && os.constants.signals[signal]) || 15;
      process.exit(128 + num);
    }
    process.exit(code === null ? 1 : code);
  });
}

// Only launch when invoked directly, not when require()'d by tests.
if (require.main === module) {
  main();
}

module.exports = {
  isNewer,
  updateNotice,
  fetchLatestVersion,
  maybeNotifyUpdate,
  cacheUpdateAvailable,
  latestVersionPath,
};
