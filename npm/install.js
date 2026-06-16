// postinstall: download the prebuilt `remote-agent` binary matching this
// platform from the matching GitHub release (tag v<package version>) and place
// it in ./bin. No external dependencies; follows GitHub's redirect to the CDN.

const fs = require("fs");
const path = require("path");
const https = require("https");
const { version } = require("./package.json");

const REPO = "ObsidianMotorman/tunshell_mcp_agents";

function target(p = process.platform, a = process.arch) {
  if (p === "linux" && a === "x64") return { triple: "x86_64-unknown-linux-musl", exe: "" };
  if (p === "linux" && a === "arm64") return { triple: "aarch64-unknown-linux-musl", exe: "" };
  if (p === "darwin" && a === "x64") return { triple: "x86_64-apple-darwin", exe: "" };
  if (p === "darwin" && a === "arm64") return { triple: "aarch64-apple-darwin", exe: "" };
  if (p === "win32" && a === "x64") return { triple: "x86_64-pc-windows-msvc", exe: ".exe" };
  return null;
}

function download(url, dest, redirects = 0) {
  return new Promise((resolve, reject) => {
    if (redirects > 10) return reject(new Error("too many redirects"));
    https
      .get(url, { headers: { "User-Agent": "remote-agents-installer" } }, (res) => {
        if ([301, 302, 303, 307, 308].includes(res.statusCode)) {
          res.resume();
          return resolve(download(res.headers.location, dest, redirects + 1));
        }
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        }
        const file = fs.createWriteStream(dest);
        res.pipe(file);
        file.on("finish", () => file.close(() => resolve()));
        file.on("error", reject);
      })
      .on("error", reject);
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Retry transient download failures (flaky networks / CDN hiccups) with a small
// linear backoff. `doDownload`/`wait` are injectable for testing.
async function downloadWithRetry(url, dest, attempts = 3, doDownload = download, wait = sleep) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try {
      await doDownload(url, dest);
      return;
    } catch (e) {
      lastErr = e;
      if (i < attempts - 1) {
        console.error(`remote-agents: download attempt ${i + 1} failed (${e.message}); retrying…`);
        await wait(500 * (i + 1));
      }
    }
  }
  throw lastErr;
}

async function main() {
  // Escape hatch for CI / source installs where no matching release exists.
  if (process.env.REMOTE_AGENTS_SKIP_DOWNLOAD) {
    console.log("remote-agents: REMOTE_AGENTS_SKIP_DOWNLOAD set, skipping binary download");
    return;
  }

  const t = target();
  if (!t) {
    console.error(
      `remote-agents: no prebuilt binary for ${process.platform}/${process.arch}.\n` +
        `  Supported: linux x64/arm64, macOS x64/arm64, windows x64.\n` +
        `  Build from source: https://github.com/${REPO}`
    );
    process.exit(1);
  }

  const asset = `remote-agent-${t.triple}${t.exe}`;
  const url = `https://github.com/${REPO}/releases/download/v${version}/${asset}`;
  const binDir = path.join(__dirname, "bin");
  fs.mkdirSync(binDir, { recursive: true });
  const dest = path.join(binDir, `remote-agent${t.exe}`);

  console.log(`remote-agents: downloading ${asset} (v${version})…`);
  try {
    await downloadWithRetry(url, dest);
    if (!t.exe) fs.chmodSync(dest, 0o755);
    console.log(`remote-agents: installed ${dest}`);
  } catch (e) {
    console.error(`remote-agents: failed to download binary: ${e.message}`);
    console.error(`  URL: ${url}`);
    console.error(`  Ensure a release tagged v${version} exists with that asset.`);
    process.exit(1);
  }
}

// Only download when run as the postinstall script, not when require()'d by tests.
if (require.main === module) {
  main();
}

module.exports = { target, downloadWithRetry };
