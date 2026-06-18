// postinstall: download the prebuilt `remote-agent` binary matching this
// platform from the matching GitHub release (tag v<package version>) and place
// it in ./bin. No external dependencies; follows GitHub's redirect to the CDN.

const fs = require("fs");
const path = require("path");
const https = require("https");
const crypto = require("crypto");
const { version } = require("./package.json");

const REPO = "47-ronn/tunshell_mcp_agents";

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

// GET a small text resource into memory (follows redirects). Used for the
// checksum sidecar.
function httpGetText(url, redirects = 0) {
  return new Promise((resolve, reject) => {
    if (redirects > 10) return reject(new Error("too many redirects"));
    https
      .get(url, { headers: { "User-Agent": "remote-agents-installer" } }, (res) => {
        if ([301, 302, 303, 307, 308].includes(res.statusCode)) {
          res.resume();
          return resolve(httpGetText(res.headers.location, redirects + 1));
        }
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        }
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (c) => (body += c));
        res.on("end", () => resolve(body));
      })
      .on("error", reject);
  });
}

// SHA-256 of a file as lowercase hex.
function sha256File(file) {
  return new Promise((resolve, reject) => {
    const h = crypto.createHash("sha256");
    const s = fs.createReadStream(file);
    s.on("data", (d) => h.update(d));
    s.on("end", () => resolve(h.digest("hex")));
    s.on("error", reject);
  });
}

// Verify `dest` against the `<url>.sha256` sidecar published with the release.
// Throws on mismatch (a corrupt/tampered download). If the sidecar is absent
// (older releases) or malformed, skips verification and returns false — so the
// installer stays compatible with releases that predate checksums.
// `fetchText`/`hashFile` are injectable for tests.
async function verifyChecksum(url, dest, fetchText = httpGetText, hashFile = sha256File) {
  let doc;
  try {
    doc = await fetchText(`${url}.sha256`);
  } catch {
    console.warn("remote-agents: no checksum published for this release; skipping verification");
    return false;
  }
  const expected = (doc.trim().split(/\s+/)[0] || "").toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(expected)) {
    console.warn("remote-agents: malformed checksum; skipping verification");
    return false;
  }
  const actual = (await hashFile(dest)).toLowerCase();
  if (actual !== expected) {
    throw new Error(`checksum mismatch (expected ${expected}, got ${actual})`);
  }
  return true;
}

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
  } catch (e) {
    console.error(`remote-agents: failed to download binary: ${e.message}`);
    console.error(`  URL: ${url}`);
    console.error(`  Ensure a release tagged v${version} exists with that asset.`);
    process.exit(1);
  }
  try {
    await verifyChecksum(url, dest);
  } catch (e) {
    try { fs.unlinkSync(dest); } catch {}
    console.error(`remote-agents: ${e.message}; the download was corrupt or tampered.`);
    process.exit(1);
  }
  if (!t.exe) fs.chmodSync(dest, 0o755);
  console.log(`remote-agents: installed ${dest}`);
}

// Only download when run as the postinstall script, not when require()'d by tests.
if (require.main === module) {
  main();
}

module.exports = { target, downloadWithRetry, verifyChecksum, sha256File };
