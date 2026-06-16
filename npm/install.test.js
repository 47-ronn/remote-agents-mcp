// Unit tests for the platform → release-target mapping in install.js.
// Run: node --test npm/   (Node 18+, no dependencies)

const { test } = require("node:test");
const assert = require("node:assert");
const { target, downloadWithRetry } = require("./install.js");

const noWait = () => Promise.resolve();

test("maps every supported platform/arch to a release target", () => {
  assert.deepStrictEqual(target("linux", "x64"), {
    triple: "x86_64-unknown-linux-musl",
    exe: "",
  });
  assert.deepStrictEqual(target("linux", "arm64"), {
    triple: "aarch64-unknown-linux-musl",
    exe: "",
  });
  assert.deepStrictEqual(target("darwin", "x64"), {
    triple: "x86_64-apple-darwin",
    exe: "",
  });
  assert.deepStrictEqual(target("darwin", "arm64"), {
    triple: "aarch64-apple-darwin",
    exe: "",
  });
  assert.deepStrictEqual(target("win32", "x64"), {
    triple: "x86_64-pc-windows-msvc",
    exe: ".exe",
  });
});

test("returns null for unsupported platform/arch", () => {
  assert.strictEqual(target("linux", "ia32"), null); // 32-bit x86
  assert.strictEqual(target("win32", "arm64"), null); // no windows arm build
  assert.strictEqual(target("freebsd", "x64"), null);
});

test("every triple matches a release.yml build target", () => {
  // Mirror of the matrix in .github/workflows/release.yml — keep in sync.
  const released = new Set([
    "x86_64-unknown-linux-musl",
    "aarch64-unknown-linux-musl",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
  ]);
  for (const [p, a] of [
    ["linux", "x64"],
    ["linux", "arm64"],
    ["darwin", "x64"],
    ["darwin", "arm64"],
    ["win32", "x64"],
  ]) {
    const t = target(p, a);
    assert.ok(t && released.has(t.triple), `${p}/${a} → ${t && t.triple} not in release matrix`);
  }
});

test("downloadWithRetry succeeds without retry on first success", async () => {
  let calls = 0;
  const ok = async () => { calls++; };
  await downloadWithRetry("u", "d", 3, ok, noWait);
  assert.strictEqual(calls, 1);
});

test("downloadWithRetry retries transient failures then succeeds", async () => {
  let calls = 0;
  const flaky = async () => { calls++; if (calls < 3) throw new Error("ECONNRESET"); };
  await downloadWithRetry("u", "d", 3, flaky, noWait);
  assert.strictEqual(calls, 3); // failed twice, succeeded on the 3rd
});

test("downloadWithRetry gives up after all attempts and rethrows last error", async () => {
  let calls = 0;
  const always = async () => { calls++; throw new Error("HTTP 500"); };
  await assert.rejects(
    () => downloadWithRetry("u", "d", 3, always, noWait),
    /HTTP 500/
  );
  assert.strictEqual(calls, 3);
});
