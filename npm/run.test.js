// Unit tests for the notify-only update check in run.js.
// Run: node --test npm/   (Node 18+, no dependencies)

const { test } = require("node:test");
const assert = require("node:assert");
const {
  isNewer,
  updateNotice,
  fetchLatestVersion,
  maybeNotifyUpdate,
  cacheUpdateAvailable,
  latestVersionPath,
} = require("./run.js");

test("isNewer compares dotted versions", () => {
  assert.strictEqual(isNewer("0.1.1", "0.1.0"), true);
  assert.strictEqual(isNewer("0.2.0", "0.1.9"), true);
  assert.strictEqual(isNewer("1.0.0", "0.9.9"), true);
  assert.strictEqual(isNewer("0.1.0", "0.1.0"), false); // equal
  assert.strictEqual(isNewer("0.1.0", "0.1.1"), false); // older
  assert.strictEqual(isNewer("0.1", "0.1.0"), false); // missing segment == 0
});

test("updateNotice only fires when a newer version exists", () => {
  assert.ok(updateNotice("0.1.0", "0.1.1").includes("0.1.0 -> 0.1.1"));
  assert.strictEqual(updateNotice("0.1.1", "0.1.1"), null); // same
  assert.strictEqual(updateNotice("0.2.0", "0.1.0"), null); // local is ahead
  assert.strictEqual(updateNotice("0.1.0", null), null); // lookup failed
});

test("fetchLatestVersion returns the registry version string", async () => {
  const get = async () => ({ version: "9.9.9", name: "remote-agents" });
  assert.strictEqual(await fetchLatestVersion(get), "9.9.9");
});

test("fetchLatestVersion swallows network errors and returns null", async () => {
  const get = async () => {
    throw new Error("ENOTFOUND");
  };
  assert.strictEqual(await fetchLatestVersion(get), null);
});

test("fetchLatestVersion returns null on a malformed document", async () => {
  const get = async () => ({ name: "remote-agents" }); // no version field
  assert.strictEqual(await fetchLatestVersion(get), null);
});

test("maybeNotifyUpdate skips one-shot subcommands", async () => {
  let called = false;
  const fetchLatest = async () => {
    called = true;
    return "9.9.9";
  };
  await maybeNotifyUpdate("config", fetchLatest);
  assert.strictEqual(called, false, "non-long-running command must not hit the registry");
});

test("maybeNotifyUpdate checks the registry for long-running modes", async () => {
  let called = false;
  const fetchLatest = async () => {
    called = true;
    return "0.0.0"; // not newer → no log, but the check still runs
  };
  await maybeNotifyUpdate("run", fetchLatest);
  assert.strictEqual(called, true);
});

test("latestVersionPath ends in the shared cache location", () => {
  const p = latestVersionPath();
  assert.ok(p.endsWith(require("path").join("remote-agents", "latest-version")), p);
});

test("cacheUpdateAvailable writes the version when newer", () => {
  let written = null;
  let mkdirCalled = false;
  const writeFile = (file, data) => {
    written = { file, data };
  };
  const mkdir = () => {
    mkdirCalled = true;
  };
  cacheUpdateAvailable("0.1.2", "0.1.0", "/tmp/x/latest-version", writeFile, mkdir);
  assert.strictEqual(mkdirCalled, true);
  assert.strictEqual(written.file, "/tmp/x/latest-version");
  assert.strictEqual(written.data, "0.1.2");
});

test("cacheUpdateAvailable clears the file when up to date", () => {
  // Same or older latest → write empty so a since-updated agent stops flagging.
  for (const latest of ["0.1.0", "0.0.9", null]) {
    let data = "unset";
    cacheUpdateAvailable(latest, "0.1.0", "/tmp/x", (_f, d) => {
      data = d;
    });
    assert.strictEqual(data, "", `latest=${latest} should clear the cache`);
  }
});

test("cacheUpdateAvailable swallows write errors", () => {
  // A failing fs must not throw (cache is best-effort).
  assert.doesNotThrow(() =>
    cacheUpdateAvailable("0.1.2", "0.1.0", "/tmp/x", () => {
      throw new Error("EACCES");
    })
  );
});

test("maybeNotifyUpdate honors REMOTE_AGENTS_NO_UPDATE_CHECK", async () => {
  process.env.REMOTE_AGENTS_NO_UPDATE_CHECK = "1";
  let called = false;
  const fetchLatest = async () => {
    called = true;
    return "9.9.9";
  };
  try {
    await maybeNotifyUpdate("run", fetchLatest);
    assert.strictEqual(called, false, "opt-out env var must disable the check");
  } finally {
    delete process.env.REMOTE_AGENTS_NO_UPDATE_CHECK;
  }
});
