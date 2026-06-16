// Unit tests for the notify-only update check in run.js.
// Run: node --test npm/   (Node 18+, no dependencies)

const { test } = require("node:test");
const assert = require("node:assert");
const { isNewer, updateNotice, fetchLatestVersion, maybeNotifyUpdate } = require("./run.js");

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
