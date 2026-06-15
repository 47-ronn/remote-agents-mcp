#!/usr/bin/env node
// Launcher: exec the downloaded native `remote-agent` binary, forwarding args,
// stdin/stdout/stderr, termination signals, and exit code.
// `remote-agents <cmd>` == `remote-agent <cmd>`.

const path = require("path");
const fs = require("fs");
const os = require("os");
const { spawn } = require("child_process");

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
