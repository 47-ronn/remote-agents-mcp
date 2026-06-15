#!/usr/bin/env node
// Launcher: exec the downloaded native `remote-agent` binary, forwarding args,
// stdin/stdout/stderr, and exit code. `remote-agents <cmd>` == `remote-agent <cmd>`.

const path = require("path");
const fs = require("fs");
const { spawnSync } = require("child_process");

const exe = process.platform === "win32" ? ".exe" : "";
const bin = path.join(__dirname, "bin", `remote-agent${exe}`);

if (!fs.existsSync(bin)) {
  console.error(
    "remote-agents: native binary not found.\n" +
      "  Reinstall it with: npm install -g remote-agents  (re-runs the download)."
  );
  process.exit(1);
}

const result = spawnSync(bin, process.argv.slice(2), { stdio: "inherit" });
if (result.error) {
  console.error(`remote-agents: failed to launch binary: ${result.error.message}`);
  process.exit(1);
}
process.exit(result.status === null ? 1 : result.status);
