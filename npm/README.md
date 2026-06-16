# remote-agents

Unified, [MCP](https://modelcontextprotocol.io)-compatible control plane for
fleets of remote machines, driven by AI agents (Claude, opencode).

This npm package is a thin launcher: on install it downloads the prebuilt
`remote-agent` binary for your platform from the matching GitHub release.

## Install

```bash
npm install -g remote-agents
# or run without installing:
npx remote-agents --help
```

Supported platforms: linux x64/arm64, macOS x64/arm64, windows x64.

## Use as an MCP server

```jsonc
{
  "mcpServers": {
    "remote-agents": {
      "command": "remote-agents",
      "args": ["mcp"],
      "env": {
        "REMOTE_AGENTS_RELAY": "wss://<your-relay-host>",
        "REMOTE_AGENTS_ROOM": "myroom",
        "REMOTE_AGENTS_TOKEN": "<secret>"
      }
    }
  }
}
```

`remote-agents <subcommand>` forwards to the native binary (`run`, `mcp`,
`install`, …). See the [project README](https://github.com/ObsidianMotorman/tunshell_mcp_agents#readme)
for the full documentation.

## Update checks

When started in a long-running mode (`run` / `mcp` / `hybrid`), the launcher does
a best-effort check against the npm registry and logs a notice if a newer
version is published. It is **notify-only** — the agent never self-updates, so a
running task is never interrupted. Update at your convenience with:

```bash
npm i -g remote-agents@latest
```

Set `REMOTE_AGENTS_NO_UPDATE_CHECK=1` to disable the check entirely.

## License

MIT
