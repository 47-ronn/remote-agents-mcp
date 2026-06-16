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

## License

MIT
