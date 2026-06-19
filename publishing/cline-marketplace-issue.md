# Cline MCP Marketplace submission

Open a new issue at: https://github.com/cline/mcp-marketplace/issues
(Use the "Add MCP Server" issue template.)

## Required
- **GitHub Repo URL:** https://github.com/47-ronn/tunshell_mcp_agents
- **Logo:** 400×400 PNG (attach to the issue). Put the same file at
  `assets/logo.png` in the repo so other catalogs can reuse it.
- **Why it should be added (value to Cline users):**

  remote-agents turns Cline into a controller for a fleet of remote machines.
  From inside the editor you can run shell commands, transfer files, drive git,
  and fan a single instruction out across many hosts (map/reduce), all over an
  end-to-end-encrypted relay the agents dial outbound — no inbound SSH ports.
  It's a single prebuilt binary installed via `npx remote-agents`, so Cline can
  set it up autonomously from the README with no build step.

- **Setup test confirmation:** "I gave Cline the README.md and it completed the
  MCP server setup autonomously." (Verify this before submitting — install with
  the config below and confirm the tools load.)

## Cline install config (stdio)
```json
{
  "mcpServers": {
    "remote-agents": {
      "command": "npx",
      "args": ["-y", "remote-agents", "mcp", "--relay", "wss://<your-relay-host>", "--room", "<room>", "--token", "<secret>"]
    }
  }
}
```

## Optional but recommended
- Add an `llms-install.md` at repo root walking an AI agent through: choosing a
  relay (CF Worker or self-hosted), picking a room/token, and the env-var vs
  CLI-flag config precedence. Reduces rejection risk for the relay setup step.
