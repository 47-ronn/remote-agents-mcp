//! One-command MCP-client registration: `remote-agent install-mcp --client <id>`.
//!
//! Writes (or merges) a `remote-agents` MCP-server entry into the config file of
//! a popular AI agent so the user doesn't have to hand-edit JSON. The connection
//! flags (`--relay`/`--room`/`--token`/…) are baked into the server's `args`,
//! exactly like the README's manual config block.
//!
//! Three integration shapes cover the ecosystem:
//!   * `McpServers` — the canonical `{ "mcpServers": { name: {command,args} } }`
//!     JSON used by Claude Desktop/Code, Cursor, Cline, Roo, Kilo and Windsurf.
//!     Merged in place, preserving any servers the user already configured.
//!   * `ContextServers` / `Opencode` — JSON with a different top-level key (Zed,
//!     opencode). Same in-place merge.
//!   * `YamlSnippet` — Continue and Goose use YAML; rather than risk mangling a
//!     hand-written YAML file we print a ready-to-paste block.

use anyhow::{bail, Context, Result};
use serde_json::{json, Map, Value};
use std::path::PathBuf;

/// How a given client stores MCP servers in its config file.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shape {
    /// `{ "mcpServers": { "<name>": { "command", "args" } } }`
    McpServers,
    /// Zed: `{ "context_servers": { "<name>": { "command": {...}, "source" } } }`
    ContextServers,
    /// opencode: `{ "mcp": { "<name>": { "type":"local", "command":[...] } } }`
    Opencode,
    /// Continue / Goose: YAML — emit a paste-able snippet instead of editing.
    YamlSnippet,
}

struct Client {
    /// CLI id, e.g. `cursor`.
    id: &'static str,
    /// Human-readable name for messages.
    display: &'static str,
    shape: Shape,
    /// One-line hint about where the config lives / scope.
    note: &'static str,
}

const CLIENTS: &[Client] = &[
    Client { id: "claude-desktop", display: "Claude Desktop", shape: Shape::McpServers, note: "global (Claude config dir)" },
    Client { id: "claude-code", display: "Claude Code", shape: Shape::McpServers, note: "project-local .mcp.json (cwd)" },
    Client { id: "cursor", display: "Cursor", shape: Shape::McpServers, note: "global (~/.cursor/mcp.json)" },
    Client { id: "cline", display: "Cline", shape: Shape::McpServers, note: "VS Code globalStorage" },
    Client { id: "roo", display: "Roo Code", shape: Shape::McpServers, note: "VS Code globalStorage" },
    Client { id: "kilo", display: "Kilo Code", shape: Shape::McpServers, note: "VS Code globalStorage" },
    Client { id: "windsurf", display: "Windsurf", shape: Shape::McpServers, note: "global (~/.codeium/windsurf)" },
    Client { id: "zed", display: "Zed", shape: Shape::ContextServers, note: "global (zed settings.json)" },
    Client { id: "opencode", display: "opencode", shape: Shape::Opencode, note: "global (opencode.json)" },
    Client { id: "continue", display: "Continue", shape: Shape::YamlSnippet, note: "YAML — snippet printed" },
    Client { id: "goose", display: "Goose", shape: Shape::YamlSnippet, note: "YAML — snippet printed" },
];

fn find_client(id: &str) -> Option<&'static Client> {
    CLIENTS.iter().find(|c| c.id == id)
}

/// Human-readable list of supported client ids (for `--help` / errors).
pub fn supported_clients() -> String {
    CLIENTS
        .iter()
        .map(|c| format!("  {:<15} {} — {}", c.id, c.display, c.note))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Resolve the config file path for a client across platforms.
///
/// Most paths key off `dirs::config_dir()`, which already maps to the right
/// place per OS (Linux `~/.config`, macOS `~/Library/Application Support`,
/// Windows `%APPDATA%`) — the same convention VS Code, Claude and Zed use.
fn config_file(client: &Client) -> Result<PathBuf> {
    let cfg = || dirs::config_dir().context("no user config dir");
    let home = || dirs::home_dir().context("no home dir");
    // VS Code extension settings live under the same globalStorage subtree.
    let vscode_ext = |ext: &str, file: &str| -> Result<PathBuf> {
        Ok(cfg()?
            .join("Code")
            .join("User")
            .join("globalStorage")
            .join(ext)
            .join("settings")
            .join(file))
    };

    let path = match client.id {
        "claude-desktop" => cfg()?.join("Claude").join("claude_desktop_config.json"),
        // Claude Code reads a project-scoped .mcp.json from the working dir.
        "claude-code" => PathBuf::from(".mcp.json"),
        "cursor" => home()?.join(".cursor").join("mcp.json"),
        "cline" => vscode_ext("saoudrizwan.claude-dev", "cline_mcp_settings.json")?,
        "roo" => vscode_ext("rooveterinaryinc.roo-cline", "mcp_settings.json")?,
        "kilo" => vscode_ext("kilocode.kilo-code", "mcp_settings.json")?,
        "windsurf" => home()?.join(".codeium").join("windsurf").join("mcp_config.json"),
        "zed" => cfg()?.join("zed").join("settings.json"),
        "opencode" => cfg()?.join("opencode").join("opencode.json"),
        "continue" => home()?.join(".continue").join("config.yaml"),
        "goose" => cfg()?.join("goose").join("config.yaml"),
        other => bail!("unknown client '{other}'"),
    };
    Ok(path)
}

/// Build the server entry value for a JSON-shaped client.
fn json_entry(shape: Shape, command: &str, args: &[String]) -> Value {
    let args_json: Vec<Value> = args.iter().map(|a| Value::String(a.clone())).collect();
    match shape {
        Shape::McpServers => json!({ "command": command, "args": args_json }),
        Shape::ContextServers => json!({
            "source": "custom",
            "command": command,
            "args": args_json,
            "env": {},
        }),
        Shape::Opencode => {
            // opencode wants a single command array: [bin, ...args].
            let mut cmd = vec![Value::String(command.to_string())];
            cmd.extend(args_json);
            json!({ "type": "local", "command": cmd, "enabled": true })
        }
        Shape::YamlSnippet => Value::Null,
    }
}

/// Top-level container key that holds the per-server map for a JSON shape.
fn container_key(shape: Shape) -> &'static str {
    match shape {
        Shape::McpServers => "mcpServers",
        Shape::ContextServers => "context_servers",
        Shape::Opencode => "mcp",
        Shape::YamlSnippet => unreachable!("yaml shape has no json container"),
    }
}

/// Merge a server entry into an existing (possibly empty) JSON document,
/// preserving every other key and every other configured server. Pure: no I/O.
fn merge_entry(
    existing: Option<&str>,
    shape: Shape,
    server_name: &str,
    entry: Value,
) -> Result<String> {
    let mut root: Value = match existing.map(str::trim) {
        None | Some("") => Value::Object(Map::new()),
        Some(text) => serde_json::from_str(text)
            .context("existing config is not valid JSON; refusing to overwrite it")?,
    };
    if !root.is_object() {
        bail!("existing config root is not a JSON object; refusing to overwrite it");
    }

    let key = container_key(shape);
    let obj = root.as_object_mut().unwrap();
    let container = obj
        .entry(key.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if !container.is_object() {
        bail!("existing '{key}' is not a JSON object; refusing to overwrite it");
    }
    container
        .as_object_mut()
        .unwrap()
        .insert(server_name.to_string(), entry);

    serde_json::to_string_pretty(&root).context("serializing merged config")
}

/// Render the YAML snippet for Continue / Goose.
fn yaml_snippet(client_id: &str, server_name: &str, command: &str, args: &[String]) -> String {
    let args_yaml = args
        .iter()
        .map(|a| format!("      - {a:?}"))
        .collect::<Vec<_>>()
        .join("\n");
    match client_id {
        "continue" => format!(
            "mcpServers:\n  - name: {server_name}\n    command: {command:?}\n    args:\n{args_yaml}\n"
        ),
        "goose" => {
            let cmd_args = args
                .iter()
                .map(|a| format!("      - {a:?}"))
                .collect::<Vec<_>>()
                .join("\n");
            format!(
                "extensions:\n  {server_name}:\n    enabled: true\n    type: stdio\n    cmd: {command:?}\n    args:\n{cmd_args}\n"
            )
        }
        _ => unreachable!(),
    }
}

/// Install/merge the `remote-agents` MCP server into `client`'s config.
///
/// `command` is the absolute path to this binary; `args` is the full argv after
/// it (`["mcp", "--relay", …]`). `server_name` is the key under which the server
/// is registered (default `remote-agents`).
pub fn install_mcp(
    client_id: &str,
    server_name: &str,
    command: &str,
    args: &[String],
) -> Result<()> {
    let client = find_client(client_id).with_context(|| {
        format!(
            "unknown client '{client_id}'. Supported:\n{}",
            supported_clients()
        )
    })?;

    let path = config_file(client)?;

    if client.shape == Shape::YamlSnippet {
        let snippet = yaml_snippet(client.id, server_name, command, args);
        println!(
            "{} uses YAML. Add this to {}:\n\n{}",
            client.display,
            path.display(),
            snippet
        );
        return Ok(());
    }

    let entry = json_entry(client.shape, command, args);
    let existing = std::fs::read_to_string(&path).ok();
    let had_config = existing.is_some();
    let merged = merge_entry(existing.as_deref(), client.shape, server_name, entry)?;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
    }
    std::fs::write(&path, merged).with_context(|| format!("writing {}", path.display()))?;

    println!(
        "✓ Registered MCP server '{}' for {} ({} {})",
        server_name,
        client.display,
        if had_config { "merged into" } else { "created" },
        path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Vec<String> {
        vec![
            "mcp".into(),
            "--relay".into(),
            "wss://r".into(),
            "--room".into(),
            "dev".into(),
        ]
    }

    #[test]
    fn merge_into_empty_creates_container() {
        let entry = json_entry(Shape::McpServers, "remote-agents", &args());
        let out = merge_entry(None, Shape::McpServers, "remote-agents", entry).unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["mcpServers"]["remote-agents"]["command"], "remote-agents");
        assert_eq!(v["mcpServers"]["remote-agents"]["args"][0], "mcp");
    }

    #[test]
    fn merge_preserves_existing_servers_and_keys() {
        let existing = r#"{
            "theme": "dark",
            "mcpServers": { "other": { "command": "foo", "args": [] } }
        }"#;
        let entry = json_entry(Shape::McpServers, "bin", &args());
        let out = merge_entry(Some(existing), Shape::McpServers, "remote-agents", entry).unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        // Unrelated key survives.
        assert_eq!(v["theme"], "dark");
        // Pre-existing server survives.
        assert_eq!(v["mcpServers"]["other"]["command"], "foo");
        // Ours is added alongside it.
        assert_eq!(v["mcpServers"]["remote-agents"]["command"], "bin");
    }

    #[test]
    fn merge_replaces_our_own_entry_idempotently() {
        let first = merge_entry(
            None,
            Shape::McpServers,
            "remote-agents",
            json_entry(Shape::McpServers, "old", &args()),
        )
        .unwrap();
        let second = merge_entry(
            Some(&first),
            Shape::McpServers,
            "remote-agents",
            json_entry(Shape::McpServers, "new", &args()),
        )
        .unwrap();
        let v: Value = serde_json::from_str(&second).unwrap();
        assert_eq!(v["mcpServers"]["remote-agents"]["command"], "new");
        // Still exactly one entry — no duplication.
        assert_eq!(v["mcpServers"].as_object().unwrap().len(), 1);
    }

    #[test]
    fn opencode_shape_uses_command_array() {
        let entry = json_entry(Shape::Opencode, "remote-agents", &args());
        let out = merge_entry(None, Shape::Opencode, "remote-agents", entry).unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["mcp"]["remote-agents"]["type"], "local");
        assert_eq!(v["mcp"]["remote-agents"]["command"][0], "remote-agents");
        assert_eq!(v["mcp"]["remote-agents"]["command"][1], "mcp");
    }

    #[test]
    fn zed_shape_uses_context_servers() {
        let entry = json_entry(Shape::ContextServers, "bin", &args());
        let out = merge_entry(None, Shape::ContextServers, "remote-agents", entry).unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["context_servers"]["remote-agents"]["source"], "custom");
        assert_eq!(v["context_servers"]["remote-agents"]["command"], "bin");
    }

    #[test]
    fn malformed_existing_json_is_rejected_not_clobbered() {
        let err = merge_entry(
            Some("{ not json"),
            Shape::McpServers,
            "remote-agents",
            json_entry(Shape::McpServers, "bin", &args()),
        );
        assert!(err.is_err(), "must refuse to overwrite invalid JSON");
    }

    #[test]
    fn every_client_resolves_a_config_path() {
        for c in CLIENTS {
            // claude-code is cwd-relative; the rest need HOME/config — guard those.
            if c.id == "claude-code" {
                assert!(config_file(c).is_ok());
            } else if dirs::config_dir().is_some() && dirs::home_dir().is_some() {
                assert!(config_file(c).is_ok(), "{} should resolve", c.id);
            }
        }
    }

    #[test]
    fn yaml_clients_emit_nonempty_snippets() {
        for id in ["continue", "goose"] {
            let s = yaml_snippet(id, "remote-agents", "remote-agents", &args());
            assert!(s.contains("remote-agents"));
            assert!(s.contains("--relay"));
        }
    }
}
