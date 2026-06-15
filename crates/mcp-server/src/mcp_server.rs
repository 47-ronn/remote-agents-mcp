//! MCP Server implementation for the unified remote-agent.
//!
//! This module provides an MCP stdio server that can be used by Claude/opencode
//! to interact with both:
//! 1. The local machine (via the existing executor)
//! 2. Remote agents connected to the same relay room
//!
//! Architecture:
//! - All tools accept an optional `agent_id` parameter
//! - If `agent_id` is omitted or empty → execute locally
//! - If `agent_id` is provided → forward to remote agent via relay
//! - Special tools: `list_agents` and `fleet_exec` for multi-agent operations

use crate::config::Config;
use crate::executor;
use crate::state::AgentState;
use anyhow::Result;
use async_trait::async_trait;
use remote_agents_shared::{AgentMode, Command, CommandResult, Target};
use rust_mcp_sdk::mcp_server::{server_runtime, ServerHandler};
use rust_mcp_sdk::schema::*;
use rust_mcp_sdk::{McpServer, StdioTransport, ToMcpServerHandler, TransportOptions};

use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ============================================================================
// Helper functions
// ============================================================================

fn default_origin() -> String {
    "origin".to_string()
}

fn make_tool(name: &str, description: &str, properties: Value, required: Vec<&str>) -> Tool {
    let props_map: Option<BTreeMap<String, Map<String, Value>>> =
        if let Value::Object(obj) = properties {
            Some(
                obj.into_iter()
                    .map(|(k, v)| {
                        let inner = match v {
                            Value::Object(m) => m,
                            _ => Map::new(),
                        };
                        (k, inner)
                    })
                    .collect(),
            )
        } else {
            None
        };

    let required_vec: Vec<String> = required.into_iter().map(String::from).collect();

    Tool {
        name: name.to_string(),
        description: Some(description.to_string()),
        input_schema: ToolInputSchema::new(required_vec, props_map, None),
        annotations: None,
        execution: None,
        icons: vec![],
        meta: None,
        output_schema: None,
        title: None,
    }
}

fn parse_error(tool_name: &str, e: impl std::fmt::Display) -> CallToolError {
    CallToolError::invalid_arguments(tool_name, Some(format!("Invalid params: {}", e)))
}

fn exec_error(e: impl std::fmt::Display) -> CallToolError {
    CallToolError::from_message(format!("Execution failed: {}", e))
}

fn text_result(text: String) -> CallToolResult {
    CallToolResult::text_content(vec![TextContent::new(text, None, None)])
}

/// Extract optional agent_id from args, returns None if empty or missing
fn extract_agent_id(args: &Map<String, Value>) -> Option<String> {
    args.get("agent_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

/// Parse a fleet target string:
/// - `all` → every agent
/// - `os:<family>` / `platform:<family>` → agents on that OS family
/// - otherwise → comma-separated tags
fn parse_target(s: &str) -> Target {
    let t = s.trim();
    if t.eq_ignore_ascii_case("all") {
        return Target::All;
    }
    for prefix in ["os:", "platform:"] {
        if let Some(family) = t.strip_prefix(prefix) {
            return Target::Platform {
                family: family.trim().to_string(),
            };
        }
    }
    Target::Tagged {
        tags: t.split(',').map(|s| s.trim().to_string()).collect(),
    }
}

/// Client-side selection of agent ids matching `target`, mirroring the relay's
/// `resolve_targets`. Used to scope the MapReduce worker pool (e.g. only
/// `os:linux` hosts) before round-robin partition dispatch.
fn agents_matching(agents: &[remote_agents_shared::AgentInfo], target: &Target) -> Vec<String> {
    agents
        .iter()
        .filter(|a| match target {
            Target::All => true,
            Target::Agent { id } => &a.id == id,
            Target::Tagged { tags } => a.tags.iter().any(|t| tags.contains(t)),
            Target::Platform { family } => {
                family.eq_ignore_ascii_case(&a.platform.family)
                    || family.eq_ignore_ascii_case(&a.os)
            }
        })
        .map(|a| a.id.clone())
        .collect()
}

/// Parse AgentMode from string
fn parse_mode(mode_str: &str) -> Result<AgentMode, String> {
    match mode_str.to_lowercase().as_str() {
        "plan" => Ok(AgentMode::Plan),
        "edit" => Ok(AgentMode::Edit),
        "bypass" => Ok(AgentMode::Bypass),
        "disabled" => Ok(AgentMode::Disabled),
        other => Err(format!(
            "Invalid mode '{}'. Use: plan, edit, bypass, disabled",
            other
        )),
    }
}

// ============================================================================
// Tool definitions - all with optional agent_id
// ============================================================================

/// Common description suffix for agent_id
const AGENT_ID_DESC: &str = "Optional: ID of remote agent. If omitted, executes locally.";

fn all_tools(has_relay: bool) -> Vec<Tool> {
    let mut tools = vec![
        // === Execution ===
        make_tool(
            "exec",
            "Execute a shell command. Runs locally or on a remote agent if agent_id is provided.",
            json!({
                "command": {"type": "string", "description": "The shell command to execute"},
                "timeout_ms": {"type": "integer", "description": "Optional timeout in milliseconds"},
                "cwd": {"type": "string", "description": "Optional working directory"},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["command"],
        ),
        
        // === File operations ===
        make_tool(
            "read_file",
            "Read a file. Reads locally or from a remote agent if agent_id is provided.",
            json!({
                "path": {"type": "string", "description": "Path to the file to read"},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["path"],
        ),
        make_tool(
            "write_file",
            "Write content to a file. Writes locally or to a remote agent if agent_id is provided.",
            json!({
                "path": {"type": "string", "description": "Path to the file to write"},
                "content": {"type": "string", "description": "Content to write"},
                "create_backup": {"type": "boolean", "description": "Whether to create a backup", "default": true},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["path", "content"],
        ),
        make_tool(
            "list_dir",
            "List directory contents. Lists locally or on a remote agent if agent_id is provided.",
            json!({
                "path": {"type": "string", "description": "Path to the directory"},
                "pattern": {"type": "string", "description": "Optional glob pattern to filter entries"},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["path"],
        ),
        
        // === Agent info & mode ===
        make_tool(
            "get_info",
            "Get agent information (mode, OS, hostname, etc.). Gets local info or from remote agent.",
            json!({
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec![],
        ),
        make_tool(
            "set_mode",
            "Set agent operating mode: plan (read-only), edit (safe writes), bypass (unrestricted), disabled.",
            json!({
                "mode": {"type": "string", "description": "New mode: plan, edit, bypass, or disabled", "enum": ["plan", "edit", "bypass", "disabled"]},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["mode"],
        ),
        
        // === Git operations ===
        make_tool(
            "git_status",
            "Get git repository status. Checks locally or on a remote agent if agent_id is provided.",
            json!({
                "repo": {"type": "string", "description": "Path to the git repository"},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["repo"],
        ),
        make_tool(
            "git_pull",
            "Pull changes from remote git repository.",
            json!({
                "repo": {"type": "string", "description": "Path to the git repository"},
                "remote": {"type": "string", "description": "Remote name", "default": "origin"},
                "branch": {"type": "string", "description": "Branch name (optional)"},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["repo"],
        ),
        make_tool(
            "git_commit",
            "Commit changes to git repository.",
            json!({
                "repo": {"type": "string", "description": "Path to the git repository"},
                "message": {"type": "string", "description": "Commit message"},
                "files": {"type": "array", "items": {"type": "string"}, "description": "Files to commit (empty = all staged)"},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["repo", "message"],
        ),
        make_tool(
            "git_push",
            "Push changes to remote git repository.",
            json!({
                "repo": {"type": "string", "description": "Path to the git repository"},
                "remote": {"type": "string", "description": "Remote name", "default": "origin"},
                "branch": {"type": "string", "description": "Branch name (optional)"},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["repo"],
        ),
        
        // === Scheduling ===
        make_tool(
            "schedule_add",
            "Add a cron-scheduled task (6-field cron: sec min hour day month weekday).",
            json!({
                "name": {"type": "string", "description": "Task name (unique identifier)"},
                "cron": {"type": "string", "description": "Cron expression"},
                "command": {"type": "string", "description": "Command to execute"},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["name", "cron", "command"],
        ),
        make_tool(
            "schedule_remove",
            "Remove a scheduled task.",
            json!({
                "name": {"type": "string", "description": "Task name to remove"},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["name"],
        ),
        make_tool(
            "schedule_list",
            "List all scheduled tasks.",
            json!({
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec![],
        ),
        
        // === Autonomous tasks ===
        make_tool(
            "task_dispatch",
            "Dispatch an autonomous AI task (requires autonomous mode enabled on target agent).",
            json!({
                "prompt": {"type": "string", "description": "The prompt/instructions for the autonomous task"},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["prompt"],
        ),
        make_tool(
            "task_get",
            "Get status and result of an autonomous task.",
            json!({
                "id": {"type": "string", "description": "Task ID"},
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec!["id"],
        ),
        make_tool(
            "task_list",
            "List all autonomous tasks.",
            json!({
                "agent_id": {"type": "string", "description": AGENT_ID_DESC}
            }),
            vec![],
        ),
    ];

    // Add relay-only tools when connected
    if has_relay {
        tools.extend(vec![
            make_tool(
                "list_agents",
                "List all agents connected to the relay room.",
                json!({}),
                vec![],
            ),
            make_tool(
                "fleet_exec",
                "Execute a command on multiple agents at once (all, by tags, or by OS).",
                json!({
                    "target": {"type": "string", "description": "Target: 'all', comma-separated tags, or 'os:<family>' (e.g. 'os:linux')"},
                    "command": {"type": "string", "description": "The shell command to execute"},
                    "timeout_ms": {"type": "integer", "description": "Optional timeout in milliseconds"}
                }),
                vec!["target", "command"],
            ),
            make_tool(
                "mapreduce",
                "Distributed MapReduce across the fleet. `data` is partitioned across workers; \
                 `map_fn` (a shell command) runs on each worker with its partition as a JSON array \
                 on stdin; outputs are then folded by `reduce_fn` (a shell command) with the map \
                 outputs newline-joined on stdin. Failed partitions are retried up to `max_retries`.",
                json!({
                    "data": {"type": "array", "items": {"type": "string"}, "description": "Input records to partition across workers"},
                    "map_fn": {"type": "string", "description": "Shell command; receives its partition (JSON array) on stdin"},
                    "reduce_fn": {"type": "string", "description": "Shell command; receives map outputs (newline-joined) on stdin"},
                    "target": {"type": "string", "description": "Optional worker pool: 'all' (default) | comma-separated tags | 'os:<family>'"},
                    "workers": {"type": "integer", "description": "Number of partitions (default: number of matched agents)"},
                    "max_retries": {"type": "integer", "description": "Max re-dispatch attempts per failed partition (default 1)"}
                }),
                vec!["data", "map_fn", "reduce_fn"],
            ),
            make_tool(
                "fleet_read",
                "Read a file from multiple agents at once (all, tags, or 'os:<family>').",
                json!({
                    "target": {"type": "string", "description": "Target: 'all', comma-separated tags, or 'os:<family>'"},
                    "path": {"type": "string", "description": "Path to the file to read on each agent"}
                }),
                vec!["target", "path"],
            ),
            make_tool(
                "fleet_write",
                "Write a file to multiple agents at once (all, tags, or 'os:<family>'). Backup is created on hosts in Edit mode.",
                json!({
                    "target": {"type": "string", "description": "Target: 'all', comma-separated tags, or 'os:<family>'"},
                    "path": {"type": "string", "description": "Path to the file to write on each agent"},
                    "content": {"type": "string", "description": "Content to write"}
                }),
                vec!["target", "path", "content"],
            ),
            make_tool(
                "fleet_git",
                "Run a git operation (status|pull|commit|push) across multiple agents (all, tags, or 'os:<family>').",
                json!({
                    "target": {"type": "string", "description": "Target: 'all', comma-separated tags, or 'os:<family>'"},
                    "op": {"type": "string", "description": "Git operation", "enum": ["status", "pull", "commit", "push"]},
                    "repo": {"type": "string", "description": "Path to the git repository on each agent"},
                    "remote": {"type": "string", "description": "Remote name (default 'origin')"},
                    "branch": {"type": "string", "description": "Branch (for pull/push)"},
                    "message": {"type": "string", "description": "Commit message (for commit)"},
                    "files": {"type": "array", "items": {"type": "string"}, "description": "Files to stage (for commit; empty = all)"}
                }),
                vec!["target", "op", "repo"],
            ),
        ]);
    }

    tools
}

// ============================================================================
// MCP Handler
// ============================================================================

/// MCP server handler that wraps the agent state and optional relay connection
pub struct McpHandler {
    state: Arc<RwLock<AgentState>>,
    /// Optional relay server for remote agent control
    relay: Option<Arc<crate::relay_api::McpServer>>,
    /// Room name for relay connection
    room: Option<String>,
}

impl McpHandler {
    pub fn new(state: AgentState) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
            relay: None,
            room: None,
        }
    }

    pub fn with_relay(mut self, relay: Arc<crate::relay_api::McpServer>, room: String) -> Self {
        self.relay = Some(relay);
        self.room = Some(room);
        self
    }

    fn has_relay(&self) -> bool {
        self.relay.is_some() && self.room.is_some()
    }

    /// Execute a command on a remote agent via relay
    async fn remote_command(
        &self,
        agent_id: &str,
        command: Command,
    ) -> Result<CommandResult, CallToolError> {
        let relay = self
            .relay
            .as_ref()
            .ok_or_else(|| exec_error("Relay not connected. Cannot execute on remote agent."))?;
        let room = self
            .room
            .as_ref()
            .ok_or_else(|| exec_error("Room not configured"))?;

        debug!("Forwarding command to agent {}: {:?}", agent_id, command);

        let results = relay
            .send_command(room, Target::Agent { id: agent_id.to_string() }, command)
            .await
            .map_err(exec_error)?;

        results
            .into_iter()
            .next()
            .map(|(_, r)| r)
            .ok_or_else(|| exec_error("No response from remote agent"))
    }
}

#[async_trait]
impl ServerHandler for McpHandler {
    async fn handle_list_tools_request(
        &self,
        _request: Option<PaginatedRequestParams>,
        _runtime: Arc<dyn McpServer>,
    ) -> std::result::Result<ListToolsResult, RpcError> {
        Ok(ListToolsResult {
            tools: all_tools(self.has_relay()),
            meta: None,
            next_cursor: None,
        })
    }

    async fn handle_call_tool_request(
        &self,
        params: CallToolRequestParams,
        _runtime: Arc<dyn McpServer>,
    ) -> std::result::Result<CallToolResult, CallToolError> {
        let tool_name = &params.name;
        let args = params.arguments.unwrap_or_default();

        // Handle special multi-agent tools
        match tool_name.as_str() {
            "list_agents" => return self.handle_list_agents().await,
            "fleet_exec" => return self.handle_fleet_exec(args).await,
            "fleet_read" => return self.handle_fleet_read(args).await,
            "fleet_write" => return self.handle_fleet_write(args).await,
            "fleet_git" => return self.handle_fleet_git(args).await,
            "mapreduce" => return self.handle_mapreduce(args).await,
            _ => {}
        }

        // Extract optional agent_id for routing
        let agent_id = extract_agent_id(&args);

        // Build the command
        let command = self.build_command(tool_name, &args)?;

        // Execute locally or remotely based on agent_id
        let result = if let Some(ref aid) = agent_id {
            debug!("Routing {} to remote agent {}", tool_name, aid);
            self.remote_command(aid, command).await?
        } else {
            debug!("Executing {} locally", tool_name);
            let state = self.state.read().await;
            executor::execute(&command, &state)
                .await
                .map_err(exec_error)?
        };

        Ok(text_result(format_result(&result)))
    }
}

impl McpHandler {
    /// Build a Command from tool name and arguments
    fn build_command(
        &self,
        tool_name: &str,
        args: &Map<String, Value>,
    ) -> Result<Command, CallToolError> {
        let get_str = |key: &str| -> Option<String> {
            args.get(key).and_then(|v| v.as_str()).map(String::from)
        };
        let get_str_required = |key: &str| -> Result<String, CallToolError> {
            get_str(key).ok_or_else(|| parse_error(tool_name, format!("missing required field '{}'", key)))
        };
        let get_u64 = |key: &str| -> Option<u64> {
            args.get(key).and_then(|v| v.as_u64())
        };
        let get_bool = |key: &str, default: bool| -> bool {
            args.get(key).and_then(|v| v.as_bool()).unwrap_or(default)
        };
        let get_str_vec = |key: &str| -> Vec<String> {
            args.get(key)
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default()
        };

        let command = match tool_name {
            "exec" => Command::Exec {
                command: get_str_required("command")?,
                timeout_ms: get_u64("timeout_ms"),
                cwd: get_str("cwd"),
            },
            "read_file" => Command::ReadFile {
                path: get_str_required("path")?,
            },
            "write_file" => Command::WriteFile {
                path: get_str_required("path")?,
                content: get_str_required("content")?,
                create_backup: get_bool("create_backup", true),
            },
            "list_dir" => Command::ListDir {
                path: get_str_required("path")?,
                pattern: get_str("pattern"),
            },
            "get_info" => Command::GetInfo,
            "set_mode" => {
                let mode_str = get_str_required("mode")?;
                let mode = parse_mode(&mode_str).map_err(|e| parse_error(tool_name, e))?;
                Command::SetMode { mode }
            }
            "git_status" => Command::GitStatus {
                repo: get_str_required("repo")?,
            },
            "git_pull" => Command::GitPull {
                repo: get_str_required("repo")?,
                remote: get_str("remote").unwrap_or_else(default_origin),
                branch: get_str("branch"),
            },
            "git_commit" => Command::GitCommit {
                repo: get_str_required("repo")?,
                message: get_str_required("message")?,
                files: get_str_vec("files"),
            },
            "git_push" => Command::GitPush {
                repo: get_str_required("repo")?,
                remote: get_str("remote").unwrap_or_else(default_origin),
                branch: get_str("branch"),
            },
            "schedule_add" => Command::ScheduleAdd {
                name: get_str_required("name")?,
                cron: get_str_required("cron")?,
                command: get_str_required("command")?,
            },
            "schedule_remove" => Command::ScheduleRemove {
                name: get_str_required("name")?,
            },
            "schedule_list" => Command::ScheduleList,
            "task_dispatch" => Command::TaskDispatch {
                prompt: get_str_required("prompt")?,
            },
            "task_get" => Command::TaskGet {
                id: get_str_required("id")?,
            },
            "task_list" => Command::TaskList,
            other => {
                return Err(CallToolError::unknown_tool(other.to_string()));
            }
        };

        Ok(command)
    }

    /// Handle list_agents tool
    async fn handle_list_agents(&self) -> Result<CallToolResult, CallToolError> {
        let relay = self
            .relay
            .as_ref()
            .ok_or_else(|| exec_error("Relay not connected"))?;
        let room = self
            .room
            .as_ref()
            .ok_or_else(|| exec_error("Room not configured"))?;

        let agents = relay.list_agents(room).await.map_err(exec_error)?;
        let text =
            serde_json::to_string_pretty(&agents).unwrap_or_else(|_| format!("{:?}", agents));
        Ok(text_result(text))
    }

    /// Handle fleet_exec tool
    async fn handle_fleet_exec(
        &self,
        args: Map<String, Value>,
    ) -> Result<CallToolResult, CallToolError> {
        let relay = self
            .relay
            .as_ref()
            .ok_or_else(|| exec_error("Relay not connected"))?;
        let room = self
            .room
            .as_ref()
            .ok_or_else(|| exec_error("Room not configured"))?;

        let target_str = args
            .get("target")
            .and_then(|v| v.as_str())
            .ok_or_else(|| parse_error("fleet_exec", "missing required field 'target'"))?;
        let command = args
            .get("command")
            .and_then(|v| v.as_str())
            .ok_or_else(|| parse_error("fleet_exec", "missing required field 'command'"))?;
        let timeout_ms = args.get("timeout_ms").and_then(|v| v.as_u64());

        let target = parse_target(target_str);

        let results = relay
            .fleet_exec(room, target, command, timeout_ms)
            .await
            .map_err(exec_error)?;

        Ok(text_result(format_outcomes(results)))
    }

    /// Resolve the connected relay + room, or a tool error if offline.
    fn relay_room(
        &self,
    ) -> Result<(&Arc<crate::relay_api::McpServer>, &String), CallToolError> {
        let relay = self
            .relay
            .as_ref()
            .ok_or_else(|| exec_error("Relay not connected"))?;
        let room = self
            .room
            .as_ref()
            .ok_or_else(|| exec_error("Room not configured"))?;
        Ok((relay, room))
    }

    /// Handle fleet_read tool.
    async fn handle_fleet_read(
        &self,
        args: Map<String, Value>,
    ) -> Result<CallToolResult, CallToolError> {
        let (relay, room) = self.relay_room()?;
        let target = parse_target(
            args.get("target")
                .and_then(|v| v.as_str())
                .ok_or_else(|| parse_error("fleet_read", "missing required field 'target'"))?,
        );
        let path = args
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| parse_error("fleet_read", "missing required field 'path'"))?;

        let results = relay.fleet_read(room, target, path).await.map_err(exec_error)?;
        Ok(text_result(format_outcomes(results)))
    }

    /// Handle fleet_write tool.
    async fn handle_fleet_write(
        &self,
        args: Map<String, Value>,
    ) -> Result<CallToolResult, CallToolError> {
        let (relay, room) = self.relay_room()?;
        let target = parse_target(
            args.get("target")
                .and_then(|v| v.as_str())
                .ok_or_else(|| parse_error("fleet_write", "missing required field 'target'"))?,
        );
        let path = args
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| parse_error("fleet_write", "missing required field 'path'"))?;
        let content = args
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| parse_error("fleet_write", "missing required field 'content'"))?;

        let results = relay
            .fleet_write(room, target, path, content)
            .await
            .map_err(exec_error)?;
        Ok(text_result(format_outcomes(results)))
    }

    /// Handle fleet_git tool.
    async fn handle_fleet_git(
        &self,
        args: Map<String, Value>,
    ) -> Result<CallToolResult, CallToolError> {
        let (relay, room) = self.relay_room()?;
        let target = parse_target(
            args.get("target")
                .and_then(|v| v.as_str())
                .ok_or_else(|| parse_error("fleet_git", "missing required field 'target'"))?,
        );
        let op = args
            .get("op")
            .and_then(|v| v.as_str())
            .ok_or_else(|| parse_error("fleet_git", "missing required field 'op'"))?;
        let repo = args
            .get("repo")
            .and_then(|v| v.as_str())
            .ok_or_else(|| parse_error("fleet_git", "missing required field 'repo'"))?;
        let remote = args.get("remote").and_then(|v| v.as_str()).map(String::from);
        let branch = args.get("branch").and_then(|v| v.as_str()).map(String::from);
        let message = args.get("message").and_then(|v| v.as_str()).map(String::from);
        let files: Vec<String> = args
            .get("files")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();

        let results = relay
            .fleet_git(room, target, op, repo, remote, branch, message, files)
            .await
            .map_err(exec_error)?;
        Ok(text_result(format_outcomes(results)))
    }

    /// Handle the `mapreduce` tool: distribute map tasks across the fleet,
    /// collect/retry, then reduce. The orchestration lives in `crate::mapreduce`;
    /// here we only supply the transport seam (dispatch one command to a worker).
    async fn handle_mapreduce(
        &self,
        args: Map<String, Value>,
    ) -> Result<CallToolResult, CallToolError> {
        let relay = self
            .relay
            .as_ref()
            .ok_or_else(|| exec_error("Relay not connected"))?;
        let room = self
            .room
            .as_ref()
            .ok_or_else(|| exec_error("Room not configured"))?;

        // Parse arguments.
        let data: Vec<String> = args
            .get("data")
            .and_then(|v| v.as_array())
            .ok_or_else(|| parse_error("mapreduce", "missing required array 'data'"))?
            .iter()
            .map(|v| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .collect();
        let map_fn = args
            .get("map_fn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| parse_error("mapreduce", "missing required field 'map_fn'"))?
            .to_string();
        let reduce_fn = args
            .get("reduce_fn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| parse_error("mapreduce", "missing required field 'reduce_fn'"))?
            .to_string();
        let max_retries = args.get("max_retries").and_then(|v| v.as_u64()).unwrap_or(1) as u32;

        // Resolve the worker pool from the current agent list, optionally
        // scoped by `target` (all | tags | os:<family>) so compute runs only on
        // suitable hosts.
        let all_agents = relay.list_agents(room).await.map_err(exec_error)?;
        let agent_ids: Vec<String> = match args.get("target").and_then(|v| v.as_str()) {
            Some(t) => agents_matching(&all_agents, &parse_target(t)),
            None => all_agents.iter().map(|a| a.id.clone()).collect(),
        };
        if agent_ids.is_empty() {
            return Err(exec_error("No agents match the target for mapreduce"));
        }
        let workers = args
            .get("workers")
            .and_then(|v| v.as_u64())
            .map(|w| w as usize)
            .unwrap_or(agent_ids.len());

        // Transport seam: send one command to a worker and await its result.
        // Map partitions are spread round-robin across agents by partition id.
        let relay = relay.clone();
        let room = room.clone();
        let dispatch = move |cmd: Command| {
            let relay = relay.clone();
            let room = room.clone();
            let ids = agent_ids.clone();
            async move {
                let target_id = match &cmd {
                    Command::MapTask { partition_id, .. } => {
                        ids[(*partition_id as usize) % ids.len()].clone()
                    }
                    _ => ids[0].clone(),
                };
                let results = relay
                    .send_command(&room, Target::Agent { id: target_id }, cmd)
                    .await
                    .map_err(|e| e.to_string())?;
                results
                    .into_iter()
                    .next()
                    .map(|(_, r)| r)
                    .ok_or_else(|| "no response from agent".to_string())
            }
        };

        let job_id = uuid::Uuid::new_v4().to_string();
        let output = crate::mapreduce::run_job(
            job_id, &data, &map_fn, &reduce_fn, workers, max_retries, dispatch,
        )
        .await
        .map_err(exec_error)?;

        Ok(text_result(output))
    }
}

/// Format per-agent fleet outcomes into a human-readable, one-block-per-host
/// report (`[<agent> OK|FAIL] <body>`), separated by `---`.
fn format_outcomes(results: Vec<crate::relay_controller::AgentOutcome>) -> String {
    results
        .into_iter()
        .map(|outcome| {
            let status = if outcome.result.is_some() { "OK" } else { "FAIL" };
            let body = match outcome.result {
                Some(r) => format_result(&r),
                None => outcome.error.unwrap_or_else(|| "unknown error".to_string()),
            };
            format!("[{} {}] {}", outcome.agent_id, status, body)
        })
        .collect::<Vec<_>>()
        .join("\n---\n")
}

/// Format CommandResult to human-readable text
fn format_result(result: &CommandResult) -> String {
    match result {
        CommandResult::Exec {
            stdout,
            stderr,
            exit_code,
        } => {
            let mut out = String::new();
            if !stdout.is_empty() {
                out.push_str(stdout);
            }
            if !stderr.is_empty() {
                if !out.is_empty() {
                    out.push_str("\n--- stderr ---\n");
                }
                out.push_str(stderr);
            }
            out.push_str(&format!("\n[exit code: {}]", exit_code));
            out
        }
        CommandResult::File { content, size } => {
            format!("{}\n[{} bytes]", content, size)
        }
        CommandResult::Dir { entries } => entries
            .iter()
            .map(|e| {
                let suffix = if e.is_dir { "/" } else { "" };
                format!("{}{} ({} bytes)", e.name, suffix, e.size)
            })
            .collect::<Vec<_>>()
            .join("\n"),
        CommandResult::Ok => "OK".to_string(),
        CommandResult::Mode { mode } => format!("Mode set to {:?}", mode),
        CommandResult::Info { info } => {
            serde_json::to_string_pretty(info).unwrap_or_else(|_| format!("{:?}", info))
        }
        CommandResult::GitStatus { status } => {
            serde_json::to_string_pretty(status).unwrap_or_else(|_| format!("{:?}", status))
        }
        CommandResult::Git { output, success } => {
            format!("{}\n[success: {}]", output, success)
        }
        CommandResult::Schedule { tasks } => {
            serde_json::to_string_pretty(tasks).unwrap_or_else(|_| format!("{:?}", tasks))
        }
        CommandResult::TaskQueued { id } => format!("Task queued: {}", id),
        CommandResult::Task { task } => {
            serde_json::to_string_pretty(task).unwrap_or_else(|_| format!("{:?}", task))
        }
        CommandResult::TaskList { tasks } => {
            serde_json::to_string_pretty(tasks).unwrap_or_else(|_| format!("{:?}", tasks))
        }
        CommandResult::MapResult {
            job_id,
            partition_id,
            output,
            success,
            error,
        } => match error {
            Some(e) => format!("map[{job_id}/{partition_id}] failed: {e}"),
            None => format!("map[{job_id}/{partition_id}] (success: {success})\n{output}"),
        },
        CommandResult::ReduceResult {
            job_id,
            output,
            success,
            error,
        } => match error {
            Some(e) => format!("reduce[{job_id}] failed: {e}"),
            None => format!("reduce[{job_id}] (success: {success})\n{output}"),
        },
    }
}

// ============================================================================
// MCP Server Runner
// ============================================================================

/// Run the MCP stdio server
pub async fn run_mcp_server(config: &Config) -> Result<()> {
    info!("Starting MCP stdio server for agent '{}'", config.name);

    // Create agent state
    let state = AgentState::new(config.clone());

    // Start scheduler in background
    state.start_scheduler();

    // Optionally connect to relay for remote agent control
    let (relay, room) = if !config.room.is_empty() && !config.token.is_empty() {
        info!(
            "Connecting to relay room '{}' for remote agent control",
            config.room
        );
        let relay = Arc::new(crate::relay_api::McpServer::new());
        match relay
            .join_room(&config.relay_url, &config.room, &config.token, None)
            .await
        {
            Ok(msg) => {
                info!("Connected to relay: {}", msg);
                (Some(relay), Some(config.room.clone()))
            }
            Err(e) => {
                warn!("Failed to connect to relay: {}. Remote tools disabled.", e);
                (None, None)
            }
        }
    } else {
        info!("No relay configured. Local-only mode.");
        (None, None)
    };

    // Build server info
    let has_relay = relay.is_some();
    let instructions = if has_relay {
        format!(
            "This is remote-agent '{}' running on {} ({}). \
             All tools accept an optional 'agent_id' parameter: \
             - If omitted: executes locally \
             - If provided: forwards to the specified remote agent \
             Use 'list_agents' to see connected agents. \
             Use 'fleet_exec' to run commands on multiple agents at once.",
            config.name,
            hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
            std::env::consts::OS
        )
    } else {
        format!(
            "This is remote-agent '{}' running on {} ({}). \
             All tools execute locally. Connect to a relay room to enable remote agent control.",
            config.name,
            hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
            std::env::consts::OS
        )
    };

    let server_info = InitializeResult {
        server_info: Implementation {
            name: "remote-agent".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            title: Some(format!("Remote Agent: {}", config.name)),
            description: Some(
                "Unified remote agent with local execution and relay connectivity. \
                 All tools work locally by default, add agent_id to target remote agents."
                    .to_string(),
            ),
            icons: vec![],
            website_url: Some("https://github.com/rust-mcp-stack/remote-agents".to_string()),
        },
        capabilities: ServerCapabilities {
            tools: Some(ServerCapabilitiesTools { list_changed: None }),
            ..Default::default()
        },
        protocol_version: ProtocolVersion::V2025_11_25.into(),
        instructions: Some(instructions),
        meta: None,
    };

    // Create transport and handler
    let transport = StdioTransport::new(TransportOptions::default())
        .map_err(|e| anyhow::anyhow!("Transport error: {:?}", e))?;

    let mut handler = McpHandler::new(state);
    if let (Some(r), Some(room)) = (relay, room) {
        handler = handler.with_relay(r, room);
    }

    // Create and start server
    use rust_mcp_sdk::mcp_server::McpServerOptions;

    let options = McpServerOptions {
        server_details: server_info,
        transport,
        handler: handler.to_mcp_server_handler(),
        task_store: None,
        client_task_store: None,
        message_observer: None,
    };

    let server = server_runtime::create_server(options);

    info!("MCP server ready, waiting for connections...");
    server
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("MCP server error: {:?}", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use remote_agents_shared::DirEntry;

    /// Build an args map from a JSON object literal.
    fn args(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => panic!("args() expects a JSON object"),
        }
    }

    fn handler() -> McpHandler {
        McpHandler::new(AgentState::new(Config::default()))
    }

    // --- parse_mode ---------------------------------------------------------

    #[test]
    fn parse_mode_accepts_all_variants_case_insensitively() {
        assert!(matches!(parse_mode("plan"), Ok(AgentMode::Plan)));
        assert!(matches!(parse_mode("EDIT"), Ok(AgentMode::Edit)));
        assert!(matches!(parse_mode("Bypass"), Ok(AgentMode::Bypass)));
        assert!(matches!(parse_mode("disabled"), Ok(AgentMode::Disabled)));
    }

    #[test]
    fn parse_mode_rejects_unknown() {
        let err = parse_mode("turbo").unwrap_err();
        assert!(err.contains("turbo"), "error should name the bad mode: {err}");
    }

    #[test]
    fn format_outcomes_reports_ok_and_fail_per_host() {
        use crate::relay_controller::AgentOutcome;
        let outcomes = vec![
            AgentOutcome {
                agent_id: "h1".into(),
                result: Some(CommandResult::Git { output: "pulled".into(), success: true }),
                error: None,
            },
            AgentOutcome {
                agent_id: "h2".into(),
                result: None,
                error: Some("boom".into()),
            },
        ];
        let text = format_outcomes(outcomes);
        assert!(text.contains("[h1 OK]"));
        assert!(text.contains("pulled"));
        assert!(text.contains("[h2 FAIL] boom"));
        assert!(text.contains("\n---\n"));
    }

    #[test]
    fn agents_matching_filters_by_target() {
        use remote_agents_shared::AgentInfo;
        let mk = |id: &str, os: &str, tags: &[&str]| {
            let mut a = AgentInfo {
                id: id.into(),
                name: id.into(),
                mode: AgentMode::Plan,
                os: os.into(),
                arch: "x86_64".into(),
                hostname: id.into(),
                tags: tags.iter().map(|s| s.to_string()).collect(),
                platform: Default::default(),
                autonomous: false,
                connected_at: 0,
                session_id: None,
            };
            a.platform.family = os.into();
            a
        };
        let agents = vec![
            mk("a", "linux", &["backend"]),
            mk("b", "windows", &["frontend"]),
            mk("c", "linux", &["db"]),
        ];

        assert_eq!(agents_matching(&agents, &Target::All).len(), 3);
        assert_eq!(
            agents_matching(&agents, &Target::Platform { family: "linux".into() }),
            vec!["a".to_string(), "c".to_string()]
        );
        assert_eq!(
            agents_matching(&agents, &Target::Tagged { tags: vec!["frontend".into()] }),
            vec!["b".to_string()]
        );
        assert!(agents_matching(&agents, &Target::Platform { family: "bsd".into() }).is_empty());
    }

    #[test]
    fn parse_target_recognizes_all_os_and_tags() {
        assert!(matches!(parse_target("all"), Target::All));
        assert!(matches!(parse_target("ALL"), Target::All));
        match parse_target("os:linux") {
            Target::Platform { family } => assert_eq!(family, "linux"),
            other => panic!("expected Platform, got {other:?}"),
        }
        match parse_target("platform: macos ") {
            Target::Platform { family } => assert_eq!(family, "macos"),
            other => panic!("expected Platform, got {other:?}"),
        }
        match parse_target("backend,db") {
            Target::Tagged { tags } => assert_eq!(tags, vec!["backend", "db"]),
            other => panic!("expected Tagged, got {other:?}"),
        }
    }

    // --- extract_agent_id ---------------------------------------------------

    #[test]
    fn extract_agent_id_handles_present_missing_and_empty() {
        assert_eq!(
            extract_agent_id(&args(json!({"agent_id": "abc"}))),
            Some("abc".to_string())
        );
        assert_eq!(extract_agent_id(&args(json!({}))), None);
        // Empty string is treated as "no agent" (local execution).
        assert_eq!(extract_agent_id(&args(json!({"agent_id": ""}))), None);
    }

    // --- all_tools ----------------------------------------------------------

    #[test]
    fn all_tools_gates_relay_only_tools_on_has_relay() {
        let names = |has_relay| -> Vec<String> {
            all_tools(has_relay).into_iter().map(|t| t.name).collect()
        };

        let local = names(false);
        // Core tools are always present.
        for core in ["exec", "read_file", "write_file", "list_dir", "get_info", "set_mode"] {
            assert!(local.contains(&core.to_string()), "missing core tool {core}");
        }
        // Relay-only tools are absent without a relay.
        assert!(!local.contains(&"list_agents".to_string()));
        assert!(!local.contains(&"fleet_exec".to_string()));

        let relay = names(true);
        for t in ["list_agents", "fleet_exec", "mapreduce", "fleet_read", "fleet_write", "fleet_git"] {
            assert!(relay.contains(&t.to_string()), "missing relay tool {t}");
        }
        // Enabling the relay only adds tools, never removes them.
        assert_eq!(relay.len(), local.len() + 6);
    }

    // --- format_result ------------------------------------------------------

    #[test]
    fn format_result_exec_includes_stderr_section_and_exit_code() {
        let r = CommandResult::Exec {
            stdout: "out".into(),
            stderr: "err".into(),
            exit_code: 2,
        };
        let s = format_result(&r);
        assert!(s.contains("out"));
        assert!(s.contains("--- stderr ---"));
        assert!(s.contains("err"));
        assert!(s.contains("[exit code: 2]"));
    }

    #[test]
    fn format_result_exec_without_stderr_has_no_stderr_section() {
        let r = CommandResult::Exec {
            stdout: "only".into(),
            stderr: String::new(),
            exit_code: 0,
        };
        let s = format_result(&r);
        assert!(!s.contains("--- stderr ---"));
        assert!(s.contains("[exit code: 0]"));
    }

    #[test]
    fn format_result_dir_marks_directories_with_slash() {
        let r = CommandResult::Dir {
            entries: vec![
                DirEntry { name: "sub".into(), is_dir: true, size: 4096, modified: None },
                DirEntry { name: "file.txt".into(), is_dir: false, size: 12, modified: None },
            ],
        };
        let s = format_result(&r);
        assert!(s.contains("sub/ (4096 bytes)"));
        assert!(s.contains("file.txt (12 bytes)"));
    }

    #[test]
    fn format_result_ok_and_git() {
        assert_eq!(format_result(&CommandResult::Ok), "OK");
        let g = CommandResult::Git { output: "pushed".into(), success: true };
        assert_eq!(format_result(&g), "pushed\n[success: true]");
    }

    // --- build_command ------------------------------------------------------

    #[test]
    fn build_command_exec_parses_optional_fields() {
        let h = handler();
        let cmd = h
            .build_command("exec", &args(json!({"command": "ls", "timeout_ms": 500})))
            .unwrap();
        match cmd {
            Command::Exec { command, timeout_ms, cwd } => {
                assert_eq!(command, "ls");
                assert_eq!(timeout_ms, Some(500));
                assert_eq!(cwd, None);
            }
            other => panic!("expected Exec, got {other:?}"),
        }
    }

    #[test]
    fn build_command_write_file_defaults_backup_true() {
        let h = handler();
        let cmd = h
            .build_command("write_file", &args(json!({"path": "/t", "content": "x"})))
            .unwrap();
        match cmd {
            Command::WriteFile { create_backup, .. } => assert!(create_backup),
            other => panic!("expected WriteFile, got {other:?}"),
        }
    }

    #[test]
    fn build_command_git_pull_defaults_remote_to_origin() {
        let h = handler();
        let cmd = h
            .build_command("git_pull", &args(json!({"repo": "/r"})))
            .unwrap();
        match cmd {
            Command::GitPull { remote, branch, .. } => {
                assert_eq!(remote, "origin");
                assert_eq!(branch, None);
            }
            other => panic!("expected GitPull, got {other:?}"),
        }
    }

    #[test]
    fn build_command_missing_required_field_errors() {
        let h = handler();
        // `exec` requires `command`.
        assert!(h.build_command("exec", &args(json!({}))).is_err());
    }

    #[test]
    fn build_command_invalid_mode_errors() {
        let h = handler();
        assert!(h
            .build_command("set_mode", &args(json!({"mode": "nope"})))
            .is_err());
    }

    #[test]
    fn build_command_unknown_tool_errors() {
        let h = handler();
        assert!(h.build_command("frobnicate", &args(json!({}))).is_err());
    }
}
