// Types matching the Rust shared crate

export type AgentMode = 'plan' | 'edit' | 'bypass' | 'disabled';
export type ClientRole = 'mcp' | 'agent';

export interface PlatformInfo {
  family: string;
  arch: string;
  distro?: string;
  kernel?: string;
  shell?: string;
}

export interface AgentInfo {
  id: string;
  name: string;
  mode: AgentMode;
  os: string;
  arch: string;
  hostname: string;
  tags: string[];
  platform?: PlatformInfo;
  autonomous?: boolean;
  /** Whether this peer executes commands from others (false = --no-agent
   * send-only). Absent (legacy) is treated as true. */
  accepts_commands?: boolean;
  /** Newer published version available for this host, if any. */
  update_available?: string;
  connected_at: number;
  session_id?: string;
}

export type TaskStatus = 'queued' | 'running' | 'done' | 'failed';

// Unsolicited agent → MCP events (forwarded opaquely by the relay).
export type AgentEvent = {
  event: 'task_completed';
  task_id: string;
  status: TaskStatus;
};

export interface AutonomousTask {
  id: string;
  prompt: string;
  status: TaskStatus;
  result?: string;
  error?: string;
  created_at: number;
  started_at?: number;
  finished_at?: number;
  exit_code?: number;
}

export type Target =
  | { type: 'agent'; id: string }
  | { type: 'all' }
  | { type: 'tagged'; tags: string[] }
  | { type: 'platform'; family: string };

export interface DirEntry {
  name: string;
  is_dir: boolean;
  size: number;
  modified?: number;
}

export interface GitStatus {
  branch: string;
  clean: boolean;
  ahead: number;
  behind: number;
  staged: string[];
  modified: string[];
  untracked: string[];
}

export interface ScheduledTask {
  name: string;
  cron: string;
  command: string;
  last_run?: number;
  run_count: number;
}

// Commands
export type Command =
  | { cmd: 'exec'; command: string; timeout_ms?: number; cwd?: string }
  | { cmd: 'read_file'; path: string }
  | { cmd: 'write_file'; path: string; content: string; create_backup?: boolean }
  | { cmd: 'list_dir'; path: string; pattern?: string }
  | { cmd: 'git_status'; repo: string }
  | { cmd: 'git_pull'; repo: string; remote?: string; branch?: string }
  | { cmd: 'git_commit'; repo: string; message: string; files: string[] }
  | { cmd: 'git_push'; repo: string; remote?: string; branch?: string }
  | { cmd: 'schedule_add'; name: string; cron: string; command: string }
  | { cmd: 'schedule_remove'; name: string }
  | { cmd: 'schedule_list' }
  | { cmd: 'task_dispatch'; prompt: string }
  | { cmd: 'task_get'; id: string }
  | { cmd: 'task_list' }
  | { cmd: 'set_mode'; mode: AgentMode }
  | { cmd: 'get_info' };

// Command Results
export type CommandResult =
  | { result_type: 'exec'; stdout: string; stderr: string; exit_code: number }
  | { result_type: 'file'; content: string; size: number }
  | { result_type: 'dir'; entries: DirEntry[] }
  | { result_type: 'git_status'; status: GitStatus }
  | { result_type: 'git'; output: string; success: boolean }
  | { result_type: 'info'; info: AgentInfo }
  | { result_type: 'mode'; mode: AgentMode }
  | { result_type: 'schedule'; tasks: ScheduledTask[] }
  | { result_type: 'task_queued'; id: string }
  | { result_type: 'task'; task: AutonomousTask }
  | { result_type: 'task_list'; tasks: AutonomousTask[] }
  | { result_type: 'ok' };

// UDP Signaling types
// Matches the Rust `Endpoint` wire format (serde field is `addr`, not `ip`).
export interface Endpoint {
  addr: string;
  port: number;
}

export interface UdpOffer {
  channel_id: string;
  from_session: string;
  to_session: string;
  local_endpoint: Endpoint;
  public_endpoint?: Endpoint;
  nonce: number[];
}

export interface UdpAnswer {
  channel_id: string;
  from_session: string;
  local_endpoint: Endpoint;
  public_endpoint?: Endpoint;
  nonce: number[];
  accepted: boolean;
}

export interface UdpChannelResult {
  channel_id: string;
  success: boolean;
  error?: string;
}

// Client → Relay Messages
//
// `payload` (command) and `result` are end-to-end encrypted base64 envelopes:
// the relay forwards them opaquely and never sees plaintext. Only routing
// metadata and error strings are in the clear.
export type ClientMessage =
  | { type: 'auth'; room: string; token: string; role: ClientRole; agent_info?: AgentInfo }
  | { type: 'list_agents' }
  | { type: 'command'; request_id: string; target: Target; payload: string }
  | { type: 'command_result'; request_id: string; result: string }
  | { type: 'command_error'; request_id: string; error: string }
  | { type: 'notify'; event: AgentEvent }
  | { type: 'udp_offer'; offer: UdpOffer }
  | { type: 'udp_answer'; answer: UdpAnswer }
  | { type: 'udp_result'; result: UdpChannelResult }
  | { type: 'ping' }
  | { type: 'close' };

// Relay → Client Messages
export type ServerMessage =
  | { type: 'auth_ok'; session_id: string }
  | { type: 'auth_failed'; reason: string }
  | { type: 'agent_list'; agents: AgentInfo[] }
  | { type: 'agent_joined'; agent: AgentInfo }
  | { type: 'agent_left'; agent_id: string }
  | { type: 'agent_mode_changed'; agent_id: string; mode: AgentMode }
  | { type: 'command'; request_id: string; from_session: string; payload: string }
  | { type: 'command_result'; request_id: string; agent_id: string; result: string }
  | { type: 'command_error'; request_id: string; agent_id: string; error: string }
  | { type: 'event'; agent_id: string; event: AgentEvent }
  | { type: 'udp_offer'; from_session: string; offer: UdpOffer }
  | { type: 'udp_answer'; from_session: string; answer: UdpAnswer }
  | { type: 'udp_result'; from_session: string; result: UdpChannelResult }
  | { type: 'your_endpoint'; endpoint: Endpoint }
  | { type: 'pong' }
  | { type: 'error'; message: string };

// Internal session tracking
export interface Session {
  id: string;
  role: ClientRole;
  agentInfo?: AgentInfo;
  ws: WebSocket;
}
