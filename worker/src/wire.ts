// Wire adapter: protobuf <-> the relay's internal JS message types (./types).
//
// The relay's routing logic (room.ts) keeps working on the hand-written
// discriminated unions keyed by `type`; this module is the only place that
// touches the protobuf encoding, mirroring the Rust `proto_convert.rs` strategy.
// The relay only ever DECODES a ClientMessage and ENCODES a ServerMessage, so
// only those two directions are implemented here.
//
// Encrypted `payload`/`result` are opaque: `bytes` on the wire, carried as a
// base64 string in JS-land (same as the Rust relay's domain representation), so
// room.ts forwards them untouched.

import * as pb from "./gen/remote_agents";
import type {
  AgentEvent,
  AgentInfo,
  AgentMode,
  ClientMessage,
  Endpoint,
  PlatformInfo,
  ServerMessage,
  Target,
  UdpAnswer,
  UdpChannelResult,
  UdpOffer,
} from "./types";

// ---- base64 (Uint8Array <-> string), for the opaque encrypted payloads ------
function bytesToB64(b: Uint8Array): string {
  let s = "";
  for (let i = 0; i < b.length; i++) s += String.fromCharCode(b[i]);
  return btoa(s);
}
function b64ToBytes(s: string): Uint8Array {
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// ---- enums ------------------------------------------------------------------
const MODE_FROM_PB: AgentMode[] = ["plan", "edit", "bypass", "disabled"];
function modeFromPb(m: pb.AgentMode): AgentMode {
  return MODE_FROM_PB[m as number] ?? "plan";
}
function modeToPb(m: AgentMode | undefined): pb.AgentMode {
  switch (m) {
    case "edit":
      return pb.AgentMode.AGENT_MODE_EDIT;
    case "bypass":
      return pb.AgentMode.AGENT_MODE_BYPASS;
    case "disabled":
      return pb.AgentMode.AGENT_MODE_DISABLED;
    default:
      return pb.AgentMode.AGENT_MODE_PLAN;
  }
}

// ---- nested: AgentInfo / PlatformInfo --------------------------------------
function platformFromPb(p: pb.PlatformInfo | undefined): PlatformInfo | undefined {
  if (!p) return undefined;
  return { family: p.family, arch: p.arch, distro: p.distro, kernel: p.kernel, shell: p.shell };
}
function agentInfoFromPb(a: pb.AgentInfo): AgentInfo {
  return {
    id: a.id,
    name: a.name,
    mode: modeFromPb(a.mode),
    os: a.os,
    arch: a.arch,
    hostname: a.hostname,
    tags: a.tags ?? [],
    platform: platformFromPb(a.platform),
    autonomous: a.autonomous,
    accepts_commands: a.acceptsCommands,
    version: a.version,
    update_available: a.updateAvailable,
    connections: a.connections,
    connected_at: a.connectedAt,
    session_id: a.sessionId,
  };
}
function agentInfoToPb(a: AgentInfo): pb.AgentInfo {
  return pb.AgentInfo.fromPartial({
    id: a.id,
    name: a.name,
    mode: modeToPb(a.mode),
    os: a.os,
    arch: a.arch,
    hostname: a.hostname,
    tags: a.tags ?? [],
    platform: a.platform
      ? {
          family: a.platform.family,
          arch: a.platform.arch,
          distro: a.platform.distro,
          kernel: a.platform.kernel,
          shell: a.platform.shell,
        }
      : undefined,
    autonomous: a.autonomous ?? false,
    acceptsCommands: a.accepts_commands ?? true,
    connectedAt: a.connected_at,
    version: a.version ?? "",
    sessionId: a.session_id,
    updateAvailable: a.update_available,
    connections: a.connections,
  });
}

// ---- nested: Endpoint / UDP -------------------------------------------------
function endpointFromPb(e: pb.Endpoint | undefined): Endpoint {
  return { addr: e?.addr ?? "", port: e?.port ?? 0 };
}
function udpOfferFromPb(o: pb.UdpOffer): UdpOffer {
  return {
    channel_id: o.channelId,
    from_session: o.fromSession,
    to_session: o.toSession,
    local_endpoint: endpointFromPb(o.localEndpoint),
    local_candidates: (o.localCandidates ?? []).map(endpointFromPb),
    public_endpoint: o.publicEndpoint ? endpointFromPb(o.publicEndpoint) : undefined,
    nonce: Array.from(o.nonce),
  };
}
function udpAnswerFromPb(a: pb.UdpAnswer): UdpAnswer {
  return {
    channel_id: a.channelId,
    from_session: a.fromSession,
    local_endpoint: endpointFromPb(a.localEndpoint),
    local_candidates: (a.localCandidates ?? []).map(endpointFromPb),
    public_endpoint: a.publicEndpoint ? endpointFromPb(a.publicEndpoint) : undefined,
    nonce: Array.from(a.nonce),
    accepted: a.accepted,
  };
}
function udpResultFromPb(r: pb.UdpChannelResult): UdpChannelResult {
  return { channel_id: r.channelId, success: r.success, error: r.error };
}
function udpOfferToPb(o: UdpOffer): pb.UdpOffer {
  return pb.UdpOffer.fromPartial({
    channelId: o.channel_id,
    fromSession: o.from_session,
    toSession: o.to_session,
    localEndpoint: o.local_endpoint,
    localCandidates: o.local_candidates ?? [],
    publicEndpoint: o.public_endpoint,
    nonce: Uint8Array.from(o.nonce ?? []),
  });
}
function udpAnswerToPb(a: UdpAnswer): pb.UdpAnswer {
  return pb.UdpAnswer.fromPartial({
    channelId: a.channel_id,
    fromSession: a.from_session,
    localEndpoint: a.local_endpoint,
    localCandidates: a.local_candidates ?? [],
    publicEndpoint: a.public_endpoint,
    nonce: Uint8Array.from(a.nonce ?? []),
    accepted: a.accepted,
  });
}
function udpResultToPb(r: UdpChannelResult): pb.UdpChannelResult {
  return pb.UdpChannelResult.fromPartial({
    channelId: r.channel_id,
    success: r.success,
    error: r.error,
  });
}

// ---- nested: AgentEvent / Target -------------------------------------------
function agentEventFromPb(e: pb.AgentEvent): AgentEvent {
  // Only TaskCompleted exists today.
  const tc = e.kind?.$case === "taskCompleted" ? e.kind.taskCompleted : undefined;
  return {
    event: "task_completed",
    task_id: tc?.taskId ?? "",
    status: (["queued", "running", "done", "failed"][tc?.status ?? 0] ?? "queued") as AgentEvent["status"],
  };
}
function agentEventToPb(e: AgentEvent): pb.AgentEvent {
  const status =
    e.status === "running"
      ? pb.TaskStatus.TASK_STATUS_RUNNING
      : e.status === "done"
        ? pb.TaskStatus.TASK_STATUS_DONE
        : e.status === "failed"
          ? pb.TaskStatus.TASK_STATUS_FAILED
          : pb.TaskStatus.TASK_STATUS_QUEUED;
  return pb.AgentEvent.fromPartial({
    kind: { $case: "taskCompleted", taskCompleted: { taskId: e.task_id, status } },
  });
}
function targetFromPb(t: pb.Target | undefined): Target {
  switch (t?.kind?.$case) {
    case "agent":
      return { type: "agent", id: t.kind.agent.id };
    case "tagged":
      return { type: "tagged", tags: t.kind.tagged.tags ?? [] };
    case "platform":
      return { type: "platform", family: t.kind.platform.family };
    default:
      return { type: "all" };
  }
}

// ============================================================================
// Public API
// ============================================================================

/** Decode a binary ClientMessage frame into the relay's internal JS type. */
export function decodeClientMessage(buf: Uint8Array): ClientMessage {
  const m = pb.ClientMessage.decode(buf);
  const k = m.kind;
  switch (k?.$case) {
    case "auth":
      return {
        type: "auth",
        room: k.auth.room,
        token: k.auth.token,
        agent_info: k.auth.agentInfo ? agentInfoFromPb(k.auth.agentInfo) : undefined,
      };
    case "listAgents":
      return { type: "list_agents" };
    case "command":
      return {
        type: "command",
        request_id: k.command.requestId,
        target: targetFromPb(k.command.target),
        payload: bytesToB64(k.command.payload),
      };
    case "commandResult":
      return {
        type: "command_result",
        request_id: k.commandResult.requestId,
        result: bytesToB64(k.commandResult.result),
      };
    case "commandError":
      return { type: "command_error", request_id: k.commandError.requestId, error: k.commandError.error };
    case "notify":
      return { type: "notify", event: agentEventFromPb(k.notify.event!) };
    case "udpOffer":
      return { type: "udp_offer", offer: udpOfferFromPb(k.udpOffer) };
    case "udpAnswer":
      return { type: "udp_answer", answer: udpAnswerFromPb(k.udpAnswer) };
    case "udpResult":
      return { type: "udp_result", result: udpResultFromPb(k.udpResult) };
    case "ping":
      return { type: "ping" };
    case "close":
      return { type: "close" };
    case "updateAgent":
      return { type: "update_agent", agent_info: agentInfoFromPb(k.updateAgent.agentInfo!) };
    default:
      throw new Error("empty or unknown ClientMessage");
  }
}

/** Encode the relay's internal ServerMessage into a binary frame. */
export function encodeServerMessage(msg: ServerMessage): Uint8Array {
  let kind: pb.ServerMessage["kind"];
  switch (msg.type) {
    case "auth_ok":
      kind = { $case: "authOk", authOk: { sessionId: msg.session_id } };
      break;
    case "auth_failed":
      kind = { $case: "authFailed", authFailed: { reason: msg.reason } };
      break;
    case "agent_list":
      kind = { $case: "agentList", agentList: { agents: msg.agents.map(agentInfoToPb) } };
      break;
    case "agent_joined":
      kind = { $case: "agentJoined", agentJoined: { agent: agentInfoToPb(msg.agent) } };
      break;
    case "agent_left":
      kind = { $case: "agentLeft", agentLeft: { agentId: msg.agent_id } };
      break;
    case "agent_mode_changed":
      kind = {
        $case: "agentModeChanged",
        agentModeChanged: { agentId: msg.agent_id, mode: modeToPb(msg.mode) },
      };
      break;
    case "command":
      kind = {
        $case: "command",
        command: { requestId: msg.request_id, fromSession: msg.from_session, payload: b64ToBytes(msg.payload) },
      };
      break;
    case "command_result":
      kind = {
        $case: "commandResult",
        commandResult: { requestId: msg.request_id, agentId: msg.agent_id, result: b64ToBytes(msg.result) },
      };
      break;
    case "command_error":
      kind = {
        $case: "commandError",
        commandError: { requestId: msg.request_id, agentId: msg.agent_id, error: msg.error },
      };
      break;
    case "event":
      kind = { $case: "event", event: { agentId: msg.agent_id, event: agentEventToPb(msg.event) } };
      break;
    case "udp_offer":
      kind = { $case: "udpOffer", udpOffer: { fromSession: msg.from_session, offer: udpOfferToPb(msg.offer) } };
      break;
    case "udp_answer":
      kind = {
        $case: "udpAnswer",
        udpAnswer: { fromSession: msg.from_session, answer: udpAnswerToPb(msg.answer) },
      };
      break;
    case "udp_result":
      kind = {
        $case: "udpResult",
        udpResult: { fromSession: msg.from_session, result: udpResultToPb(msg.result) },
      };
      break;
    case "your_endpoint":
      kind = { $case: "yourEndpoint", yourEndpoint: { endpoint: msg.endpoint } };
      break;
    case "pong":
      kind = { $case: "pong", pong: {} };
      break;
    case "error":
      kind = { $case: "error", error: { message: msg.message } };
      break;
    default:
      throw new Error("unknown ServerMessage");
  }
  return pb.ServerMessage.encode(pb.ServerMessage.fromPartial({ kind })).finish();
}
