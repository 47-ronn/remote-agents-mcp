import type { AgentInfo, ClientMessage, ServerMessage, Target, UdpOffer, UdpAnswer, UdpChannelResult } from './types';

/** Reap a socket after this long with no inbound frame. Agents ping every 30s
 * (PING_INTERVAL in connection.rs), so 90s = three missed pings — a socket
 * whose TCP died silently (NAT/idle timeout) without Cloudflare firing
 * webSocketClose. This is what produces phantom "N connections under one
 * agent-id" duplicates, since send() failures don't reap a hibernating socket. */
export const STALE_MS = 90_000;
/** How often the reaper alarm runs while any socket is connected. */
export const REAP_INTERVAL_MS = 60_000;

/** Pure: which entries are stale (no frame seen within `staleMs`). Anything
 * without a `lastSeen` is treated as fresh (never reaped on absence alone). */
export function selectStale<T extends { lastSeen?: number }>(
  entries: T[],
  now: number,
  staleMs: number
): T[] {
  return entries.filter((e) => e.lastSeen !== undefined && now - e.lastSeen > staleMs);
}

/**
 * Capability score used to pick the most-capable socket of a host: a socket
 * that executes scores 1, an autonomous one +2 (so an autonomous+executing
 * socket wins). Kept in lock-step with the Rust relay's `routing::score`.
 */
export function scoreAgent(a: AgentInfo): number {
  return (a.accepts_commands !== false ? 1 : 0) + (a.autonomous ? 2 : 0);
}

/**
 * Collapse sockets sharing one agent-id into a single logical host. A machine
 * has a stable agent-id but may hold several connections at once (many
 * terminals / AI sessions on the same box); it is listed once, and a capability
 * (autonomous / accepts_commands) is present if ANY of its sockets has it. The
 * representative carries the most-capable socket's metadata with the merged
 * flags applied — so a host shown as autonomous always has an autonomous socket
 * to receive the task (lock-step with resolveTarget('agent')).
 */
export function dedupAgents(infos: AgentInfo[]): AgentInfo[] {
  const byId = new Map<string, AgentInfo>();
  const counts = new Map<string, number>();
  for (const info of infos) {
    counts.set(info.id, (counts.get(info.id) ?? 0) + 1);
    const prev = byId.get(info.id);
    const autonomous = (prev?.autonomous ?? false) || info.autonomous;
    const accepts =
      (prev ? prev.accepts_commands !== false : false) ||
      info.accepts_commands !== false;
    const rep = prev && scoreAgent(prev) >= scoreAgent(info) ? prev : info;
    byId.set(info.id, { ...rep, autonomous, accepts_commands: accepts });
  }
  // Surface how many live connections share this machine's agent-id. >1 means
  // several sockets (many terminals, or stale/mis-tokened processes) under one
  // id — the situation where a wrong-keyed socket can hijack routing, so the
  // panel can warn. The relay is E2E-blind to keys; it can only count sockets.
  for (const [id, rep] of byId) rep.connections = counts.get(id);
  return [...byId.values()];
}

/**
 * Resolve a `Target` to the connections that should receive a command, one per
 * logical host (a machine with several open terminals is contacted once, on its
 * most-capable socket). Pure over a candidate list of `{ info, item }`, where
 * `item` is whatever the caller wants back (the socket tuple).
 *
 * Broadcasts (all/tagged/platform) skip send-only peers (`accepts_commands ===
 * false`) and DEDUP by id (so a multi-terminal machine isn't hit N times). An
 * explicit `agent` target is delivered to the single most-capable socket — even
 * a send-only one (it self-rejects). Mirrors the Rust relay's `resolve_targets`.
 */
export function selectTargets<T>(
  candidates: { info: AgentInfo; item: T }[],
  target: Target
): T[] {
  const accepts = (i: AgentInfo) => i.accepts_commands !== false;

  if (target.type === 'agent') {
    const matches = candidates.filter((c) => c.info.id === target.id);
    if (matches.length <= 1) return matches.map((c) => c.item);
    const best = matches.reduce((b, c) =>
      scoreAgent(c.info) > scoreAgent(b.info) ? c : b
    );
    return [best.item];
  }

  const fam = target.type === 'platform' ? target.family.toLowerCase() : '';
  const matchesTarget = (i: AgentInfo): boolean => {
    if (!accepts(i)) return false;
    switch (target.type) {
      case 'all':
        return true;
      case 'tagged':
        return i.tags.some((t) => target.tags.includes(t));
      case 'platform':
        return i.platform?.family.toLowerCase() === fam || i.os.toLowerCase() === fam;
    }
  };

  // One most-capable socket per machine id.
  const byId = new Map<string, { info: AgentInfo; item: T }>();
  for (const c of candidates) {
    if (!matchesTarget(c.info)) continue;
    const prev = byId.get(c.info.id);
    if (!prev || scoreAgent(c.info) > scoreAgent(prev.info)) byId.set(c.info.id, c);
  }
  return [...byId.values()].map((c) => c.item);
}

/**
 * Room Durable Object — routes messages between agents and MCP clients.
 *
 * IMPORTANT: this uses the WebSocket *Hibernation* API (`acceptWebSocket`), so
 * the DO can be evicted from memory between messages while connections stay
 * alive. Therefore we keep NO session state in instance fields — it would be
 * lost on hibernation. Instead, all session state lives in each socket's
 * `serializeAttachment()` and is derived on demand from `getWebSockets()`.
 */
interface Attachment {
  sessionId?: string;
  agentInfo?: AgentInfo;
  token: string;
  clientIp?: string;
  /** Last time an inbound frame was seen on this socket (ms). Drives the
   * stale-socket reaper; updated on every message, set on accept. */
  lastSeen?: number;
}

export class Room implements DurableObject {
  // In-flight commands: request_id → originating session id, so a result routes
  // back to the peer that issued it (peer-model) instead of all controllers.
  // In-memory; if lost to hibernation, routeToOrigin falls back to broadcast.
  private pending = new Map<string, string>();

  constructor(
    private state: DurableObjectState,
    private env: unknown
  ) {}

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (request.headers.get('Upgrade') === 'websocket') {
      const token = url.searchParams.get('token') || '';
      // Cloudflare exposes the client's public IP here; reflected later via
      // your_endpoint for UDP hole-punching.
      const clientIp = request.headers.get('CF-Connecting-IP') || '';
      return this.handleWebSocket(token, clientIp);
    }

    if (url.pathname === '/info') {
      return Response.json({
        agents: this.dedupAgents(
          this.agentSockets()
            .map(([, a]) => a.agentInfo)
            .filter((a): a is AgentInfo => a !== undefined)
        ),
        mcp_clients: this.mcpSockets().length,
      });
    }

    return new Response('Not Found', { status: 404 });
  }

  private async handleWebSocket(token: string, clientIp: string): Promise<Response> {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.state.acceptWebSocket(server);
    // Stash the query token + client IP for use when the auth frame arrives;
    // seed lastSeen so a socket that never authenticates is still reapable.
    server.serializeAttachment({ token, clientIp, lastSeen: Date.now() } as Attachment);
    await this.ensureReaper();
    return new Response(null, { status: 101, webSocket: client });
  }

  /** Ensure the stale-socket reaper alarm is scheduled (idempotent). */
  private async ensureReaper() {
    if ((await this.state.storage.getAlarm()) === null) {
      await this.state.storage.setAlarm(Date.now() + REAP_INTERVAL_MS);
    }
  }

  /** Merge a patch into a socket's attachment, preserving existing fields. */
  private patchAtt(ws: WebSocket, patch: Partial<Attachment>) {
    try {
      ws.serializeAttachment({ ...this.att(ws), ...patch } as Attachment);
    } catch {
      // Socket already dead — nothing to persist.
    }
  }

  // --- hibernation handlers ------------------------------------------------

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    // Any inbound frame (including the 30s ping) proves the socket is alive.
    this.patchAtt(ws, { lastSeen: Date.now() });
    if (typeof message !== 'string') {
      this.sendError(ws, 'Binary messages not supported');
      return;
    }
    let msg: ClientMessage;
    try {
      msg = JSON.parse(message);
    } catch (e) {
      this.sendError(ws, `Invalid message: ${e}`);
      return;
    }
    await this.handleMessage(ws, msg);
  }

  async webSocketClose(ws: WebSocket) {
    this.handleDisconnect(ws);
  }

  async webSocketError(ws: WebSocket, error: unknown) {
    console.error('WebSocket error:', error);
    this.handleDisconnect(ws);
  }

  /** Periodic reaper: close sockets whose TCP died silently (no frame within
   * STALE_MS) so Cloudflare drops them, clearing phantom duplicate connections
   * that webSocketClose never fired for. Reschedules while any socket remains. */
  async alarm() {
    const now = Date.now();
    const entries = this.sockets().map(([ws, a]) => ({ ws, lastSeen: a.lastSeen }));
    for (const { ws } of selectStale(entries, now, STALE_MS)) {
      // Announce departure once (reads the live attachment), strip identity so a
      // socket that lingers a cycle isn't re-announced, then force the close.
      this.handleDisconnect(ws);
      this.patchAtt(ws, { agentInfo: undefined, sessionId: undefined });
      try {
        ws.close(1001, 'stale connection reaped');
      } catch {
        // Already dead — closing is best-effort.
      }
    }
    if (this.state.getWebSockets().length > 0) {
      await this.state.storage.setAlarm(now + REAP_INTERVAL_MS);
    }
  }

  // --- message routing -----------------------------------------------------

  private async handleMessage(ws: WebSocket, msg: ClientMessage) {
    const att = this.att(ws);

    switch (msg.type) {
      case 'auth':
        return this.handleAuth(ws, msg);

      case 'list_agents':
        this.send(ws, {
          type: 'agent_list',
          agents: this.dedupAgents(
            this.agentSockets()
              .map(([, a]) => a.agentInfo)
              .filter((a): a is AgentInfo => a !== undefined)
          ),
        });
        return;

      case 'command': {
        const targets = this.resolveTarget(msg.target);
        if (targets.length === 0) {
          this.send(ws, {
            type: 'command_error',
            request_id: msg.request_id,
            agent_id: '',
            error: 'No matching agents found',
          });
          return;
        }
        // Remember who asked, so the result(s) route back to them only.
        this.pending.set(msg.request_id, att.sessionId || '');
        for (const [sock] of targets) {
          this.send(sock, {
            type: 'command',
            request_id: msg.request_id,
            from_session: att.sessionId || '',
            payload: msg.payload,
          });
        }
        return;
      }

      case 'command_result':
        this.routeToOrigin(msg.request_id, {
          type: 'command_result',
          request_id: msg.request_id,
          agent_id: att.agentInfo?.id || 'unknown',
          result: msg.result,
        });
        return;

      case 'command_error':
        this.routeToOrigin(msg.request_id, {
          type: 'command_error',
          request_id: msg.request_id,
          agent_id: att.agentInfo?.id || 'unknown',
          error: msg.error,
        });
        return;

      case 'notify':
        this.broadcastToMcp({
          type: 'event',
          agent_id: att.agentInfo?.id || 'unknown',
          event: msg.event,
        });
        return;

      case 'ping':
        this.send(ws, { type: 'pong' });
        return;

      case 'close':
        this.handleDisconnect(ws);
        ws.close(1000, 'Goodbye');
        return;

      // UDP Signaling: forward offer to target session
      case 'udp_offer': {
        const offer = msg.offer as UdpOffer;
        const targetSession = offer.to_session;
        const targetSocket = this.findSocketBySession(targetSession);
        if (targetSocket) {
          this.send(targetSocket, {
            type: 'udp_offer',
            from_session: att.sessionId || '',
            offer,
          });
        }
        return;
      }

      // UDP Signaling: forward answer back to offering session
      case 'udp_answer': {
        const answer = msg.answer as UdpAnswer;
        // Broadcast to MCP clients (they track channels)
        this.broadcastToMcp({
          type: 'udp_answer',
          from_session: att.sessionId || '',
          answer,
        });
        return;
      }

      // UDP Signaling: forward channel result
      case 'udp_result': {
        const result = msg.result as UdpChannelResult;
        this.broadcastToMcp({
          type: 'udp_result',
          from_session: att.sessionId || '',
          result,
        });
        return;
      }

      // Agent is updating its info (e.g. after mode change)
      case 'update_agent': {
        const newInfo = msg.agent_info;
        if (!att.agentInfo || att.agentInfo.id !== newInfo.id) {
          // Reject updates from sessions without matching agent_info
          return;
        }
        // Preserve session_id and connected_at from the current attachment
        const updatedInfo: AgentInfo = { 
          ...newInfo, 
          session_id: att.sessionId,
          connected_at: att.agentInfo.connected_at,
        };
        this.patchAtt(ws, { agentInfo: updatedInfo });
        
        // Broadcast the update using the updatedInfo directly (not reading
        // from sockets which may not reflect the patch yet in all code paths)
        this.broadcastToMcp({ type: 'agent_joined', agent: updatedInfo });
        this.broadcastToAgents({ type: 'agent_joined', agent: updatedInfo }, newInfo.id);
        return;
      }
    }
  }

  private handleAuth(ws: WebSocket, msg: Extract<ClientMessage, { type: 'auth' }>) {
    const att = this.att(ws);

    // The auth token must equal the connection's query token (our clients send
    // the same value in both). An empty/absent query token only admits an empty
    // auth token — it no longer admits an arbitrary one (closed open-access hole).
    if (msg.token !== att.token) {
      this.send(ws, { type: 'auth_failed', reason: 'Invalid token' });
      ws.close(1008, 'Invalid token');
      return;
    }

    const sessionId = crypto.randomUUID();
    // Store session_id in agentInfo for later use
    const agentInfo = msg.agent_info ? { ...msg.agent_info, session_id: sessionId } : undefined;
    ws.serializeAttachment({
      token: att.token,
      sessionId,
      agentInfo,
      clientIp: att.clientIp,
      lastSeen: Date.now(),
    } as Attachment);

    // auth_ok MUST be the first frame the client sees (the agent reads it as
    // the auth response); peer-awareness frames follow. Keep this ordering in
    // lock-step with the Rust relay (crates/relay/src/handler.rs).
    this.send(ws, { type: 'auth_ok', session_id: sessionId });

    // Reflect the client's observed public IP for UDP hole-punching (port is
    // the client's own UDP port, so 0 here). Mirrors the Rust relay.
    if (att.clientIp) {
      this.send(ws, { type: 'your_endpoint', endpoint: { addr: att.clientIp, port: 0 } });
    }

    if (agentInfo) {
      // Tell the newcomer who its peers are (everyone already here, minus
      // itself) so a host knows its surroundings immediately. Collapsed by id:
      // one machine may hold several connections (many terminals), but it is
      // one logical peer.
      const peers = this.dedupAgents(
        this.agentSockets()
          .map(([, a]) => a.agentInfo)
          .filter((a): a is AgentInfo => !!a && a.id !== agentInfo.id)
      );
      this.send(ws, { type: 'agent_list', agents: peers });

      // Announce the join to MCP clients and to the other agents, carrying the
      // host's MERGED capabilities (this id may already have other sockets), so
      // controller caches don't get downgraded by a less-capable connection.
      const merged =
        this.dedupAgents(
          this.agentSockets()
            .map(([, a]) => a.agentInfo)
            .filter((a): a is AgentInfo => !!a && a.id === agentInfo.id)
        )[0] ?? agentInfo;
      this.broadcastToMcp({ type: 'agent_joined', agent: merged });
      this.broadcastToAgents({ type: 'agent_joined', agent: merged }, agentInfo.id);
    }
  }

  private resolveTarget(target: Target): [WebSocket, Attachment][] {
    // Pair each identified socket with its agent_info, then delegate to the
    // pure, unit-tested selector (one most-capable connection per machine).
    const candidates = this.agentSockets()
      .filter(([, a]) => a.agentInfo)
      .map(([ws, a]) => ({ info: a.agentInfo as AgentInfo, item: [ws, a] as [WebSocket, Attachment] }));
    return selectTargets(candidates, target);
  }

  private handleDisconnect(ws: WebSocket) {
    const att = this.att(ws);
    if (att.agentInfo) {
      // Only announce departure if no OTHER live socket still holds this id.
      // On a reconnect/replacement the new socket is registered first and then
      // the old one is closed — suppressing a spurious agent_left that would
      // otherwise race the agent_joined and make the peer flicker out.
      const id = att.agentInfo.id;
      const stillPresent = this.agentSockets().some(
        ([sock, a]) => sock !== ws && a.agentInfo?.id === id
      );
      if (!stillPresent) {
        this.broadcastToMcp({ type: 'agent_left', agent_id: id });
        this.broadcastToAgents({ type: 'agent_left', agent_id: id }, id);
      }
    }
    // Drop any in-flight requests this session initiated.
    if (att.sessionId) {
      for (const [rid, sid] of this.pending) {
        if (sid === att.sessionId) this.pending.delete(rid);
      }
    }
  }

  /** Route a result/error back to the initiating session; fall back to
   * broadcasting to controllers if the origin is unknown or has disconnected. */
  private routeToOrigin(requestId: string, msg: ServerMessage) {
    const origin = this.pending.get(requestId);
    if (origin) {
      const sock = this.findSocketBySession(origin);
      if (sock) {
        this.send(sock, msg);
        return;
      }
    }
    this.broadcastToMcp(msg);
  }

  // --- helpers (state derived from live sockets, hibernation-safe) ---------

  private att(ws: WebSocket): Attachment {
    return (ws.deserializeAttachment() as Attachment) || { token: '' };
  }

  private sockets(): [WebSocket, Attachment][] {
    return this.state.getWebSockets().map((ws) => [ws, this.att(ws)]);
  }

  private agentSockets(): [WebSocket, Attachment][] {
    // Peer model: identified peers (those carrying agent_info) are the agents.
    return this.sockets().filter(([, a]) => a.agentInfo);
  }

  /** Collapse multiple live sockets that share one agent-id into a single
   * logical host (delegates to the pure, unit-tested `dedupAgents`). */
  private dedupAgents(infos: AgentInfo[]): AgentInfo[] {
    return dedupAgents(infos);
  }

  private mcpSockets(): [WebSocket, Attachment][] {
    // Anonymous observers (no agent_info) — e.g. browser stats/control clients.
    return this.sockets().filter(([, a]) => !a.agentInfo);
  }

  private findSocketBySession(sessionId: string): WebSocket | undefined {
    const found = this.sockets().find(([, a]) => a.sessionId === sessionId);
    return found ? found[0] : undefined;
  }

  private send(ws: WebSocket, msg: ServerMessage) {
    try {
      ws.send(JSON.stringify(msg));
    } catch (e) {
      console.error('Failed to send message:', e);
    }
  }

  private sendError(ws: WebSocket, message: string) {
    this.send(ws, { type: 'error', message });
  }

  private broadcastToMcp(msg: ServerMessage) {
    for (const [sock] of this.mcpSockets()) {
      this.send(sock, msg);
    }
  }

  /** Broadcast to all agents, optionally skipping one id (e.g. the trigger). */
  private broadcastToAgents(msg: ServerMessage, exceptId?: string) {
    for (const [sock, a] of this.agentSockets()) {
      if (exceptId && a.agentInfo?.id === exceptId) continue;
      this.send(sock, msg);
    }
  }
}
