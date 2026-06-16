import type { AgentInfo, ClientMessage, ServerMessage, Target, UdpOffer, UdpAnswer, UdpChannelResult } from './types';

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
        agents: this.agentSockets().map(([, a]) => a.agentInfo),
        mcp_clients: this.mcpSockets().length,
      });
    }

    return new Response('Not Found', { status: 404 });
  }

  private handleWebSocket(token: string, clientIp: string): Response {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.state.acceptWebSocket(server);
    // Stash the query token + client IP for use when the auth frame arrives.
    server.serializeAttachment({ token, clientIp } as Attachment);
    return new Response(null, { status: 101, webSocket: client });
  }

  // --- hibernation handlers ------------------------------------------------

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
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

  // --- message routing -----------------------------------------------------

  private async handleMessage(ws: WebSocket, msg: ClientMessage) {
    const att = this.att(ws);

    switch (msg.type) {
      case 'auth':
        return this.handleAuth(ws, msg);

      case 'list_agents':
        this.send(ws, {
          type: 'agent_list',
          agents: this.agentSockets()
            .map(([, a]) => a.agentInfo)
            .filter((a): a is AgentInfo => a !== undefined),
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
      // itself) so a host knows its surroundings immediately.
      const peers = this.agentSockets()
        .map(([, a]) => a.agentInfo)
        .filter((a): a is AgentInfo => !!a && a.id !== agentInfo.id);
      this.send(ws, { type: 'agent_list', agents: peers });

      // Announce the join to MCP clients and to the other agents.
      this.broadcastToMcp({ type: 'agent_joined', agent: agentInfo });
      this.broadcastToAgents({ type: 'agent_joined', agent: agentInfo }, agentInfo.id);
    }
  }

  private resolveTarget(target: Target): [WebSocket, Attachment][] {
    const agents = this.agentSockets();
    switch (target.type) {
      case 'agent':
        // Explicit target is delivered even to a send-only node (it replies
        // with its own --no-agent rejection).
        return agents.filter(([, a]) => a.agentInfo?.id === target.id);
      case 'all':
        // Broadcasts skip send-only peers (accepts_commands === false): they
        // never execute, so fanning out to them is pointless.
        return agents.filter(([, a]) => a.agentInfo?.accepts_commands !== false);
      case 'tagged':
        return agents.filter(
          ([, a]) =>
            a.agentInfo?.accepts_commands !== false &&
            a.agentInfo?.tags.some((t) => target.tags.includes(t))
        );
      case 'platform': {
        const fam = target.family.toLowerCase();
        return agents.filter(
          ([, a]) =>
            a.agentInfo?.accepts_commands !== false &&
            (a.agentInfo?.platform?.family.toLowerCase() === fam ||
              a.agentInfo?.os.toLowerCase() === fam)
        );
      }
    }
  }

  private handleDisconnect(ws: WebSocket) {
    const att = this.att(ws);
    if (att.agentInfo) {
      this.broadcastToMcp({ type: 'agent_left', agent_id: att.agentInfo.id });
      this.broadcastToAgents(
        { type: 'agent_left', agent_id: att.agentInfo.id },
        att.agentInfo.id
      );
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
