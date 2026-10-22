/**
 * Remote Agents Relay Server
 *
 * Cloudflare Worker that manages WebSocket connections between
 * MCP servers and remote agents.
 */

import { Room } from './room';
import { roomKey } from './roomkey';
import { constTimeEq, Gate } from './gate';

export { Room, Gate };

export interface Env {
  ROOM: DurableObjectNamespace;
  /**
   * Optional per-IP abuse gate (connection budget + auth-failure bans),
   * implemented by the `Gate` Durable Object. Optional so deployments that
   * predate the class keep working unthrottled; add the binding + migration
   * (see wrangler.jsonc) to enable.
   */
  GATE?: DurableObjectNamespace;
  /**
   * Optional server-wide auth token (parity with the Rust relay's `--token`).
   * When set, every connection's token (header/query and auth frame) must
   * equal it — a mismatch is rejected outright. Set via
   * `wrangler secret put AUTH_TOKEN`. When unset, rooms are still
   * token-addressed (see roomKey): a wrong token cannot reach another token
   * group's room.
   */
  AUTH_TOKEN?: string;
  /**
   * Minimum room-token length when AUTH_TOKEN is unset (the room token is then
   * the room's only gate, and a short one is guessable online). Default 32;
   * set the var to "0" to disable for token-less dev rooms. Parity with the
   * Rust relay's `--min-token-len`.
   */
  MIN_TOKEN_LEN?: string;
}

/** Default/enforced minimum room-token length when AUTH_TOKEN is unset. */
function minTokenLen(env: Env): number {
  const n = parseInt(env.MIN_TOKEN_LEN ?? '32', 10);
  return Number.isFinite(n) && n >= 0 ? n : 32;
}

/** The room token from the Authorization header (preferred — keeps the
 *  secret out of URLs/proxy logs) or the legacy query param. */
function roomToken(request: Request, url: URL): string {
  const h = request.headers.get('authorization');
  if (h && h.startsWith('Bearer ')) return h.slice('Bearer '.length);
  const sp = request.headers.get('sec-websocket-protocol');
  if (sp) {
    // `bearer.<base64url(token)>`: the only header a browser WebSocket can
    // set, so the panel can keep its token out of the URL.
    const bearer = sp.split(',').map((p) => p.trim()).find((p) => p.startsWith('bearer.'));
    if (bearer) {
      try {
        return atob(bearer.slice('bearer.'.length).replace(/-/g, '+').replace(/_/g, '/'));
      } catch {
        // fall through to the query param
      }
    }
  }
  return url.searchParams.get('token') ?? '';
}

/** Ask this IP's Gate DO to gate + record one connection attempt. Fail-open
 *  (null) when the binding is absent or the DO errors — the limiter is
 *  defense-in-depth, not a hard dependency. */
async function gateAttempt(
  env: Env,
  ip: string
): Promise<{ allowed: boolean; retryAfterSec: number } | null> {
  if (!env.GATE) return null;
  try {
    const id = env.GATE.idFromName('ip:' + ip);
    const r = await env.GATE.get(id).fetch('https://gate/attempt', { method: 'POST' });
    return (await r.json()) as { allowed: boolean; retryAfterSec: number };
  } catch {
    return null;
  }
}

/** Record one failed authentication against this IP's Gate DO. */
async function gateFailure(
  env: Env,
  ip: string
): Promise<{ banned: boolean; retryAfterSec: number } | null> {
  if (!env.GATE) return null;
  try {
    const id = env.GATE.idFromName('ip:' + ip);
    const r = await env.GATE.get(id).fetch('https://gate/auth-failure', { method: 'POST' });
    return (await r.json()) as { banned: boolean; retryAfterSec: number };
  } catch {
    return null;
  }
}

function clientIp(request: Request): string {
  // CF-Connecting-IP is set by Cloudflare and not client-forgeable.
  return request.headers.get('CF-Connecting-IP') || 'unknown';
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: corsHeaders(),
      });
    }

    // Health check
    if (url.pathname === '/health' || url.pathname === '/') {
      return new Response(
        JSON.stringify({
          status: 'ok',
          service: 'remote-agents-relay',
          timestamp: Date.now(),
        }),
        {
          status: 200,
          headers: {
            'Content-Type': 'application/json',
            ...corsHeaders(),
          },
        }
      );
    }

    // Room WebSocket endpoint: /ws/room/:roomId
    const wsMatch = url.pathname.match(/^\/ws\/room\/([^/]+)$/);
    if (wsMatch) {
      const roomId = wsMatch[1];

      // Validate WebSocket upgrade
      if (request.headers.get('Upgrade') !== 'websocket') {
        return json({ error: 'Expected WebSocket' }, 426);
      }

      // Abuse gate first: per-IP connection budget + active bans are enforced
      // BEFORE the auth handshake, so a guess-flood never reaches it.
      const gate = await gateAttempt(env, clientIp(request));
      if (gate && !gate.allowed) {
        return json({ error: 'rate limited; retry later' }, 429, {
          'Retry-After': String(Math.max(1, gate.retryAfterSec)),
        });
      }

      const token = roomToken(request, url);

      // Minimum room-token length (room-token mode only): a short token is
      // the room's only gate and is guessable online.
      if (!env.AUTH_TOKEN) {
        const min = minTokenLen(env);
        if (min > 0 && token.length < min) {
          return json(
            { error: `room token too short: ${token.length} chars < ${min} required (set MIN_TOKEN_LEN=0 for dev/tokenless rooms)` },
            401
          );
        }
      }

      // Optional server-wide token (AUTH_TOKEN secret): a mismatching
      // connection is rejected at the edge, before any DO is contacted, and
      // counts toward the IP's auth-failure ban.
      if (env.AUTH_TOKEN && !constTimeEq(token, env.AUTH_TOKEN)) {
        const f = await gateFailure(env, clientIp(request));
        if (f?.banned) {
          return json({ error: 'rate limited; retry later' }, 429, {
            'Retry-After': String(Math.max(1, f.retryAfterSec)),
          });
        }
        return json({ error: 'invalid token' }, 401);
      }

      // Rooms are token-addressed: the DO name is hash(room, token), so a
      // wrong-token host lands in its own (empty) DO and can never reach the
      // real room's roster. Clients are unaffected — they already send room +
      // token consistently on every connection.
      const key = await roomKey(roomId, token);
      const roomObjectId = env.ROOM.idFromName(key);
      const roomObject = env.ROOM.get(roomObjectId);

      // Forward the WebSocket request to the Room
      const newUrl = new URL(request.url);
      newUrl.pathname = '/ws';
      // Strip the `bearer.*` subprotocol before forwarding to the DO: the DO's
      // WebSocketPair never negotiates subprotocols, and a browser that
      // offered one fails the handshake unless the server selects it. The
      // token itself already travels via the resolved `token` above.
      const fwdHeaders = new Headers(request.headers);
      fwdHeaders.delete('sec-websocket-protocol');

      return roomObject.fetch(
        new Request(newUrl.toString(), {
          headers: fwdHeaders,
        })
      );
    }

    // Room info endpoint: /api/room/:roomId — the caller MUST present the room
    // token (Authorization header or query ?token=): it both addresses the
    // token-keyed room and, under an AUTH_TOKEN secret, must equal it. Without
    // a token this endpoint used to hand out any room's roster unauthenticated.
    const infoMatch = url.pathname.match(/^\/api\/room\/([^/]+)$/);
    if (infoMatch) {
      // The roster's empty-vs-populated answer doubles as a guess-verification
      // oracle, so info calls share the per-IP connection budget.
      const gate = await gateAttempt(env, clientIp(request));
      if (gate && !gate.allowed) {
        return json({ error: 'rate limited; retry later' }, 429, {
          'Retry-After': String(Math.max(1, gate.retryAfterSec)),
        });
      }

      const roomId = infoMatch[1];
      const token = roomToken(request, url);
      if (!token) {
        return json({ error: 'token required to address a room' }, 401);
      }
      if (!env.AUTH_TOKEN) {
        const min = minTokenLen(env);
        if (min > 0 && token.length < min) {
          return json(
            { error: `room token too short: ${token.length} chars < ${min} required (set MIN_TOKEN_LEN=0 for dev/tokenless rooms)` },
            401
          );
        }
      }
      if (env.AUTH_TOKEN && !constTimeEq(token, env.AUTH_TOKEN)) {
        const f = await gateFailure(env, clientIp(request));
        if (f?.banned) {
          return json({ error: 'rate limited; retry later' }, 429, {
            'Retry-After': String(Math.max(1, f.retryAfterSec)),
          });
        }
        return json({ error: 'invalid token' }, 401);
      }

      const key = await roomKey(roomId, token);
      const roomObjectId = env.ROOM.idFromName(key);
      const roomObject = env.ROOM.get(roomObjectId);

      const infoResponse = await roomObject.fetch(
        new Request('https://internal/info')
      );

      const data = await infoResponse.json();
      return json(data);
    }

    // List rooms (for debugging)
    if (url.pathname === '/api/rooms') {
      return json({ message: 'Room listing not implemented' });
    }

    return json({ error: 'Not Found' }, 404);
  },
};

function json(data: unknown, status = 200, extra: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...corsHeaders(),
      ...extra,
    },
  });
}

function corsHeaders(): Record<string, string> {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  };
}
