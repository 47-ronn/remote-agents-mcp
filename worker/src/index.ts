/**
 * Remote Agents Relay Server
 *
 * Cloudflare Worker that manages WebSocket connections between
 * MCP servers and remote agents.
 */

import { Room } from './room';
import { roomKey } from './roomkey';

export { Room };

export interface Env {
  ROOM: DurableObjectNamespace;
  /**
   * Optional server-wide auth token (parity with the Rust relay's `--token`).
   * When set, every connection's token (query string and auth frame) must
   * equal it — a mismatch is rejected outright. Set via
   * `wrangler secret put AUTH_TOKEN`. When unset, rooms are still
   * token-addressed (see roomKey): a wrong token cannot reach another token
   * group's room.
   */
  AUTH_TOKEN?: string;
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

      const token = url.searchParams.get('token') ?? '';

      // Optional server-wide token (AUTH_TOKEN secret): a mismatching
      // connection is rejected at the edge, before any DO is contacted.
      if (env.AUTH_TOKEN && token !== env.AUTH_TOKEN) {
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

      return roomObject.fetch(
        new Request(newUrl.toString(), {
          headers: request.headers,
        })
      );
    }

    // Room info endpoint: /api/room/:roomId — the caller MUST present the room
    // token (query ?token=): it both addresses the token-keyed room and, under
    // an AUTH_TOKEN secret, must equal it. Without a token this endpoint used
    // to hand out any room's roster unauthenticated.
    const infoMatch = url.pathname.match(/^\/api\/room\/([^/]+)$/);
    if (infoMatch) {
      const roomId = infoMatch[1];
      const token = url.searchParams.get('token') ?? '';
      if (!token) {
        return json({ error: 'token required to address a room' }, 401);
      }
      if (env.AUTH_TOKEN && token !== env.AUTH_TOKEN) {
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

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...corsHeaders(),
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
