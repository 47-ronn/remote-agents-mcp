/**
 * Remote Agents Relay Server
 *
 * Cloudflare Worker that manages WebSocket connections between
 * MCP servers and remote agents.
 */

import { Room } from './room';

export { Room };

export interface Env {
  ROOM: DurableObjectNamespace;
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

      // Get or create Room Durable Object
      const roomObjectId = env.ROOM.idFromName(roomId);
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

    // Room info endpoint: /api/room/:roomId
    const infoMatch = url.pathname.match(/^\/api\/room\/([^/]+)$/);
    if (infoMatch) {
      const roomId = infoMatch[1];
      const roomObjectId = env.ROOM.idFromName(roomId);
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
