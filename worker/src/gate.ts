/**
 * Per-IP abuse gate for the worker relay: a sliding-window connection budget
 * plus an auth-failure ban list, mirroring the Rust relay's `limiter.rs`.
 *
 * One Durable Object instance per client IP (idFromName('ip:<ip>')), so an
 * attacker's window is theirs alone and a fleet behind one NAT shares one
 * generous budget. The state is in-memory: a DO eviction resets the windows,
 * which is acceptable — windows are seconds, and eviction is rare relative to
 * a sustained guess-flood.
 *
 * The window logic lives in pure, unit-tested functions; the DO class is a
 * thin HTTP wrapper over them.
 */

/** Sliding window for counting connection attempts. */
export const CONN_WINDOW_MS = 10_000;
/** Max connection attempts (WS upgrades + HTTP info calls) per IP per window. */
export const MAX_CONNS = 60;
/** Sliding window for counting failed authentications. */
export const FAIL_WINDOW_MS = 60_000;
/** Failures within FAIL_WINDOW_MS before the IP is banned. */
export const MAX_FAILS = 10;
/** How long a ban lasts once tripped. */
export const BAN_MS = 300_000;

export interface GateState {
  /** Timestamps (epoch ms) of connection attempts. */
  conns: number[];
  /** Timestamps (epoch ms) of failed authentications. */
  fails: number[];
  /** Epoch ms until which the IP is banned; 0 = not banned. */
  bannedUntil: number;
}

export function newGateState(): GateState {
  return { conns: [], fails: [], bannedUntil: 0 };
}

/** Constant-time string equality for secrets: no early exit that a network
 *  observer could time into a prefix oracle. */
export function constTimeEq(a: string, b: string): boolean {
  const max = Math.max(a.length, b.length);
  let diff = a.length === b.length ? 0 : 1;
  for (let i = 0; i < max; i++) {
    diff |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
  }
  return diff === 0;
}

function prune(list: number[], now: number, window: number): void {
  const cutoff = now - window;
  while (list.length > 0 && list[0] <= cutoff) list.shift();
}

export interface Verdict {
  allowed: boolean;
  retryAfterSec: number;
}

/**
 * Check — and count — one connection attempt. Denials also count, so a client
 * that keeps hammering stays denied until it pauses for a full window.
 */
export function gateAttempt(s: GateState, now: number): Verdict {
  prune(s.conns, now, CONN_WINDOW_MS);
  prune(s.fails, now, FAIL_WINDOW_MS);
  if (s.bannedUntil > now) {
    return { allowed: false, retryAfterSec: Math.ceil((s.bannedUntil - now) / 1000) };
  }
  s.conns.push(now);
  if (s.conns.length > MAX_CONNS) {
    const oldest = s.conns[0];
    return {
      allowed: false,
      retryAfterSec: Math.max(1, Math.ceil((oldest + CONN_WINDOW_MS - now) / 1000)),
    };
  }
  return { allowed: true, retryAfterSec: 0 };
}

/** Record a failed authentication; too many within FAIL_WINDOW_MS bans the IP. */
export function recordFailure(s: GateState, now: number): { banned: boolean } & Verdict {
  prune(s.fails, now, FAIL_WINDOW_MS);
  s.fails.push(now);
  if (s.fails.length > MAX_FAILS) {
    s.bannedUntil = now + BAN_MS;
    s.fails = [];
    return { banned: true, allowed: false, retryAfterSec: Math.ceil(BAN_MS / 1000) };
  }
  return { banned: false, allowed: true, retryAfterSec: 0 };
}

/** Clear the failure streak after a successful authentication. */
export function clearFails(s: GateState): void {
  s.fails = [];
}

/**
 * The Durable Object. Endpoints (all POST, JSON responses):
 * - `/attempt`       — gate + record one connection attempt.
 * - `/auth-failure`  — record one failed authentication (may trip a ban).
 * - `/success`       — clear the failure streak (successful auth).
 */
export class Gate implements DurableObject {
  private state: GateState = newGateState();

  constructor(
    _stub: DurableObjectState,
    _env: unknown
  ) {}

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const now = Date.now();
    if (url.pathname === '/attempt') {
      return Response.json(gateAttempt(this.state, now));
    }
    if (url.pathname === '/auth-failure') {
      return Response.json(recordFailure(this.state, now));
    }
    if (url.pathname === '/success') {
      clearFails(this.state);
      return Response.json({ ok: true });
    }
    return new Response('Not Found', { status: 404 });
  }
}
