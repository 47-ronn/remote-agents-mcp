import { describe, expect, it } from 'vitest';
import {
  BAN_MS,
  CONN_WINDOW_MS,
  clearFails,
  constTimeEq,
  gateAttempt,
  MAX_CONNS,
  MAX_FAILS,
  newGateState,
  recordFailure,
} from './gate';

const T0 = 1_000_000; // arbitrary epoch ms

describe('constTimeEq', () => {
  it('matches plain equality semantics', () => {
    expect(constTimeEq('secret', 'secret')).toBe(true);
    expect(constTimeEq('secret', 'secreX')).toBe(false);
    expect(constTimeEq('secret', 'secrets')).toBe(false);
    expect(constTimeEq('', '')).toBe(true);
  });
});

describe('gateAttempt (connection budget)', () => {
  it('allows up to MAX_CONNS then denies with a bounded retry', () => {
    const s = newGateState();
    for (let i = 0; i < MAX_CONNS; i++) {
      expect(gateAttempt(s, T0).allowed).toBe(true);
    }
    const v = gateAttempt(s, T0 + 1);
    expect(v.allowed).toBe(false);
    expect(v.retryAfterSec).toBeGreaterThan(0);
    expect(v.retryAfterSec).toBeLessThanOrEqual(Math.ceil(CONN_WINDOW_MS / 1000));
  });

  it('recovers after the window drains', () => {
    const s = newGateState();
    for (let i = 0; i < MAX_CONNS; i++) gateAttempt(s, T0);
    expect(gateAttempt(s, T0).allowed).toBe(false);
    expect(gateAttempt(s, T0 + CONN_WINDOW_MS + 1).allowed).toBe(true);
  });

  it('denials count, so hammering stays denied mid-window', () => {
    const s = newGateState();
    for (let i = 0; i < MAX_CONNS; i++) gateAttempt(s, T0);
    // Halfway through the window the attempts keep it full.
    expect(gateAttempt(s, T0 + CONN_WINDOW_MS / 2).allowed).toBe(false);
  });

  it('each GateState instance is a separate IP budget', () => {
    const a = newGateState();
    const b = newGateState();
    for (let i = 0; i < MAX_CONNS; i++) gateAttempt(a, T0);
    expect(gateAttempt(a, T0).allowed).toBe(false);
    expect(gateAttempt(b, T0).allowed).toBe(true);
  });

  it('denials carry increasing nth for log dedup, restarting after recovery', () => {
    const s = newGateState();
    gateAttempt(s, T0); // budget used up (MAX_CONNS == 1 in spirit; with the
    // real MAX_CONNS the loop below is what matters)
    const full = newGateState();
    for (let i = 0; i < MAX_CONNS; i++) expect(gateAttempt(full, T0).nth).toBe(0);
    for (const expectN of [1, 2, 3]) {
      const v = gateAttempt(full, T0);
      expect(v.allowed).toBe(false);
      expect(v.nth).toBe(expectN);
    }
    // after the window drains the sequence restarts
    const later = T0 + CONN_WINDOW_MS + 1;
    const v2 = gateAttempt(full, later);
    expect(v2.allowed).toBe(true);
    expect(v2.nth).toBe(0);
    for (let i = 1; i < MAX_CONNS; i++) gateAttempt(full, later); // refill budget
    const v3 = gateAttempt(full, later);
    expect(v3.allowed).toBe(false);
    expect(v3.nth).toBe(1);
  });
});

describe('recordFailure (auth-failure ban)', () => {
  it('bans after MAX_FAILS failures within the window', () => {
    const s = newGateState();
    for (let i = 0; i < MAX_FAILS; i++) {
      expect(recordFailure(s, T0).banned).toBe(false);
    }
    expect(recordFailure(s, T0).banned).toBe(true);
    // While banned, connection attempts are denied with the ban's cooldown.
    const v = gateAttempt(s, T0 + 1);
    expect(v.allowed).toBe(false);
    expect(v.retryAfterSec).toBeLessThanOrEqual(Math.ceil(BAN_MS / 1000));
  });

  it('the ban lifts after BAN_MS', () => {
    const s = newGateState();
    for (let i = 0; i <= MAX_FAILS; i++) recordFailure(s, T0);
    expect(gateAttempt(s, T0 + 1).allowed).toBe(false);
    expect(gateAttempt(s, T0 + BAN_MS + 2).allowed).toBe(true);
  });

  it('clearFails resets a streak before it becomes a ban', () => {
    const s = newGateState();
    recordFailure(s, T0);
    recordFailure(s, T0);
    clearFails(s);
    recordFailure(s, T0);
    expect(gateAttempt(s, T0).allowed).toBe(true);
  });

  it('failures older than the window do not accumulate', () => {
    const s = newGateState();
    recordFailure(s, T0);
    // 61 s later the first failure expired (window is 60 s).
    const later = T0 + 61_000;
    for (let i = 0; i < MAX_FAILS; i++) recordFailure(s, later);
    // MAX_FAILS fresh failures == max, not over → no ban yet.
    expect(gateAttempt(s, later).allowed).toBe(true);
  });
});
