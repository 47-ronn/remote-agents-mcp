import { describe, it, expect } from 'vitest';
import { scoreAgent, dedupAgents, selectTargets, selectStale, STALE_MS } from './room';
import type { AgentInfo, Target } from './types';

// Minimal AgentInfo factory; capabilities default to a plain executing peer.
function agent(over: Partial<AgentInfo> & { id: string }): AgentInfo {
  return {
    name: over.id,
    mode: 'plan',
    os: 'linux',
    arch: 'x86_64',
    hostname: over.id,
    tags: [],
    connected_at: 0,
    ...over,
  } as AgentInfo;
}

describe('scoreAgent', () => {
  it('ranks autonomous+executing above plain above send-only', () => {
    const auto = agent({ id: 'a', autonomous: true, accepts_commands: true });
    const plain = agent({ id: 'b', autonomous: false, accepts_commands: true });
    const sendOnly = agent({ id: 'c', accepts_commands: false });
    expect(scoreAgent(auto)).toBeGreaterThan(scoreAgent(plain));
    expect(scoreAgent(plain)).toBeGreaterThan(scoreAgent(sendOnly));
  });

  it('treats absent accepts_commands as executing (legacy)', () => {
    expect(scoreAgent(agent({ id: 'x' }))).toBe(1);
  });
});

describe('dedupAgents', () => {
  it('keeps distinct machines separate', () => {
    const out = dedupAgents([agent({ id: 'a' }), agent({ id: 'b' })]);
    expect(out.map((a) => a.id).sort()).toEqual(['a', 'b']);
  });

  it('collapses one machine and merges capabilities (true if any socket)', () => {
    // Same id, two sockets: one plain, one autonomous.
    const out = dedupAgents([
      agent({ id: 'dup', session_id: 's1', autonomous: false, accepts_commands: true }),
      agent({ id: 'dup', session_id: 's2', autonomous: true, accepts_commands: true }),
    ]);
    expect(out).toHaveLength(1);
    expect(out[0].id).toBe('dup');
    expect(out[0].autonomous).toBe(true);
    expect(out[0].accepts_commands).not.toBe(false);
    // Representative is the most-capable (autonomous) socket.
    expect(out[0].session_id).toBe('s2');
  });

  it('a machine is command-accepting if any socket accepts, even with a send-only one', () => {
    const out = dedupAgents([
      agent({ id: 'm', accepts_commands: false }),
      agent({ id: 'm', accepts_commands: true }),
    ]);
    expect(out).toHaveLength(1);
    expect(out[0].accepts_commands).not.toBe(false);
  });

  it('a machine whose every socket is send-only stays send-only', () => {
    const out = dedupAgents([
      agent({ id: 'm', accepts_commands: false }),
      agent({ id: 'm', accepts_commands: false }),
    ]);
    expect(out).toHaveLength(1);
    expect(out[0].accepts_commands).toBe(false);
  });

  it('reports the connection count per machine (duplicate-socket warning)', () => {
    const out = dedupAgents([
      agent({ id: 'm', session_id: 's1' }),
      agent({ id: 'm', session_id: 's2' }),
      agent({ id: 'm', session_id: 's3' }),
      agent({ id: 'solo' }),
    ]);
    const m = out.find((a) => a.id === 'm');
    const solo = out.find((a) => a.id === 'solo');
    expect(m?.connections).toBe(3);
    expect(solo?.connections).toBe(1);
  });

  it('preserves the version field through dedup (fleet version visibility)', () => {
    // Single socket: version carried straight through.
    const one = dedupAgents([agent({ id: 'a', version: '0.1.9' })]);
    expect(one[0].version).toBe('0.1.9');

    // Multiple sockets of one machine: the representative (most-capable) socket's
    // version is kept — the relay must forward what each host runs.
    const dup = dedupAgents([
      agent({ id: 'm', session_id: 's1', autonomous: false, version: '0.1.8' }),
      agent({ id: 'm', session_id: 's2', autonomous: true, version: '0.1.9' }),
    ]);
    expect(dup).toHaveLength(1);
    expect(dup[0].version).toBe('0.1.9');
  });
});

describe('selectStale', () => {
  const now = 1_000_000;

  it('reaps only sockets past the staleness window', () => {
    const entries = [
      { id: 'fresh', lastSeen: now - 1_000 }, // 1s ago
      { id: 'edge', lastSeen: now - STALE_MS }, // exactly at threshold → not stale
      { id: 'dead', lastSeen: now - STALE_MS - 1 }, // just over → stale
      { id: 'ancient', lastSeen: now - 10 * STALE_MS },
    ];
    const reaped = selectStale(entries, now, STALE_MS).map((e) => e.id);
    expect(reaped.sort()).toEqual(['ancient', 'dead']);
  });

  it('never reaps an entry that has no lastSeen (treated as fresh)', () => {
    const entries = [{ id: 'unknown' }, { id: 'old', lastSeen: now - 10 * STALE_MS }];
    expect(selectStale(entries, now, STALE_MS).map((e) => e.id)).toEqual(['old']);
  });

  it('reaps nothing when all sockets are within the window', () => {
    const entries = [
      { id: 'a', lastSeen: now - 5_000 },
      { id: 'b', lastSeen: now - 30_000 },
    ];
    expect(selectStale(entries, now, STALE_MS)).toEqual([]);
  });
});

describe('selectTargets', () => {
  // Candidates pair an agent_info with a tag we can assert on (the "socket").
  const cand = (info: AgentInfo) => ({ info, item: `${info.id}:${info.session_id ?? ''}` });

  it('agent target picks the single most-capable socket of a machine', () => {
    const candidates = [
      cand(agent({ id: 'dup', session_id: 's1', autonomous: false })),
      cand(agent({ id: 'dup', session_id: 's2', autonomous: true })),
    ];
    const picked = selectTargets(candidates, { type: 'agent', id: 'dup' } as Target);
    expect(picked).toEqual(['dup:s2']); // the autonomous socket
  });

  it('agent target reaches even a send-only node (self-rejects)', () => {
    const candidates = [cand(agent({ id: 'so', accepts_commands: false }))];
    expect(selectTargets(candidates, { type: 'agent', id: 'so' } as Target)).toEqual(['so:']);
  });

  it('all broadcast dedups one machine and skips send-only peers', () => {
    const candidates = [
      cand(agent({ id: 'm', session_id: 's1', autonomous: false })),
      cand(agent({ id: 'm', session_id: 's2', autonomous: true })),
      cand(agent({ id: 'other' })),
      cand(agent({ id: 'sendonly', accepts_commands: false })),
    ];
    const picked = selectTargets(candidates, { type: 'all' } as Target);
    // one delivery per machine, send-only excluded, most-capable socket chosen
    expect(picked.sort()).toEqual(['m:s2', 'other:'].sort());
  });

  it('tagged matches any overlapping tag and skips send-only', () => {
    const candidates = [
      cand(agent({ id: 'a', tags: ['backend', 'db'] })),
      cand(agent({ id: 'b', tags: ['frontend'] })),
      cand(agent({ id: 'c', tags: ['backend'], accepts_commands: false })),
    ];
    const picked = selectTargets(candidates, { type: 'tagged', tags: ['backend'] } as Target);
    expect(picked).toEqual(['a:']);
  });

  it('platform matches os family (case-insensitive), send-only excluded', () => {
    const candidates = [
      cand(agent({ id: 'lin', os: 'linux' })),
      cand(agent({ id: 'mac', os: 'macos' })),
    ];
    const picked = selectTargets(candidates, { type: 'platform', family: 'LINUX' } as Target);
    expect(picked).toEqual(['lin:']);
  });
});
