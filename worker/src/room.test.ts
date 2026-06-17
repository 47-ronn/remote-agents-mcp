import { describe, it, expect } from 'vitest';
import { scoreAgent, dedupAgents } from './room';
import type { AgentInfo } from './types';

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
});
