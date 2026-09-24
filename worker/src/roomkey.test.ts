import { describe, it, expect } from 'vitest';
import { roomKey } from './roomkey';

/// Known-answer pin: the room key must be identical to the Rust relay's
/// `room_key` (crates/shared/src/crypto.rs asserts the same vector) — the Rust
/// relay and this worker only interoperate while both derive the same storage
/// key from the same room + token.
describe('roomKey', () => {
  it('matches the Rust relay known answer', async () => {
    expect(await roomKey('iwejf', 'secret')).toBe(
      '6245a336fff2893dd0039b91661241fa593e66808d9ee14357ba0ab3fc077d8a'
    );
  });

  it('separates token groups (wrong token → different, empty room)', async () => {
    expect(await roomKey('iwejf', 'wrong')).not.toBe(await roomKey('iwejf', 'secret'));
    // Room name is part of the key too.
    expect(await roomKey('dev', 'secret')).not.toBe(await roomKey('iwejf', 'secret'));
    // Deterministic for repeated calls (same room+token → same DO).
    expect(await roomKey('dev', 'tok')).toBe(await roomKey('dev', 'tok'));
  });
});
