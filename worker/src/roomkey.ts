/**
 * Derive the internal room storage key from the room name and its token.
 *
 * The DO for a room is addressed by this hash instead of the raw room name,
 * which makes the token the room's actual gate: a host presenting a wrong
 * token derives a DIFFERENT key and lands in its own (empty) DO — it can never
 * see the real room's roster, and no server-side secret or registry is needed.
 *
 * MUST stay in lock-step with the Rust relay's `room_key`
 * (crates/shared/src/crypto.rs) — same namespaced input, same SHA-256
 * lowercase hex. Known-answer tests on both sides pin the compatibility.
 */
export async function roomKey(room: string, token: string): Promise<string> {
  const data = new TextEncoder().encode(`remote-agents-room/v1:${room}:${token}`);
  const digest = await crypto.subtle.digest('SHA-256', data);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}
