// Live end-to-end smoke test for the deployed Cloudflare worker relay.
// The worker's room.ts logic has no automated unit coverage, so this drives the
// real protocol over WebSocket: auth ordering, peer-awareness, platform
// targeting, and agent_left fan-out to both MCP and peer agents.
//
// Usage:
//   node worker/e2e.mjs <wss://your-worker-host> [room] [token]
// Pass your deployed worker URL as the first arg. Exits 0 on success, 1 on first failure.

const URL = process.argv[2] || "ws://127.0.0.1:8080";
const ROOM = process.argv[3] || `e2e-${Math.floor(Date.now() / 1000)}`;
const TOK = process.argv[4] || "e2etok";

const fail = (m) => { console.error("FAIL:", m); process.exit(1); };

const conn = (role, info) =>
  new Promise((resolve, reject) => {
    const ws = new WebSocket(`${URL}/ws/room/${ROOM}?token=${TOK}`);
    ws.queue = [];
    ws.waiters = [];
    ws.addEventListener("message", (e) => {
      const msg = JSON.parse(e.data);
      if (ws.waiters.length) ws.waiters.shift().resolve(msg);
      else ws.queue.push(msg);
    });
    // next() removes its waiter on timeout so a late message is never consumed
    // by a dead waiter (the bug that masked a passing worker during dev).
    ws.next = (timeoutMs = 5000) =>
      new Promise((res, rej) => {
        if (ws.queue.length) return res(ws.queue.shift());
        const waiter = { resolve: res };
        const t = setTimeout(() => {
          const i = ws.waiters.indexOf(waiter);
          if (i >= 0) ws.waiters.splice(i, 1);
          rej(new Error("recv timeout"));
        }, timeoutMs);
        waiter.resolve = (m) => { clearTimeout(t); res(m); };
        ws.waiters.push(waiter);
      });
    ws.addEventListener("open", () => {
      ws.send(JSON.stringify({ type: "auth", room: ROOM, token: TOK, role, agent_info: info }));
      resolve(ws);
    });
    ws.addEventListener("error", reject);
  });

const agentInfo = (id, os) => ({
  id, name: id, mode: "bypass", os, arch: "x86_64", hostname: id,
  tags: [], platform: { family: os, arch: "x86_64" }, autonomous: false, connected_at: 0,
});

const expect = (msg, type) => {
  if (msg.type !== type) fail(`expected ${type}, got ${msg.type} (${JSON.stringify(msg)})`);
  return msg;
};

const main = async () => {
  console.log(`e2e: ${URL} room=${ROOM}`);

  // auth_ok MUST arrive before any peer-awareness frame (regression guard).
  const a1 = await conn("agent", agentInfo("a1", "linux"));
  expect(await a1.next(), "auth_ok");
  const list1 = expect(await a1.next(), "agent_list");
  if (list1.agents.length !== 0) fail(`a1 expected empty peer list, got ${list1.agents.length}`);
  console.log("OK: auth_ok precedes agent_list; a1 empty peer list");

  // a2 joins → sees a1; a1 is told a2 joined.
  const a2 = await conn("agent", agentInfo("a2", "windows"));
  expect(await a2.next(), "auth_ok");
  const list2 = expect(await a2.next(), "agent_list");
  if (!list2.agents.some((a) => a.id === "a1")) fail("a2 peer list should contain a1");
  if (expect(await a1.next(), "agent_joined").agent.id !== "a2") fail("a1 should see a2 join");
  console.log("OK: peer-awareness (a2 sees a1; a1 notified of a2)");

  // MCP sees both.
  const mcp = await conn("mcp", undefined);
  expect(await mcp.next(), "auth_ok");
  mcp.send(JSON.stringify({ type: "list_agents" }));
  if (expect(await mcp.next(), "agent_list").agents.length !== 2) fail("mcp should see 2 agents");
  console.log("OK: mcp sees both agents");

  // Platform targeting: os:linux reaches a1 only.
  mcp.send(JSON.stringify({
    type: "command", request_id: "r1",
    target: { type: "platform", family: "linux" }, payload: "OPAQUE",
  }));
  if (expect(await a1.next(), "command").request_id !== "r1") fail("a1 should receive r1");
  let a2got = false;
  try { await a2.next(1500); a2got = true; } catch {}
  if (a2got) fail("a2 (windows) must NOT receive an os:linux command");
  console.log("OK: platform targeting (os:linux → a1 only)");

  // agent_left fans out to remaining agents and MCP.
  a1.close();
  const waitLeft = async (ws, who) => {
    for (;;) {
      const m = await ws.next(12000);
      if (m.type === "agent_left") {
        if (m.agent_id !== "a1") fail(`${who} got agent_left for ${m.agent_id}`);
        return;
      }
    }
  };
  await Promise.all([waitLeft(a2, "a2"), waitLeft(mcp, "mcp")]);
  console.log("OK: agent_left fan-out to peer agent + mcp");

  a2.close(); mcp.close();
  console.log("\nALL E2E CHECKS PASSED");
  process.exit(0);
};
main().catch((e) => fail(e.message || String(e)));
