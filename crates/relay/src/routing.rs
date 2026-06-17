//! Target resolution — mirror of the relay's `resolveTarget` in
//! `worker/src/room.ts`, kept in lock-step via the shared `Target` type.

use crate::state::{Room, Tx};
use remote_agents_shared::{AgentInfo, Target};
use std::collections::HashMap;

/// Capability score for picking the most-capable connection of a host: a socket
/// that executes scores 1, an autonomous one scores 2 (so an autonomous+
/// executing socket wins). Kept in lock-step with the worker's `score` in
/// `resolveTarget('agent')` / `dedupAgents`.
fn score(info: &AgentInfo) -> u8 {
    (info.accepts_commands as u8) + if info.autonomous { 2 } else { 0 }
}

/// Collapse agent sessions that share one agent-id into a single logical host.
/// A machine may hold several live connections (many terminals on the same box);
/// it is listed once, and a capability (autonomous / accepts_commands) is present
/// if ANY of its connections has it. The representative carries the most-capable
/// connection's metadata (session_id/platform) with the merged flags applied.
pub fn dedup_agents(room: &Room) -> Vec<AgentInfo> {
    let mut by_id: HashMap<String, AgentInfo> = HashMap::new();
    for e in room.agents.iter() {
        let info = &e.value().info;
        match by_id.get_mut(&info.id) {
            None => {
                by_id.insert(info.id.clone(), info.clone());
            }
            Some(rep) => {
                let autonomous = rep.autonomous || info.autonomous;
                let accepts = rep.accepts_commands || info.accepts_commands;
                if score(info) > score(rep) {
                    *rep = info.clone();
                }
                rep.autonomous = autonomous;
                rep.accepts_commands = accepts;
            }
        }
    }
    by_id.into_values().collect()
}

/// One capable `(id, tx)` per host id, among sockets matching `pred`. Used by
/// broadcast targets so each machine receives a command once (on its most-
/// capable connection), never several times for its several open terminals.
fn dedup_targets(room: &Room, pred: impl Fn(&AgentInfo) -> bool) -> Vec<(String, Tx)> {
    let mut by_id: HashMap<String, (u8, Tx)> = HashMap::new();
    for e in room.agents.iter() {
        let info = &e.value().info;
        if !pred(info) {
            continue;
        }
        let s = score(info);
        match by_id.get(&info.id) {
            Some((best, _)) if *best >= s => {}
            _ => {
                by_id.insert(info.id.clone(), (s, e.value().tx.clone()));
            }
        }
    }
    by_id.into_iter().map(|(id, (_, tx))| (id, tx)).collect()
}

/// Resolve the outbound senders matching `target`, one per host id (a machine
/// with several open terminals is one logical peer and is contacted once, on
/// its most-capable connection).
///
/// Broadcast targets (All/Tagged/Platform) skip send-only peers
/// (`accepts_commands == false`: `--no-agent` controllers/dashboards) — they
/// never execute, so fanning out to them is pointless. An explicit
/// `Target::Agent` is still delivered even to a send-only host (it replies with
/// its own --no-agent rejection, which is informative).
pub fn resolve_targets(room: &Room, target: &Target) -> Vec<(String, Tx)> {
    match target {
        // Pick the single most-capable connection for this id (prefer one that
        // executes AND is autonomous), so a stale/less-capable terminal of the
        // same machine can't answer first (e.g. "autonomous not enabled").
        Target::Agent { id } => room
            .agents
            .iter()
            .filter(|e| &e.value().info.id == id)
            .max_by_key(|e| score(&e.value().info))
            .map(|e| vec![(id.clone(), e.value().tx.clone())])
            .unwrap_or_default(),

        Target::All => dedup_targets(room, |i| i.accepts_commands),

        // Tagged = any tag overlaps (matches the worker's `.some(...)`).
        Target::Tagged { tags } => dedup_targets(room, |i| {
            i.accepts_commands && i.tags.iter().any(|t| tags.contains(t))
        }),

        // Platform = OS family match (case-insensitive), against the richer
        // `platform.family` with a fallback to the legacy `os` field.
        Target::Platform { family } => {
            dedup_targets(room, |i| i.accepts_commands && platform_matches(i, family))
        }
    }
}

/// Whether an agent's OS family matches `family` (case-insensitive). Checks the
/// structured `platform.family` first, then the legacy `os` field for agents
/// that predate platform metadata.
pub fn platform_matches(info: &remote_agents_shared::AgentInfo, family: &str) -> bool {
    family.eq_ignore_ascii_case(&info.platform.family) || family.eq_ignore_ascii_case(&info.os)
}

/// Find the outbound sender for a connection by its **session id** — checking
/// MCP clients (keyed by session id) and then agents (matched on
/// `info.session_id`, which the relay assigns at join). Used to route UDP
/// signaling, where peers address each other by session id, not agent id.
/// Mirrors the worker's `findSocketBySession`.
pub fn find_session_tx(room: &Room, session_id: &str) -> Option<Tx> {
    if let Some(mcp) = room.mcp.get(session_id) {
        return Some(mcp.tx.clone());
    }
    // Agents are keyed by session id, so a direct lookup wins; fall back to the
    // info.session_id scan for robustness.
    if let Some(s) = room.agents.get(session_id) {
        return Some(s.tx.clone());
    }
    room.agents
        .iter()
        .find(|e| e.info.session_id.as_deref() == Some(session_id))
        .map(|e| e.tx.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{AgentSession, McpSession, Room};
    use remote_agents_shared::{AgentInfo, AgentMode};
    use tokio::sync::mpsc;

    fn agent(id: &str, tags: &[&str]) -> AgentSession {
        let (tx, _rx) = mpsc::channel(8);
        AgentSession {
            info: AgentInfo {
                id: id.to_string(),
                name: id.to_string(),
                mode: AgentMode::Plan,
                os: "linux".into(),
                arch: "x86_64".into(),
                hostname: id.to_string(),
                tags: tags.iter().map(|s| s.to_string()).collect(),
                platform: Default::default(),
                autonomous: false,
                accepts_commands: true,
                connected_at: 0,
                session_id: None,
                version: String::new(), update_available: None,
            },
            tx,
        }
    }

    fn room() -> Room {
        let r = Room::default();
        r.agents.insert("a".into(), agent("a", &["backend"]));
        r.agents.insert("b".into(), agent("b", &["frontend"]));
        r.agents.insert("c".into(), agent("c", &["backend", "db"]));
        r
    }

    #[test]
    fn target_agent() {
        let r = room();
        assert_eq!(resolve_targets(&r, &Target::Agent { id: "b".into() }).len(), 1);
        assert_eq!(resolve_targets(&r, &Target::Agent { id: "zzz".into() }).len(), 0);
    }

    #[test]
    fn target_all() {
        let r = room();
        assert_eq!(resolve_targets(&r, &Target::All).len(), 3);
    }

    #[test]
    fn broadcasts_skip_send_only_peers_but_explicit_target_reaches_them() {
        let r = room();
        // Make "b" a send-only peer (--no-agent).
        r.agents.get_mut("b").unwrap().info.accepts_commands = false;

        // All / tagged / platform skip the send-only peer.
        assert_eq!(resolve_targets(&r, &Target::All).len(), 2);
        let backend = resolve_targets(&r, &Target::Tagged { tags: vec!["frontend".into()] });
        assert!(backend.is_empty(), "b is frontend but send-only → skipped");
        assert_eq!(
            resolve_targets(&r, &Target::Platform { family: "linux".into() }).len(),
            2
        );

        // ...but an explicit Agent target still reaches it (node self-rejects).
        assert_eq!(resolve_targets(&r, &Target::Agent { id: "b".into() }).len(), 1);
    }

    #[test]
    fn target_tagged_overlap() {
        let r = room();
        let backend = resolve_targets(&r, &Target::Tagged { tags: vec!["backend".into()] });
        let mut ids: Vec<String> = backend.into_iter().map(|(id, _)| id).collect();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "c".to_string()]);

        let none = resolve_targets(&r, &Target::Tagged { tags: vec!["nope".into()] });
        assert_eq!(none.len(), 0);
    }

    #[test]
    fn target_platform_matches_os_family_case_insensitively() {
        // The shared agent() helper sets os = "linux".
        let r = room();
        assert_eq!(
            resolve_targets(&r, &Target::Platform { family: "linux".into() }).len(),
            3
        );
        assert_eq!(
            resolve_targets(&r, &Target::Platform { family: "LINUX".into() }).len(),
            3
        );
        assert_eq!(
            resolve_targets(&r, &Target::Platform { family: "windows".into() }).len(),
            0
        );
    }

    #[test]
    fn platform_matches_prefers_platform_family_with_os_fallback() {
        let mut info = agent("x", &[]).info;
        // Structured family wins.
        info.platform.family = "macos".into();
        info.os = "linux".into();
        assert!(platform_matches(&info, "macos"));
        assert!(platform_matches(&info, "linux")); // legacy os fallback
        assert!(!platform_matches(&info, "windows"));

        // Older agent without platform metadata → falls back to os only.
        info.platform = Default::default();
        info.os = "windows".into();
        assert!(platform_matches(&info, "WINDOWS"));
        assert!(!platform_matches(&info, "linux"));
    }

    #[test]
    fn find_session_tx_resolves_by_session_id_not_agent_id() {
        let r = Room::default();
        // Agent keyed by its session id "sess-a" (its agent id is "a").
        let mut s = agent("a", &[]);
        s.info.session_id = Some("sess-a".into());
        r.agents.insert("sess-a".into(), s);
        // MCP client keyed by its session id.
        let (mtx, _mrx) = mpsc::channel(8);
        r.mcp.insert("mcp-1".into(), McpSession { id: "mcp-1".into(), tx: mtx });

        assert!(find_session_tx(&r, "sess-a").is_some(), "agent by session_id");
        assert!(find_session_tx(&r, "mcp-1").is_some(), "mcp by session id");
        // The agent's *id* is not a session id — must not resolve (the bug
        // fixed in iter25 matched on info.id by mistake).
        assert!(find_session_tx(&r, "a").is_none(), "agent id is not a session id");
        assert!(find_session_tx(&r, "nope").is_none());
    }

    /// Build an agent session for `id` keyed by `session`, with explicit
    /// capabilities and a controllable tx (its rx is returned for assertions).
    fn sess(
        id: &str,
        session: &str,
        autonomous: bool,
        accepts: bool,
    ) -> (AgentSession, tokio::sync::mpsc::Receiver<String>) {
        let (tx, rx) = mpsc::channel(8);
        let mut s = agent(id, &[]);
        s.info.session_id = Some(session.to_string());
        s.info.autonomous = autonomous;
        s.info.accepts_commands = accepts;
        s.tx = tx;
        (s, rx)
    }

    #[test]
    fn dedup_merges_capabilities_and_routes_agent_to_most_capable() {
        // Two live sockets for the SAME machine id "dup": one plain, one that
        // both executes and is autonomous (many terminals on one box, iter89).
        let r = Room::default();
        let (plain, _rx_plain) = sess("dup", "s1", false, true);
        let (auto, mut rx_auto) = sess("dup", "s2", true, true);
        r.agents.insert("s1".into(), plain);
        r.agents.insert("s2".into(), auto);

        // Collapsed to ONE logical host, capability flags merged (true if any).
        let merged = dedup_agents(&r);
        assert_eq!(merged.len(), 1, "two sockets of one machine = one host");
        assert_eq!(merged[0].id, "dup");
        assert!(merged[0].autonomous, "autonomous if any socket is");
        assert!(merged[0].accepts_commands);

        // An Agent-targeted command goes to exactly one socket — the autonomous
        // one — not fanned out to both (the bug that surfaced "not enabled").
        let t = resolve_targets(&r, &Target::Agent { id: "dup".into() });
        assert_eq!(t.len(), 1, "single most-capable socket per id");
        t[0].1.try_send("X".to_string()).unwrap();
        assert_eq!(
            rx_auto.try_recv().unwrap(),
            "X",
            "command must reach the autonomous socket"
        );

        // Broadcast (All) also collapses the machine to one delivery.
        assert_eq!(resolve_targets(&r, &Target::All).len(), 1, "one delivery per machine");
    }

    #[test]
    fn dedup_target_all_skips_machine_with_all_send_only_sockets() {
        // A machine whose every socket is send-only (--no-agent) is skipped by
        // broadcasts, even with several connections.
        let r = Room::default();
        let (a, _ra) = sess("x", "s1", false, false);
        let (b, _rb) = sess("x", "s2", false, false);
        r.agents.insert("s1".into(), a);
        r.agents.insert("s2".into(), b);
        assert_eq!(resolve_targets(&r, &Target::All).len(), 0);
        // ...but an explicit Agent target still reaches it (self-rejects).
        assert_eq!(resolve_targets(&r, &Target::Agent { id: "x".into() }).len(), 1);
    }
}
