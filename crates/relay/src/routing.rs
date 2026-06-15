//! Target resolution — mirror of the relay's `resolveTarget` in
//! `worker/src/room.ts`, kept in lock-step via the shared `Target` type.

use crate::state::{Room, Tx};
use remote_agents_shared::Target;

/// Resolve the outbound senders of all agents matching `target`.
/// Returns `(agent_id, tx)` pairs.
pub fn resolve_targets(room: &Room, target: &Target) -> Vec<(String, Tx)> {
    match target {
        Target::Agent { id } => room
            .agents
            .get(id)
            .map(|s| vec![(id.clone(), s.tx.clone())])
            .unwrap_or_default(),

        Target::All => room
            .agents
            .iter()
            .map(|e| (e.key().clone(), e.tx.clone()))
            .collect(),

        // Tagged = any tag overlaps (matches the worker's `.some(...)`).
        Target::Tagged { tags } => room
            .agents
            .iter()
            .filter(|e| e.info.tags.iter().any(|t| tags.contains(t)))
            .map(|e| (e.key().clone(), e.tx.clone()))
            .collect(),

        // Platform = OS family match (case-insensitive), against the richer
        // `platform.family` with a fallback to the legacy `os` field.
        Target::Platform { family } => room
            .agents
            .iter()
            .filter(|e| platform_matches(&e.info, family))
            .map(|e| (e.key().clone(), e.tx.clone()))
            .collect(),
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
                connected_at: 0,
                session_id: None,
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
        // Agent keyed by id "a" but assigned session_id "sess-a" at join.
        let mut s = agent("a", &[]);
        s.info.session_id = Some("sess-a".into());
        r.agents.insert("a".into(), s);
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
}
