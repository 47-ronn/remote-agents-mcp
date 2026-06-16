#![no_main]
//! Fuzz the relay's target resolution: build a room from an arbitrary set of
//! agents (ids + tags) and resolve an arbitrary target. Must never panic, and
//! the resolved set must always be a subset of the registered agents with the
//! expected cardinality per target kind.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use remote_agents_relay::routing::resolve_targets;
use remote_agents_relay::state::{AgentSession, Room};
use remote_agents_shared::{AgentInfo, AgentMode, Target};
use std::collections::HashSet;

#[derive(Arbitrary, Debug)]
enum TargetKind {
    Agent(String),
    All,
    Tagged(Vec<String>),
}

#[derive(Arbitrary, Debug)]
struct Input {
    /// (agent_id, tags)
    agents: Vec<(String, Vec<String>)>,
    target: TargetKind,
}

fn mk_agent(id: &str, tags: Vec<String>) -> AgentSession {
    // Receiver dropped immediately — we never send, only clone the sender.
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    AgentSession {
        info: AgentInfo {
            id: id.to_string(),
            name: String::new(),
            mode: AgentMode::Plan,
            os: String::new(),
            arch: String::new(),
            hostname: String::new(),
            tags,
            platform: Default::default(),
            autonomous: false,
            accepts_commands: true,
            connected_at: 0,
            session_id: None,
            update_available: None,
        },
        tx,
    }
}

fuzz_target!(|input: Input| {
    let room = Room::default();
    let mut ids = HashSet::new();
    for (id, tags) in &input.agents {
        room.agents.insert(id.clone(), mk_agent(id, tags.clone()));
        ids.insert(id.clone()); // DashMap + HashSet both dedup by id
    }

    let target = match &input.target {
        TargetKind::Agent(id) => Target::Agent { id: id.clone() },
        TargetKind::All => Target::All,
        TargetKind::Tagged(tags) => Target::Tagged { tags: tags.clone() },
    };

    let resolved = resolve_targets(&room, &target);

    // Invariant 1: result is a subset of the registered agents.
    assert!(resolved.len() <= ids.len());
    for (id, _tx) in &resolved {
        assert!(ids.contains(id), "resolved unknown agent {:?}", id);
    }
    // No duplicate agents in the result.
    let unique: HashSet<&String> = resolved.iter().map(|(id, _)| id).collect();
    assert_eq!(unique.len(), resolved.len(), "duplicate agent in result");

    // Invariant 2: cardinality per target kind.
    match &target {
        Target::Agent { id } => {
            assert!(resolved.len() <= 1);
            if !ids.contains(id) {
                assert!(resolved.is_empty());
            }
        }
        Target::All => assert_eq!(resolved.len(), ids.len()),
        Target::Tagged { .. } | Target::Platform { .. } => {} // covered by the subset invariant
    }
});
