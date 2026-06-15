#![no_main]
//! Fuzz the command security gate: it must never panic for any input, in any
//! mode, and Plan mode must never accept an obviously destructive command.

use libfuzzer_sys::fuzz_target;
use remote_agent::config::SecurityConfig;
use remote_agent::safety;
use remote_agents_shared::AgentMode;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let mode = match data[0] % 4 {
        0 => AgentMode::Plan,
        1 => AgentMode::Edit,
        2 => AgentMode::Bypass,
        _ => AgentMode::Disabled,
    };
    let command = String::from_utf8_lossy(&data[1..]);
    let sec = SecurityConfig::default();

    // Primary invariant: never panic regardless of input.
    let allowed = safety::check_command_allowed(&command, mode, &sec).is_ok();

    // Secondary invariant: the always-on hard denylist must hold in every mode.
    if command.contains("rm -rf /") || command.contains(":(){:|:&};:") {
        assert!(!allowed, "hard-denied command was allowed: {:?}", command);
    }
});
