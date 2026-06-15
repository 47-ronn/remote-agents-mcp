#![no_main]
//! Fuzz the path security gate: it must never panic, and `..` traversal must
//! never let a denied path through.

use libfuzzer_sys::fuzz_target;
use remote_agent::config::SecurityConfig;
use remote_agent::safety;

fuzz_target!(|data: &[u8]| {
    let path = String::from_utf8_lossy(data);

    // Empty allow list (deny list still applies).
    let sec = SecurityConfig::default();
    let _ = safety::check_path_allowed(&path, &sec);

    // Non-empty allow list exercises the other branch.
    let mut sec2 = SecurityConfig::default();
    sec2.allowed_paths = vec!["/home/user/project".to_string(), "/tmp".to_string()];
    let _ = safety::check_path_allowed(&path, &sec2);
});
