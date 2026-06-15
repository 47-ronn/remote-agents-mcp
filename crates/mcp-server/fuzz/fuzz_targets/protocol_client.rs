#![no_main]
//! Fuzz client-message JSON parsing (untrusted input arriving over the relay).
//! Parsing must never panic; round-tripping a successfully parsed message must
//! re-serialize without error.

use libfuzzer_sys::fuzz_target;
use remote_agents_shared::ClientMessage;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        if let Ok(msg) = ClientMessage::from_json(s) {
            let _ = msg.to_json();
        }
    }
});
