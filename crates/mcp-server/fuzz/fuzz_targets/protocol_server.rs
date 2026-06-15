#![no_main]
//! Fuzz server-message JSON parsing (untrusted input arriving over the relay).

use libfuzzer_sys::fuzz_target;
use remote_agents_shared::ServerMessage;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        if let Ok(msg) = ServerMessage::from_json(s) {
            let _ = msg.to_json();
        }
    }
});
