#![no_main]
//! Fuzz decryption + deserialization of the command/result envelopes that the
//! agent and MCP server receive over the relay. Must never panic.

use libfuzzer_sys::fuzz_target;
use remote_agents_shared::{Cipher, Command, CommandResult};

fuzz_target!(|data: &[u8]| {
    let cipher = Cipher::for_transport("fuzz-token", None);
    let s = String::from_utf8_lossy(data);
    let _ = Command::decrypt(&s, &cipher);
    let _ = CommandResult::decrypt(&s, &cipher);
});
