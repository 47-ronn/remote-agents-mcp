#![no_main]
//! Fuzz AES-GCM decryption of attacker-controlled ciphertext. Decryption must
//! never panic — it should only ever return Ok or Err.

use libfuzzer_sys::fuzz_target;
use remote_agents_shared::Cipher;

fuzz_target!(|data: &[u8]| {
    let cipher = Cipher::for_transport("fuzz-token", None);
    let s = String::from_utf8_lossy(data);
    let _ = cipher.decrypt(&s);
    let _ = cipher.decrypt_str(&s);
});
