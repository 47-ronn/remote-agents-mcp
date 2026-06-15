#![no_main]
//! Fuzz cron-expression validation (untrusted schedule strings). Must never
//! panic.

use libfuzzer_sys::fuzz_target;
use remote_agent::scheduler;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = scheduler::is_valid_cron(s);
    }
});
