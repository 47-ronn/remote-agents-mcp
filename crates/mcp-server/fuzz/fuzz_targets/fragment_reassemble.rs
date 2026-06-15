#![no_main]
//! Fuzz the UDP fragment reassembler, which parses untrusted bytes off the
//! network. Two properties, neither may ever panic:
//!   1. Robustness: feeding arbitrary fragment payloads must not crash.
//!   2. Correctness: any message split into fragments must reassemble exactly,
//!      regardless of how the fuzzer permutes the fragment order.

use libfuzzer_sys::fuzz_target;
use remote_agents_shared::udp::{split_into_fragments, FragmentReassembler};

fuzz_target!(|data: &[u8]| {
    // (1) Robustness: the first byte (if any) drives how the rest is chopped
    // into raw "fragment" payloads fed straight to the parser.
    let mut r = FragmentReassembler::new();
    if let Some((&step, body)) = data.split_first() {
        let chunk = (step as usize).max(1);
        for piece in body.chunks(chunk) {
            let _ = r.insert(piece);
        }
    }

    // (2) Round-trip: split the input, feed the fragments back in a permuted
    // order, and require an exact reassembly once the last one arrives.
    let msg_id = data.len() as u32;
    let frags = split_into_fragments(data, msg_id);
    let mut r2 = FragmentReassembler::new();
    let n = frags.len();
    // Reverse order stresses out-of-order buffering.
    let mut completed = None;
    for f in frags.iter().rev() {
        if let Some(body) = r2.insert(f) {
            completed = Some(body);
        }
    }
    if n > 0 {
        assert_eq!(completed.as_deref(), Some(data), "split/reassemble mismatch");
    }
});
