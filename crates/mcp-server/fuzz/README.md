# Fuzzing

Coverage-guided fuzz targets (libFuzzer via [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz))
for the untrusted-input surfaces of the agent.

## Targets

| Target | Surface | Invariant |
|--------|---------|-----------|
| `safety_command`  | `safety::check_command_allowed` | never panics; hard-denied commands rejected in every mode |
| `safety_path`     | `safety::check_path_allowed` | never panics; `..` cannot escape allow/deny lists |
| `protocol_client` | `ClientMessage::from_json` | parse never panics; parsed msg re-serializes |
| `protocol_server` | `ServerMessage::from_json` | parse never panics; parsed msg re-serializes |
| `crypto_decrypt`  | `Cipher::decrypt` of attacker ciphertext | never panics (Ok/Err only) |
| `envelope_decrypt`| `Command/CommandResult::decrypt` envelopes | never panics |
| `cron_validate`   | `scheduler::is_valid_cron` | never panics |

## Running

```bash
cd crates/agent
cargo +nightly fuzz build                       # build all targets (ASAN + libFuzzer)
cargo +nightly fuzz run safety_command          # run until a crash is found
cargo +nightly fuzz run safety_command -- -max_total_time=30   # time-boxed
```

Crashes are written to `fuzz/artifacts/<target>/`; reproduce with
`cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<crash-file>`.

The `corpus/`, `artifacts/`, and `target/` directories are git-ignored.
