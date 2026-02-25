# Executor Phase 2B Slice 32 Evidence (2026-02-25)

## Scope

1. Extracted graceful-shutdown wait primitives into testable helpers in `main.rs`:
   1. `await_shutdown_signal_ctrl_c_only`,
   2. unix `await_shutdown_signal_unix`.
2. Added async guard tests for shutdown signal helper completion paths.
3. Removed time-based sleep from forward-skew HMAC replay guard test by asserting replay-cache expiry horizon directly before second verify.
4. Registered shutdown helper test in contract smoke guard-pack.

## Files

1. `crates/executor/src/main.rs`
2. `crates/executor/src/auth_verifier.rs`
3. `tools/executor_contract_smoke_test.sh`
4. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q shutdown_signal_`
3. `cargo test -p copybot-executor -q auth_verifier_hmac_keeps_nonce_through_forward_skew_window`
4. `cargo test -p copybot-executor -q auth_verifier_`
5. `bash tools/executor_contract_smoke_test.sh`
