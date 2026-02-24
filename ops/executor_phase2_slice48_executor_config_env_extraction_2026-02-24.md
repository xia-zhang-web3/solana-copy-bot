# Executor Phase 2 Slice 48 Evidence (2026-02-24)

## Scope

Extract `ExecutorConfig::from_env()` startup parsing/validation logic out of `main.rs`.

## Implemented

1. Added `crates/executor/src/executor_config_env.rs` with `impl ExecutorConfig { from_env() }`.
2. Moved unchanged startup config logic from `main.rs` into new module:
   1. bind/contract/signer parsing,
   2. route allowlist + per-route backend topology validation,
   3. secret source resolution,
   4. auth mode/HMAC validation,
   5. timeout + submit budget + idempotency claim TTL guards,
   6. submit-verify config parsing and min-claim-ttl invariant.
3. `main.rs` now calls the same `ExecutorConfig::from_env()` method provided by extracted module.

## Effect

1. `main.rs` no longer contains large startup parsing block.
2. Startup fail-closed behavior remains unchanged.
3. Configuration concerns are isolated for further phase-2 cleanup.

## Regression Pack (quick)

1. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
2. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
