# Executor Phase 3 Slice 7 Evidence — Claim TTL Aligned with Runtime HTTP Timeout Floor

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Scope

1. Fixed claim TTL lower-bound calculation mismatch:
   1. `min_claim_ttl_sec_for_submit_path` now uses effective timeout floor `request_timeout_ms.max(500)`.
   2. This matches actual reqwest client timeout behavior in runtime.
2. Updated config error detail to report derived bound from effective timeout floor.
3. Added unit test:
   1. `min_claim_ttl_sec_for_submit_path_applies_500ms_runtime_floor`.

## Files

1. `crates/executor/src/main.rs`

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS (74)
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `cargo test --workspace -q` — PASS

