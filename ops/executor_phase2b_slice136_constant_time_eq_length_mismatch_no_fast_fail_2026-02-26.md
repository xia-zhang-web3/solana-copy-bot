# Executor Phase 2B Slice 136 — constant_time_eq Length-Mismatch No Fast-Fail (2026-02-26)

## Scope

- remove early-return fast-fail branch in `constant_time_eq` for length mismatch
- close residual crypto-hygiene low finding on auth compare primitive

## Changes

1. Updated `crates/executor/src/auth_crypto.rs`:
   - `constant_time_eq` no longer returns immediately when lengths differ
   - now accumulates diff across `max(left.len(), right.len())` with zero-padding via `get(...).unwrap_or(&0)`
   - length mismatch contributes to `diff` via `left.len() ^ right.len()`
2. Added unit test:
   - `constant_time_eq_rejects_length_mismatch`
3. Updated contract smoke guard registry:
   - added `constant_time_eq_rejects_length_mismatch`
4. Updated ROAD ledger item `296`.

## Files

- `crates/executor/src/auth_crypto.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q constant_time_eq_` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
