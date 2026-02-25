# Executor Phase 2B Slice 48 — Cleanup Marker Skew Clamp (2026-02-25)

## Scope

- harden shared cleanup metadata handling under cross-instance clock skew
- prevent future-dated global markers from suppressing cleanup cadence locally
- add explicit regression coverage for claim/response marker clamp behavior

## Changes

1. Added helper:
   - `clamp_cleanup_marker_to_now(marker_unix, now_unix)`
2. Claim cleanup path now clamps global marker before cadence checks:
   - `load_cached_or_claim_submit(...)`
3. Response cleanup path now clamps global marker before cadence checks:
   - `run_response_cleanup_if_due_internal(...)`
4. Added guard tests:
   - `clamp_cleanup_marker_to_now_limits_future_marker`
   - `claim_cleanup_clamps_future_global_marker_to_now`
   - `response_cleanup_clamps_future_global_marker_to_now`
5. Added claim-marker test helper:
   - `claim_cleanup_marker_value(...)`
6. Added new guards to `tools/executor_contract_smoke_test.sh`.
7. Updated roadmap ledger.

## Files

- `crates/executor/src/idempotency.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q clamp_cleanup_marker_to_now_limits_future_marker` — PASS
3. `cargo test -p copybot-executor -q claim_cleanup_clamps_future_global_marker_to_now` — PASS
4. `cargo test -p copybot-executor -q response_cleanup_clamps_future_global_marker_to_now` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
