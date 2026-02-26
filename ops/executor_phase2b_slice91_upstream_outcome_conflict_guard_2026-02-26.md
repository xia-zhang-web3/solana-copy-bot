# Executor Phase 2B Slice 91 — Upstream Outcome Conflict Guard (2026-02-26)

## Scope

- harden upstream outcome classification against contradictory status/flag envelopes
- prevent ambiguous upstream payloads from being interpreted as valid business rejects/successes

## Changes

1. Updated `parse_upstream_outcome(...)` in `upstream_outcome.rs`:
   - added fail-closed conflict guard:
     - `status in {ok,accepted,success}` + any reject flag (`ok=false` or `accepted=false`) => terminal `upstream_invalid_response`
     - `status in {reject,rejected,error,failed,failure}` + any success flag (`ok=true` or `accepted=true`) => terminal `upstream_invalid_response`
2. Added unit coverage:
   - `upstream_outcome_rejects_conflicting_success_status_with_reject_flags`
   - `upstream_outcome_rejects_conflicting_reject_status_with_success_flags`
3. Added integration coverage in `main.rs`:
   - `handle_simulate_rejects_upstream_conflicting_status_flags`
4. Registered new guards in `tools/executor_contract_smoke_test.sh` and updated roadmap ledger.

## Files

- `crates/executor/src/upstream_outcome.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q upstream_outcome_rejects_conflicting_` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_upstream_conflicting_status_flags` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
