# Executor Phase 2 Slice 30 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by moving duplicated HTTP ingress guard logic out of `main.rs` into a shared module.

## Implemented

1. Added `crates/executor/src/request_ingress.rs`:
   1. `verify_auth_or_reject`:
      1. wraps `AuthVerifier::verify`,
      2. returns standard reject payload (`StatusCode::OK` + `reject_to_json`) on auth failure.
   2. `parse_json_or_reject<T>`:
      1. wraps serde body parse,
      2. returns standard `invalid_json` reject payload on parse failure.
2. Updated `crates/executor/src/main.rs`:
   1. `/simulate` and `/submit` now call shared ingress helpers instead of duplicated inline blocks.
   2. Existing endpoint behavior preserved (same status code and reject body shape).
3. Added module tests in `request_ingress.rs`:
   1. invalid JSON path,
   2. auth reject path,
   3. valid auth pass-through path.
4. Updated `tools/executor_contract_smoke_test.sh`:
   1. added direct module guard test entries for ingress helpers.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q request_ingress_` — PASS
2. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
