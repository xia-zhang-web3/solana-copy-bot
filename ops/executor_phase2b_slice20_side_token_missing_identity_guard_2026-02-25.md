# Executor Phase 2B Slice 20 Evidence (2026-02-25)

## Scope

1. Hardened route-adapter identity helper to fail-closed when `expected_value` is present and payload field is missing.
2. Added unit coverage for missing `side`/`token` expected-value branches in submit/simulate adapter validators.
3. Added integration pre-forward coverage for missing `side`/`token` payload branches in `/simulate` and `/submit`.
4. Updated executor contract smoke guard list and roadmap ledger.

## Files

1. `crates/executor/src/route_adapters.rs`
2. `crates/executor/src/main.rs`
3. `tools/executor_contract_smoke_test.sh`
4. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_missing_side_when_expected`
3. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_missing_token_when_expected`
4. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_missing_side_when_expected`
5. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_missing_token_when_expected`
6. `cargo test -p copybot-executor -q handle_simulate_rejects_missing_side_payload_before_forward`
7. `cargo test -p copybot-executor -q handle_simulate_rejects_missing_token_payload_before_forward`
8. `cargo test -p copybot-executor -q handle_submit_rejects_missing_side_payload_before_forward`
9. `cargo test -p copybot-executor -q handle_submit_rejects_missing_token_payload_before_forward`
10. `bash -n tools/executor_contract_smoke_test.sh`
11. `bash tools/executor_contract_smoke_test.sh`
