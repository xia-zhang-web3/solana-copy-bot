# Executor Phase 2B Slice 19 Evidence (2026-02-25)

## Scope

1. Added missing integration pre-forward guards for adapter-boundary `side` and `token` identity mismatch on both `/simulate` and `/submit`.
2. Expanded unit coverage for route-adapter `side`/`token` expected-value mismatch branches across both actions.
3. Updated executor contract smoke guard pack with the new test names.
4. Updated roadmap ledger entry (#177).

## Files

1. `crates/executor/src/main.rs`
2. `crates/executor/src/route_adapters.rs`
3. `tools/executor_contract_smoke_test.sh`
4. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q side_payload_mismatch_before_forward`
3. `cargo test -p copybot-executor -q token_payload_mismatch_before_forward`
4. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_token_mismatch_when_expected`
5. `cargo test -p copybot-executor -q validate_simulate_payload_for_route_rejects_side_mismatch_when_expected`
6. `bash -n tools/executor_contract_smoke_test.sh`
7. `bash tools/executor_contract_smoke_test.sh`
