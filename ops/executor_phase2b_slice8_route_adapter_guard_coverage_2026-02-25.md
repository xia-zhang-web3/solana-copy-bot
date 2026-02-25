# Executor Phase 2B Slice 8 Evidence (2026-02-25)

## Scope

Close remaining low-severity coverage gaps reported by audit for route-adapter submit payload guards.

## Implemented

1. `crates/executor/src/route_adapters.rs` test coverage expansion:
   1. Added `validate_submit_payload_for_route_rejects_missing_route`.
   2. Added `validate_submit_payload_for_route_rejects_non_string_route`.
2. Integration guard coverage in `crates/executor/src/main.rs`:
   1. Added `handle_submit_rejects_route_payload_mismatch_before_forward`.
   2. This test uses mismatched request-vs-raw submit route and asserts fail-closed `invalid_request_body` before any forwarding path.
3. `tools/executor_contract_smoke_test.sh` guard list updated with all three tests.

## Effect

1. Route-adapter submit payload guard now has explicit branch coverage for:
   1. missing `route`,
   2. non-string `route`,
   3. mismatched `route`.
2. Integration-level evidence confirms mismatch is rejected at adapter boundary prior to upstream forwarding.

## Regression Pack (quick)

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_missing_route` — PASS
3. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_non_string_route` — PASS
4. `cargo test -p copybot-executor -q handle_submit_rejects_route_payload_mismatch_before_forward` — PASS
5. `cargo test -p copybot-executor -q route_executor_` — PASS
6. `cargo test -p copybot-executor -q validate_rpc_submit_tip_payload_rejects_invalid_json` — PASS
7. `cargo test -p copybot-executor -q validate_rpc_submit_tip_payload_rejects_non_object_payload` — PASS
8. `bash -n tools/executor_contract_smoke_test.sh` — PASS
