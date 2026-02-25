# Executor Phase 2B Slice 12 Evidence (2026-02-25)

## Scope

Close audit-reported submit action guard test gap and extend adapter-boundary action coverage for submit path.

## Implemented

1. `crates/executor/src/route_adapters.rs`:
1. Added unit test `validate_submit_payload_for_route_rejects_non_string_action_when_present`.
1. Added unit test `validate_submit_payload_for_route_accepts_matching_action_case_insensitive_when_present`.
1. `crates/executor/src/main.rs`:
1. Added integration test `handle_submit_rejects_non_string_action_payload_before_forward`.
1. `tools/executor_contract_smoke_test.sh`:
1. Added all new tests to `contract_guard_tests`.

## Effect

1. Submit action guard now has explicit branch coverage for:
1. non-string `action` reject (`invalid_request_body`),
1. normalized/case-insensitive `action=submit` accept.
1. Integration guard proves non-string submit action is rejected before upstream network I/O.

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_rejects_non_string_action_when_present` — PASS
1. `cargo test -p copybot-executor -q validate_submit_payload_for_route_accepts_matching_action_case_insensitive_when_present` — PASS
1. `cargo test -p copybot-executor -q handle_submit_rejects_non_string_action_payload_before_forward` — PASS
1. `cargo test -p copybot-executor -q route_adapter_` — PASS
1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
1. `bash tools/executor_contract_smoke_test.sh` — PASS
