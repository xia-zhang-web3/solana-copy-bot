# Executor Phase 2A Post-Audit Build Fix Evidence (2026-02-24)

## Scope

Fix non-test build regression reported after Phase 2A extraction baseline.

## Findings addressed

1. Runtime router import regression:
   1. `simulate` import was accidentally test-scoped (`#[cfg(test)]`) while used by runtime router.
2. Root-scope send-rpc dependency leak:
   1. `submit_handler` depended on `crate::send_signed_transaction_via_rpc` re-exported by `main.rs` import scope.

## Implemented

1. `crates/executor/src/main.rs`:
   1. restored runtime import for `use crate::request_endpoints::simulate;` (non-test).
2. `crates/executor/src/submit_handler.rs`:
   1. switched to direct dependency import `use crate::send_rpc::send_signed_transaction_via_rpc;`.
3. Import hygiene cleanup after fix:
   1. test-only imports moved under `#[cfg(test)]` where appropriate,
   2. dead-code warning removed by test-gating `simulate_http_status_for_reject` in `crates/executor/src/reject_mapping.rs`.

## Result

1. `cargo check -p copybot-executor -q` — PASS (non-test build restored).
2. Runtime router wiring compiles with `/simulate` + `/submit` endpoints.
3. Contract quick checks remain green.

## Regression Pack (quick)

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q handle_submit_rejects_empty_signal_id` — PASS
3. `cargo test -p copybot-executor -q handle_simulate_rejects_empty_request_id` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
