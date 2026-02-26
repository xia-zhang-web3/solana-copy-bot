# Executor Phase 2B — Slice 171

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering changes).

## Scope

- extend full submit pipeline coverage to pin explicit send-rpc oversized failure causes when no send-rpc fallback endpoint exists.

## Changes

1. Integration coverage (`crates/executor/src/main.rs`):
   - added:
     - `handle_submit_rejects_when_send_rpc_primary_declared_oversized_without_fallback`
     - `handle_submit_rejects_when_send_rpc_primary_truncated_without_fallback`
   - both tests run full `handle_submit` chain:
     - upstream `/submit` success (`signed_tx_base64`),
     - send-rpc primary failure on oversized success-body class,
     - no send-rpc fallback endpoint configured.
   - assertions pin explicit cause:
     - `code=send_rpc_response_too_large`
     - declared case contains `declared content-length` and `max_bytes=65536`
     - truncated case contains `max_bytes=65536`
2. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered both new cause guards.
3. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 331.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q handle_submit_rejects_when_send_rpc_primary_declared_oversized_without_fallback` — PASS
   - `cargo test -p copybot-executor -q handle_submit_rejects_when_send_rpc_primary_truncated_without_fallback` — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- full submit chain now has symmetric cause-level regression guards for both oversized classes at `send_rpc` and `submit_verify` boundaries, reducing false-green space in fallback-success coverage.
