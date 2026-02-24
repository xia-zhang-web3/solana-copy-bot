# Executor Phase 2 Slice 20 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by moving upstream forwarding logic out of `main.rs` into a dedicated module.

## Implemented

1. `crates/executor/src/upstream_forward.rs`:
   1. Added shared forwarding entrypoint:
      1. `forward_to_upstream(state, route, action, raw_body, submit_deadline)`
   2. Preserved existing behavior:
      1. route-not-configured fail-closed reject,
      2. retryable fallback chain for transport/429/5xx,
      3. terminal reject for non-retryable HTTP statuses,
      4. fail-closed on invalid JSON,
      5. submit-deadline per-hop timeout enforcement.
2. `crates/executor/src/main.rs`:
   1. Added module wiring:
      1. `mod upstream_forward`
   2. Rewired simulate/submit handlers to shared forwarding import.
   3. Removed duplicated inline `forward_to_upstream` implementation.
3. `tools/executor_contract_smoke_test.sh`:
   1. Added guard coverage for forwarding semantics:
      1. `forward_to_upstream_uses_fallback_after_primary_retryable_status`
      2. `forward_to_upstream_does_not_fallback_after_primary_terminal_status`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q forward_to_upstream_uses_fallback_after_primary_retryable_status` — PASS
2. `cargo test -p copybot-executor -q forward_to_upstream_does_not_fallback_after_primary_terminal_status` — PASS
3. `cargo test -p copybot-executor -q forward_to_upstream_uses_fallback_auth_token_when_retrying` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
