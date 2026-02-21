# Refactor Evidence: Phase 4 Slice 1 (`execution.submitter_response`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of adapter submit response parsing logic from `crates/execution/src/submitter.rs` into `crates/execution/src/submitter_response.rs`.

Extracted API:
1. `parse_adapter_submit_response`
2. `normalize_route`

Private helpers moved with identical logic:
1. `parse_rfc3339_utc`
2. `parse_optional_non_negative_u64_field`
3. `approx_f64_eq`

## Files Changed
1. `crates/execution/src/lib.rs`
2. `crates/execution/src/submitter.rs`
3. `crates/execution/src/submitter_response.rs`
4. `ops/refactor_phase4_slice1_submitter_response_evidence_2026-02-21.md`

## Validation Commands
1. `cargo fmt --all`
2. `cargo test -p copybot-execution -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`
5. `cargo test -p copybot-execution -q adapter_intent_simulator_does_not_fallback_on_invalid_json_terminal_reject`
6. `cargo test -p copybot-execution -q adapter_intent_simulator_redacts_endpoint_on_retryable_send_error`
7. `cargo test -p copybot-execution -q parse_adapter_submit_response_rejects_missing_required_policy_echo`
8. `cargo test -p copybot-execution -q parse_adapter_submit_response_returns_retryable_on_retryable_reject`
9. `cargo test -p copybot-execution -q parse_adapter_submit_response_returns_terminal_on_terminal_reject`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/execution/src/submitter.rs`: raw `1644`, runtime (excl `#[cfg(test)]`) `588`, cfg-test `1056`
- `crates/execution/src/submitter_response.rs`: raw `388`, runtime `388`, cfg-test `0`

## Notes
- Retryable/terminal mapping behavior is unchanged; response parser tests remained in `submitter.rs` and continue to pass.
- `normalize_route` moved to parser module and is reused by both submit call-path and parser validation to preserve normalization parity.
