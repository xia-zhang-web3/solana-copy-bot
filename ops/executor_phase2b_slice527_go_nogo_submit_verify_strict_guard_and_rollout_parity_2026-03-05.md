# Slice 527 — go/no-go strict submit-verify guard + rollout parity wiring

## Scope

1. `tools/execution_go_nogo_report.sh`
   - Added strict bool parse for `GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT` (fail-closed).
   - Added submit-verify strict guard with observability fields:
     - `executor_submit_verify_strict_observed`
     - `executor_submit_verify_configured`
     - `executor_submit_verify_fallback_configured`
     - `submit_verify_guard_verdict`
     - `submit_verify_guard_reason`
     - `submit_verify_guard_reason_code`
   - Guard behavior when strict mode is required:
     - `WARN` when strict submit-verify is not enabled.
     - `UNKNOWN` on malformed strict token or missing required topology.
     - `WARN` on fallback=primary or placeholder verify endpoints in upstream mode.
     - `PASS` only for enabled+configured valid topology.
   - Integrated into overall verdict chain:
     - `UNKNOWN => NO_GO (submit_verify_guard_unknown)`
     - `WARN => NO_GO (submit_verify_guard_not_pass)`
     - `PASS` required in all-pass predicate when strict mode is enabled.

2. `tools/execution_server_rollout_report.sh`
   - Kept strict submit-verify boolean parity extraction for direct go/no-go and executor-final nested rollout fields.
   - Propagation retained only for direct go/no-go + executor-final chain (removed unnecessary passthrough to branches that do not consume this gate).

3. `tools/ops_scripts_smoke_test.sh`
   - Extended `go_nogo_executor_backend_mode_guard` case with strict submit-verify matrix:
     - strict submit-verify `PASS`
     - strict submit-verify `WARN` (`submit_verify_strict_not_enabled`)
     - strict submit-verify `UNKNOWN` (`submit_verify_strict_invalid`)
     - invalid top-level bool token reject for `GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT`
   - Added parity assertions for `go_nogo_require_submit_verify_strict` in rollout/final/server-rollout baseline flows.
   - Added invalid bool reject pins for `GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT` in:
     - `executor_rollout_evidence`
     - `executor_final_evidence`
     - `execution_server_rollout`

4. `ROAD_TO_PRODUCTION.md`
   - Added item `527`.

## Validation

1. `bash -n tools/execution_go_nogo_report.sh tools/ops_scripts_smoke_test.sh tools/execution_server_rollout_report.sh tools/executor_rollout_evidence_report.sh tools/executor_final_evidence_report.sh` — PASS
2. `OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES="go_nogo_executor_backend_mode_guard,executor_rollout_evidence_fast,execution_server_rollout_fast" bash tools/ops_scripts_smoke_test.sh` — PASS
3. `cargo check -p copybot-executor -q` — PASS

## Notes

- This slice is fail-closed governance hardening and parity observability only.
- No runtime Rust execution path changed.
