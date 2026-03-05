# Slice 528 — submit-verify strict propagation into signoff/rehearsal/final chain

## Scope
- `tools/execution_devnet_rehearsal.sh`
- `tools/execution_windowed_signoff_report.sh`
- `tools/execution_route_fee_signoff_report.sh`
- `tools/execution_route_fee_final_evidence_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Goal
Close residual observability/parity gap for `GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT` outside direct go/no-go and executor-rollout path:
- propagate strict flag through rehearsal/signoff/final evidence chain,
- emit nested `submit_verify_guard_*` fields in summaries,
- fail-close on malformed or drifted nested values,
- keep relaxed-mode (`false`) contract deterministic (`SKIP/gate_disabled`).

## Implemented
1. Added strict flag parse/propagation to nested go/no-go calls:
   - `execution_devnet_rehearsal.sh`
   - `execution_windowed_signoff_report.sh`
   - `execution_route_fee_signoff_report.sh`
   - `execution_route_fee_final_evidence_report.sh`
2. Added nested extraction/validation for:
   - `go_nogo_require_submit_verify_strict`
   - `submit_verify_guard_verdict`
   - `submit_verify_guard_reason_code`
3. Added parity checks:
   - strict required (`true`) => guard verdict must be non-`SKIP`
   - strict relaxed (`false`) => guard verdict must be `SKIP`
4. Extended dynamic guard keying in route-fee final evidence:
   - supports `WINDOWS_CSV` custom last-window extraction for submit-verify fields.
5. Expanded smoke assertions to pin submit-verify fields in:
   - `windowed_signoff` (HOLD/GO)
   - `route_fee_signoff` (HOLD + final package HOLD/GO/1,6/bundle)
   - `devnet_rehearsal` baseline path (`go_nogo_submit_verify_guard_*`)

## Validation
- `bash -n tools/execution_go_nogo_report.sh tools/execution_server_rollout_report.sh tools/executor_rollout_evidence_report.sh tools/executor_final_evidence_report.sh tools/execution_devnet_rehearsal.sh tools/execution_windowed_signoff_report.sh tools/execution_route_fee_signoff_report.sh tools/execution_route_fee_final_evidence_report.sh tools/ops_scripts_smoke_test.sh`
- `cargo check -p copybot-executor -q`
- `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=go_nogo_executor_backend_mode_guard bash tools/ops_scripts_smoke_test.sh`
- `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=windowed_signoff bash tools/ops_scripts_smoke_test.sh`
- `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=route_fee_signoff bash tools/ops_scripts_smoke_test.sh`
- `OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES=devnet_rehearsal bash tools/ops_scripts_smoke_test.sh`
- `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=execution_runtime_readiness bash tools/ops_scripts_smoke_test.sh`
- `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=execution_server_rollout bash tools/ops_scripts_smoke_test.sh`

All commands passed.
