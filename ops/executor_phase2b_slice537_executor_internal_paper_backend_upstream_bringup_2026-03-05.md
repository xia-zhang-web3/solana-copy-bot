# Slice 537 — Executor Internal Paper Backend in Upstream Mode

Date: 2026-03-05
Owner: execution-dev
Type: runtime/e2e progress (non-coverage-only)
Ordering policy checkpoint: `N=0` (`ops/executor_ordering_coverage_policy.md`)

## Scope

1. `crates/executor/src/route_adapters.rs`
2. `crates/executor/src/main.rs`
3. `crates/executor/src/executor_config_env.rs`
4. `tools/executor_preflight.sh`
5. `tools/execution_go_nogo_report.sh`
6. `ROAD_TO_PRODUCTION.md`

## Changes

1. Added internal paper route backend responses in executor route adapters:
   1. `simulate` returns contract-compatible success payload with `detail=executor_paper_simulation_ok`.
   2. `submit` returns contract-compatible success payload with `detail=executor_paper_submit_ok` and deterministic `tx_signature`.
2. Added deterministic paper submit signature builder (hash-based, stable by request identity fields) and tests for payload contract fields/signature determinism.
3. Added route-action integration tests to pin that upstream-mode paper actions are served by internal backend (no external forward required).
4. Relaxed executor env parsing for paper route in upstream mode:
   1. when route is `paper` and submit/simulate URL is not configured, config now synthesizes internal paper endpoints (`https://executor.paper.local/paper/{submit|simulate}`),
   2. keeps upstream mode valid for paper-only non-live bring-up.
5. Brought script parity to runtime behavior:
   1. `tools/executor_preflight.sh` now applies the same internal paper endpoint fallback during topology checks,
   2. `tools/execution_go_nogo_report.sh` now applies the same fallback in strict upstream topology guard resolution.

## Validation

1. `timeout 120 cargo check -p copybot-executor -q` — PASS
2. `bash -n tools/executor_preflight.sh tools/execution_go_nogo_report.sh` — PASS
3. `timeout 120 cargo test -p copybot-executor -q internal_paper_backend` — PASS
4. `timeout 120 cargo test -p copybot-executor -q paper_submit_backend_response` — PASS
5. `timeout 120 cargo test -p copybot-executor -q paper_simulate_backend_response` — PASS
6. `timeout 120 cargo test -p copybot-executor -q allows_upstream_mode_with_paper_route_without_upstream_urls` — PASS
7. `timeout 120 env OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES=go_nogo_executor_backend_mode_guard bash tools/ops_scripts_smoke_test.sh` — PASS
8. `timeout 120 env OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS

## Notes

1. This slice enables practical non-live server bring-up for paper route without requiring external upstream submit/simulate endpoints, while preserving strict topology contracts for non-paper routes.
