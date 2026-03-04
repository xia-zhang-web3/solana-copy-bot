# Executor Phase2B Slice 474-476 Evidence (2026-03-04)

## Scope

Batch objective: close false-green readiness/preflight when executor runs in `backend_mode=upstream` but upstream submit/simulate topology is missing or uses placeholder hosts.

Files changed:

1. `tools/executor_preflight.sh`
2. `tools/execution_go_nogo_report.sh`
3. `tools/ops_scripts_smoke_test.sh`
4. `ROAD_TO_PRODUCTION.md`

## Implemented changes

### 474) Executor preflight placeholder-host guard

`tools/executor_preflight.sh` now includes `endpoint_placeholder_host()` and fail-closes route-effective executor endpoints in `upstream` mode when host is:

1. `example.com` (`*.example.com`)
2. `executor.mock.local` (`*.executor.mock.local`)

Guard coverage applies to effective per-route:

1. submit primary/fallback
2. simulate primary/fallback
3. send-rpc primary/fallback

### 475) Go/No-Go strict upstream topology guard

`tools/execution_go_nogo_report.sh` now evaluates strict upstream topology when `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true`:

1. resolves effective per-route submit/simulate endpoints (`route override -> global default`)
2. classifies gate as:
   1. `PASS` (`topology_pass`)
   2. `UNKNOWN` (`endpoint_missing`)
   3. `WARN` (`endpoint_placeholder`)
3. emits summary fields:
   1. `executor_upstream_endpoint_guard_verdict`
   2. `executor_upstream_endpoint_guard_reason`
   3. `executor_upstream_endpoint_guard_reason_code`
4. fail-closes overall verdict on strict mode:
   1. `executor_upstream_topology_unknown`
   2. `executor_upstream_topology_not_pass`

### 476) Smoke coverage and strict fixture updates

`tools/ops_scripts_smoke_test.sh` updates:

1. new go/no-go pins:
   1. strict `upstream` + placeholder endpoint -> `NO_GO` (`executor_upstream_topology_not_pass`)
   2. strict `upstream` + missing submit/simulate topology -> `NO_GO` (`executor_upstream_topology_unknown`)
2. new executor preflight pins:
   1. `example.com` placeholder reject
   2. `executor.mock.local` placeholder reject
3. strict helper fixtures now include canonical non-placeholder upstream defaults:
   1. `COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL=http://127.0.0.1:18080/submit`
   2. `COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL=http://127.0.0.1:18080/simulate`

## Verification

Commands executed:

1. `bash -n tools/executor_preflight.sh tools/execution_go_nogo_report.sh tools/ops_scripts_smoke_test.sh`
2. `OPS_SMOKE_TARGET_CASES=go_nogo_executor_backend_mode_guard bash tools/ops_scripts_smoke_test.sh`
3. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh`
4. `OPS_SMOKE_TARGET_CASES="windowed_signoff,route_fee_signoff,devnet_rehearsal,adapter_rollout_evidence,execution_runtime_readiness" bash tools/ops_scripts_smoke_test.sh`
5. `cargo check -p copybot-executor -q`

Result: all PASS.

## Mapping

Maps to `ROAD_TO_PRODUCTION.md` next-code-queue item `5` (executor upstream backend closure hardening path), specifically strict fail-closed governance for upstream topology readiness checks.
