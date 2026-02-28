# Executor Phase 2B — Slice 187

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- harden `execution_fee_calibration_report.sh` by strict-parsing policy-echo bool setting before DB-heavy work.

## Changes

1. Ops runtime hardening (`tools/execution_fee_calibration_report.sh`):
   - replaced local boolean parser usage with source-aware strict helper:
     - `parse_bool_token_strict(...)`
     - `cfg_or_env_bool_setting(...)`
   - `execution.submit_adapter_require_policy_echo` now parses early (before DB path checks/column introspection/query blocks).
   - invalid tokens fail-close with explicit source context:
     - `env SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO`
     - `config [execution].submit_adapter_require_policy_echo`
2. Ops smoke (`tools/ops_scripts_smoke_test.sh`):
   - added guard:
     - `run_calibration_invalid_env_bool_case`
   - verifies invalid env token (`maybe`) fails with exit code 1 and expected source-aware error.
3. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item `347`.

## Validation

1. `bash -n tools/execution_fee_calibration_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. Targeted runtime check:
   - `SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO=maybe ... tools/execution_fee_calibration_report.sh` — FAIL-CLOSED (exit 1) with explicit source-aware error.
3. `cargo check -p copybot-executor -q` — PASS
4. `cargo test -p copybot-executor -q` — PASS (`625/625`)
5. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- calibration helper now rejects invalid policy-echo bool tokens before expensive DB work and reports actionable source context for faster operator triage.
