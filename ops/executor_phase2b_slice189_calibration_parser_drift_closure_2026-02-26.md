# Executor Phase 2B — Slice 189

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering-chain changes).

## Scope

- close residual parser drift in calibration helper by removing local `parse_bool_token_strict` duplicate and using shared helper from `tools/lib/common.sh`.

## Changes

1. Ops runtime hardening (`tools/execution_fee_calibration_report.sh`):
   - added shared helper source:
     - `SCRIPT_DIR=...`
     - `source "$SCRIPT_DIR/lib/common.sh"`
   - removed local `parse_bool_token_strict()` implementation.
   - switched local trimming callsites to shared `trim_string(...)` where touched.
2. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item `349` documenting parser-drift closure.

## Validation

1. `bash -n tools/lib/common.sh tools/execution_fee_calibration_report.sh` — PASS
2. Targeted invalid env token check:
   - `SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO=maybe ... execution_fee_calibration_report.sh` — exit `1`, explicit source-aware env error.
3. Targeted invalid config token check:
   - `execution.submit_adapter_require_policy_echo = "maybe"` in temp config — exit `1`, explicit source-aware config error.
4. `cargo check -p copybot-executor -q` — PASS
5. `cargo test -p copybot-executor -q` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- strict bool parsing contract is now single-sourced for calibration + rollout/go-no-go helpers, removing same-name semantic drift risk while preserving fail-closed behavior.
