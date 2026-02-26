# Executor Phase 2B Slice 114 — Payload-Shape Ordering Matrix Closure (2026-02-26)

## Scope

- close remaining ordering matrix v1 residuals (`N=2 -> N=0`)
- add missing integration guards for payload-shape priority symmetry

## Changes

1. Added integration guard test for submit allowlist priority:
   - `execute_route_action_rejects_submit_payload_shape_before_allowlist_check`
   - proves `payload_shape` reject fires before `allowlist` on submit path.
2. Added integration guard test for simulate feature-gate priority:
   - `execute_route_action_rejects_simulate_payload_shape_before_fastlane_feature_gate`
   - proves `payload_shape` reject fires before `feature_gate` on simulate path.
3. Updated contract smoke registry with both new guard tests.
4. Updated ordering governance matrix in `ops/executor_ordering_coverage_policy.md`:
   - `payload_shape > allowlist` moved to `done` with submit+simulate symmetry.
   - `payload_shape > feature_gate` moved to `done` with submit+simulate symmetry.
   - residual count updated to `N = 0`.
5. Updated `ROAD_TO_PRODUCTION.md` ledger with item `273`.

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ops/executor_ordering_coverage_policy.md`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q execute_route_action_rejects_submit_payload_shape_before_allowlist_check` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_simulate_payload_shape_before_fastlane_feature_gate` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
