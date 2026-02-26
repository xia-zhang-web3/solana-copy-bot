# Executor Ordering Coverage Policy (Persistent)

Date: 2026-02-26  
Owner: execution-dev  
Applies to: `crates/executor/src/route_executor.rs` guard-ordering work

## 1) Why this exists

This policy prevents endless coverage-only growth by fixing:

1. a finite ordering matrix scope,
2. explicit residual gap count,
3. strict closure criteria,
4. post-closure rules for when new ordering tests are allowed.

## 2) Canonical Guard Chain

`execute_route_action` ordering (current canonical):

1. `payload_route`
2. `payload_shape`
3. `deadline_context`
4. `action_context`
5. `allowlist`
6. `backend`
7. `feature_gate`
8. adapter execution

## 3) Matrix v1 (finite scope)

Status key:

1. `done`: both submit and simulate covered where semantics differ by action
2. `partial`: one action covered, symmetric pair missing

| Priority pair | Submit coverage | Simulate coverage | Status |
|---|---|---|---|
| `payload_route > payload_shape` | `execute_route_action_rejects_route_hint_before_payload_shape_on_submit` | `execute_route_action_rejects_route_hint_before_payload_shape_on_simulate` | done |
| `payload_route > deadline_context` | `execute_route_action_rejects_route_hint_before_deadline_context_on_submit` | `execute_route_action_rejects_route_hint_before_deadline_context_on_simulate` | done |
| `payload_route > action_context` | `execute_route_action_rejects_route_hint_before_action_context_on_submit` | `execute_route_action_rejects_route_hint_before_action_context_on_simulate` | done |
| `payload_route > allowlist` | `execute_route_action_rejects_submit_route_hint_mismatch_before_allowlist_check` | `execute_route_action_rejects_route_hint_mismatch_before_allowlist_check` | done |
| `payload_route > backend` | `execute_route_action_rejects_submit_route_hint_missing_before_backend_check` | `execute_route_action_rejects_route_hint_missing_before_backend_check` | done |
| `payload_route > feature_gate` | `execute_route_action_rejects_route_hint_before_fastlane_feature_gate_on_submit` | `execute_route_action_rejects_route_hint_before_fastlane_feature_gate_on_simulate` | done |
| `payload_shape > deadline_context` | `execute_route_action_rejects_payload_shape_before_deadline_context_on_submit` | `execute_route_action_rejects_payload_shape_before_deadline_context_on_simulate` | done |
| `payload_shape > action_context` | `execute_route_action_rejects_payload_shape_before_action_context_on_submit` | `execute_route_action_rejects_payload_shape_before_action_context_on_simulate` | done |
| `payload_shape > allowlist` | `execute_route_action_rejects_submit_payload_shape_before_allowlist_check` | `execute_route_action_rejects_payload_shape_before_allowlist_check` | done |
| `payload_shape > feature_gate` | `execute_route_action_rejects_payload_shape_before_fastlane_feature_gate` | `execute_route_action_rejects_simulate_payload_shape_before_fastlane_feature_gate` | done |
| `deadline_context > action_context` | `execute_route_action_rejects_deadline_context_before_action_context_on_submit` | `execute_route_action_rejects_deadline_context_before_action_context_on_simulate` | done |
| `action_context > allowlist` | `execute_route_action_rejects_action_context_before_allowlist_check` | `execute_route_action_rejects_simulate_action_context_before_allowlist_check` | done |
| `action_context > backend` | `execute_route_action_rejects_action_context_before_backend_check` | `execute_route_action_rejects_simulate_action_context_before_backend_check` | done |
| `action_context > feature_gate` | `execute_route_action_rejects_action_context_before_fastlane_feature_gate` | `execute_route_action_rejects_simulate_action_context_before_fastlane_feature_gate` | done |

### Current residual count

`N = 0` (matrix v1 fully covered).

## 4) Exit Criteria for Ordering Hardening

Ordering hardening is considered closed only when both conditions hold:

1. Matrix v1 residual count reaches `N = 0`.
2. Two consecutive independent audit cycles report no low-gap findings on ordering coverage.

## 5) Post-Closure Rule (Coverage Freeze)

After closure:

1. New ordering test is allowed only together with a new runtime guard branch or a guard-order change.
2. Pure coverage-only ordering slices are disallowed if runtime logic is unchanged.

## 6) Throughput KPI (to prevent moving target)

Until closure:

1. No more than one coverage-only slice in a row.
2. At least every second slice must be runtime/e2e progress tied to `ROAD_TO_PRODUCTION.md` next-code-queue items.

## 7) Session Recovery Rule

Any new agent/session continuing executor work must:

1. read this file first,
2. report current residual count `N`,
3. state whether the next slice is `coverage-only` or `runtime/e2e`,
4. map it to next-code-queue before coding.

## 8) Decision Record — Deferred `M-1` Reorder

Date: 2026-02-26

Decision: `NO-GO` for now (deferred by design).

Context:

1. Matrix v1 is already closed (`N = 0`) for the current canonical guard chain.
2. Main submit/simulate handler paths already enforce route/allowlist checks before route-executor invocation.
3. Reordering to policy-first (`allowlist/backend` before payload/context) would change reject precedence, require matrix rebuild, and create high churn for low immediate runtime gain.

Revisit condition:

1. Re-open only if there is an explicit new product/security requirement that mandates policy-first rejects at route-executor boundary for all call-sites.

If reopened:

1. Approve new canonical order first.
2. Rebuild ordering matrix to `N = 0` under the new order.
3. Sync integration guards + smoke + ROAD/evidence before closure.
