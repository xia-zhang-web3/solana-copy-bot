# Executor Phase 2B Slice 151 — Ordering `M-1` Deferred Decision Record (2026-02-26)

## Scope

- formalize `M-1` status (`NO-GO for now`) in persistent governance docs to avoid repeated re-evaluation churn

## Decision

`M-1` (route-executor policy-first reorder) remains deferred by design.

## Rationale

1. Current canonical ordering matrix is closed (`N = 0`) for the approved chain.
2. Main handler paths already validate route/allowlist before route-executor.
3. Reorder now would change reject precedence and require matrix/smoke rebuild with low immediate runtime gain.

## Reopen Condition

1. Reopen only with explicit new product/security requirement requiring policy-first rejects at route-executor boundary.

## If Reopened (required sequence)

1. Approve new canonical guard chain first.
2. Rebuild ordering matrix to `N = 0` for the new order.
3. Sync integration guards, smoke registry, ROAD, and evidence notes.

## Files

- `ops/executor_ordering_coverage_policy.md`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. Policy file contains explicit `M-1` decision record and reopen criteria.
2. ROAD has synchronized decision entry (`311`).
