# Executor Phase 2B Slice 62 — Route-Layer Allowlist Guard (2026-02-25)

## Scope

- harden route-layer policy enforcement to match common-contract allowlist semantics
- prevent direct `execute_route_action` usage from bypassing route allowlist

## Changes

1. Added route-layer allowlist validator in `route_executor`:
   - rejects non-allowlisted routes with `route_not_allowed`
2. Wired allowlist check into `execute_route_action(...)` before feature-gate/context/adapter dispatch.
3. Added unit coverage:
   - `route_executor_allowlist_rejects_route_not_in_allowlist`
   - `route_executor_allowlist_accepts_route_in_allowlist`
4. Added integration pre-forward guard:
   - `execute_route_action_rejects_route_not_in_allowlist_before_forward`
   - scenario uses known route (`jito`) with backend configured but absent from allowlist.
5. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/route_executor.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q route_executor_allowlist_` — PASS
3. `cargo test -p copybot-executor -q execute_route_action_rejects_route_not_in_allowlist_before_forward` — PASS
4. `cargo test -p copybot-executor -q route_executor_action_context_` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
