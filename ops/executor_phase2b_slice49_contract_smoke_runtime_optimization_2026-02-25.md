# Executor Phase 2B Slice 49 — Contract Smoke Runtime Optimization (2026-02-25)

## Scope

- reduce local audit/runtime feedback loop latency
- preserve contract-smoke guarantees (full suite pass + guard test presence)
- keep backward-compatible strict mode for deep audits

## Changes

1. Updated `tools/executor_contract_smoke_test.sh` guard verification strategy:
   - default path:
     1. run full suite once (`cargo test -p copybot-executor -q`)
     2. fetch test registry once (`cargo test -p copybot-executor -- --list`)
     3. assert every `contract_guard_tests` entry exists exactly (`^name: test$`)
2. Added opt-in strict mode:
   - `EXECUTOR_CONTRACT_SMOKE_RUN_EACH_GUARD=true`
   - keeps previous behavior (run every guard test by separate `cargo test` invocation)
3. Preserved output contract:
   - success message remains `[ok] contract guard tests pass`
   - final line remains `executor contract smoke: PASS`
4. Updated roadmap ledger.

## Files

- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `bash -n tools/executor_contract_smoke_test.sh` — PASS
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `cargo check -p copybot-executor -q` — PASS
