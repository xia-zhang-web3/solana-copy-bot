# Executor Phase 2B Slice 152 — Env Parsing UTF-8 Boundary Completion (2026-02-26)

## Scope

- complete UTF-8 boundary semantics in `env_parsing` helpers used by executor startup config

## Changes

1. `env_parsing.rs`:
   - `non_empty_env` now handles env errors explicitly:
     - `NotPresent` -> `"... is required"`
     - `NotUnicode` -> `"... must be valid UTF-8 string"`
   - added unix unit guards:
     - `non_empty_env_rejects_non_utf8_value`
     - `optional_non_empty_env_rejects_non_utf8_value`
2. `tools/executor_contract_smoke_test.sh`:
   - registered both parser-level UTF-8 guard tests
3. `ROAD_TO_PRODUCTION.md`:
   - added item `312` for env parsing boundary completion

## Files

- `crates/executor/src/env_parsing.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q non_empty_env_rejects_non_utf8_value` — PASS
3. `cargo test -p copybot-executor -q optional_non_empty_env_rejects_non_utf8_value` — PASS
4. `cargo test -p copybot-executor -q` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
