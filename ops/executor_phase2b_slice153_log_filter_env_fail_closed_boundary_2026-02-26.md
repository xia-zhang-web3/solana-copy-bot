# Executor Phase 2B Slice 153 — Log Filter Env Fail-Closed Boundary (2026-02-26)

## Scope

- harden startup log-filter env handling to avoid silent fallback on malformed configured values

## Changes

1. `main.rs`:
   - extracted `parse_executor_log_env_filter() -> Result<EnvFilter>`
   - semantics:
     - `COPYBOT_EXECUTOR_LOG_FILTER` missing -> use default filter (`info,reqwest=warn,hyper=warn,h2=warn`)
     - non-UTF8 value -> startup error (fail-closed)
     - invalid tracing filter syntax -> startup error (fail-closed)
   - main startup now uses parsed filter directly
2. Tests in `main.rs`:
   - `parse_executor_log_env_filter_uses_default_when_missing`
   - `parse_executor_log_env_filter_rejects_invalid_syntax`
   - `parse_executor_log_env_filter_rejects_non_utf8` (`#[cfg(unix)]`)
3. Contract smoke:
   - registered all three new guard tests

## Files

- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q parse_executor_log_env_filter_uses_default_when_missing` — PASS
3. `cargo test -p copybot-executor -q parse_executor_log_env_filter_rejects_invalid_syntax` — PASS
4. `cargo test -p copybot-executor -q parse_executor_log_env_filter_rejects_non_utf8` — PASS
5. `cargo test -p copybot-executor -q` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
