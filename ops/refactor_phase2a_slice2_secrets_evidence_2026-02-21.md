# Refactor Evidence: Phase 2a Slice 2 (`secrets`) (2026-02-21)

Branch:
- `refactor/phase-2a-app-safe`

## Scope
Move+wire extraction of execution adapter secret-resolution logic from `crates/app/src/main.rs` into `crates/app/src/secrets.rs`.

Extracted API:
1. `resolve_execution_adapter_secrets`

Moved helper logic:
1. `resolve_secret_file_path`
2. `read_trimmed_secret_file`

## Files Changed
1. `crates/app/src/main.rs`
2. `crates/app/src/secrets.rs`

## Validation Commands
1. `cargo fmt --all`
2. `cargo test -p copybot-app -q`
3. `cargo test --workspace -q`
4. `tools/ops_scripts_smoke_test.sh`
5. Secrets targeted tests:
   1. `cargo test -p copybot-app -q resolve_execution_adapter_secrets_reads_file_sources`
   2. `cargo test -p copybot-app -q resolve_execution_adapter_secrets_rejects_inline_and_file_conflict`
   3. `cargo test -p copybot-app -q resolve_execution_adapter_secrets_rejects_hmac_inline_and_file_conflict`
   4. `cargo test -p copybot-app -q resolve_execution_adapter_secrets_rejects_empty_secret_file`
   5. `cargo test -p copybot-app -q resolve_execution_adapter_secrets_rejects_missing_secret_file`
   6. `cargo test -p copybot-app -q resolve_execution_adapter_secrets_resolves_relative_paths_from_config_dir`

## Results
- All commands above: PASS.

## LOC Snapshot (post-slice)
From `tools/refactor_loc_report.sh`:
- `crates/app/src/main.rs`: raw `5087`, runtime (excl `#[cfg(test)]`) `2948`, cfg-test `2139`
- `crates/app/src/secrets.rs`: raw `72`, runtime `72`, cfg-test `0`

## Notes
- Error messages and fail-closed behavior are preserved (including missing/empty secret file paths and inline+file conflict semantics).
