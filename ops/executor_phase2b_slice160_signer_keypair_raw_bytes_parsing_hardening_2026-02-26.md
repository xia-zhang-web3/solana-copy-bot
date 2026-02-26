# Executor Phase 2B — Slice 160

Date: 2026-02-26  
Owner: execution-dev

## Scope

- harden signer keypair file parsing boundary to avoid intermediate UTF-8 string materialization.

## Changes

1. Runtime hardening (`crates/executor/src/signer_source.rs`):
   - switched keypair file read from `fs::read_to_string` to `fs::read`.
   - switched JSON decode from `serde_json::from_str` to `serde_json::from_slice`.
   - preserved fail-closed empty-after-trim behavior via byte-level ASCII-whitespace check.
   - preserved existing fail-closed 64-byte length and pubkey-mismatch checks.
2. Added guard test (`crates/executor/src/main.rs`):
   - `resolve_signer_source_config_rejects_non_json_keypair_payload`.
3. Added test helper for byte payload fixtures:
   - `write_temp_secret_file_bytes`.
4. Registered guard in contract smoke:
   - `tools/executor_contract_smoke_test.sh`.
5. Updated roadmap item 320 in `ROAD_TO_PRODUCTION.md`.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q resolve_signer_source_config_rejects_non_json_keypair_payload` — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- signer keypair loader now validates JSON from raw bytes and avoids unnecessary sensitive-string materialization in this boundary.
- guard coverage protects the non-JSON byte payload reject path.
