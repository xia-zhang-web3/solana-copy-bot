# Executor Phase 2B — Slice 173

Date: 2026-02-26  
Owner: execution-dev

## Governance

- ordering policy checked first (`ops/executor_ordering_coverage_policy.md`).
- ordering residual count remains `N=0` (no ordering changes).

## Scope

- harden signer file validation comparison path by removing direct pubkey byte inequality check in favor of constant-time compare.

## Changes

1. Runtime hardening (`crates/executor/src/signer_source.rs`):
   - signer pubkey mismatch check switched from direct slice comparison:
     - before: `expected_pubkey_bytes.as_slice() != &keypair_bytes[32..64]`
     - after: `!constant_time_eq(expected_pubkey_bytes.as_slice(), &keypair_bytes[32..64])`
   - uses existing hardened `auth_crypto::constant_time_eq` helper.
2. Guard coverage (`crates/executor/src/main.rs`):
   - added `resolve_signer_source_config_accepts_file_source_with_matching_pubkey`
   - pins positive file-source path using matching base58 pubkey derived from fixture bytes.
3. Smoke (`tools/executor_contract_smoke_test.sh`):
   - registered the new signer-source positive guard.
4. Roadmap (`ROAD_TO_PRODUCTION.md`):
   - added item 333.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. Targeted tests:
   - `cargo test -p copybot-executor -q resolve_signer_source_config_accepts_file_source_with_matching_pubkey` — PASS
   - `cargo test -p copybot-executor -q resolve_signer_source_config_rejects_keypair_pubkey_mismatch` — PASS
   - `cargo test -p copybot-executor -q constant_time_eq_rejects_length_mismatch` — PASS
3. `cargo test -p copybot-executor -q` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- signer source validation no longer uses direct variable-time byte inequality for pubkey mismatch, and file-signer success path is explicitly pinned in regression coverage.
