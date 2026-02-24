# Executor Phase 2 Slice 21 Evidence (2026-02-24)

## Scope

Continued Phase 2 extraction by moving signer-source model and startup validation out of `main.rs` into a dedicated shared module.

## Implemented

1. `crates/executor/src/signer_source.rs`:
   1. Added shared signer model:
      1. `SignerSource` (`file|kms`)
   2. Added shared startup resolver:
      1. `resolve_signer_source_config(source_raw, keypair_file_raw, kms_key_id_raw, signer_pubkey)`
   3. Added keypair validation path:
      1. file readability + restrictive permission check,
      2. non-empty trimmed file content,
      3. JSON array with exact 64-byte keypair length,
      4. derived pubkey must match configured signer pubkey.
2. `crates/executor/src/main.rs`:
   1. Added module wiring:
      1. `mod signer_source`
   2. Rewired executor config bootstrap to shared signer-source imports.
   3. Removed duplicated inline signer-source/type helper implementations.

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q resolve_signer_source_config_rejects_keypair_pubkey_mismatch` — PASS
2. `cargo test -p copybot-executor -q resolve_signer_source_config_rejects_non_restrictive_keypair_permissions` — PASS
3. `cargo test -p copybot-executor -q require_authenticated_mode_fails_closed_by_default` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
