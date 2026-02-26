# Executor Phase 2B Slice 139 — Runtime Secret Zeroize Hardening (2026-02-26)

## Scope

- close `M-2` (secrets stored as plaintext `String` without zeroization) in executor runtime paths
- close signer keypair byte-retention low gap (`L-10`) in signer validation path

## Changes

1. Added `crates/executor/src/secret_value.rs`:
   - new `SecretValue` wrapper over `Zeroizing<String>`
   - explicit accessors (`as_str`, `as_bytes`) for callsites that need secret bytes
   - redacted debug output (`SecretValue([REDACTED])`)
2. Added dependency:
   - `crates/executor/Cargo.toml`: `zeroize = "1.8"`
3. Migrated secret-bearing runtime fields to `SecretValue`:
   - `ExecutorConfig.bearer_token`
   - `ExecutorConfig.hmac_secret`
   - `RouteBackend.{primary_auth_token,fallback_auth_token,send_rpc_primary_auth_token,send_rpc_fallback_auth_token}`
   - `AuthVerifier.bearer_token`
   - `AuthVerifier::HmacConfig.secret`
4. `secret_source` hardened:
   - `resolve_secret_source(...) -> Result<Option<SecretValue>>`
   - file-read raw content wrapped in `Zeroizing<String>` before trim/extract
5. `main` startup copy reduction:
   - auth secrets are moved from config into verifier via `take()`:
     - `config.bearer_token.take()`
     - `config.hmac_secret.take()`
   - avoids duplicate secret copies in `AppState.config` once `AuthVerifier` is initialized
6. Signer keypair validation hardened (`signer_source.rs`):
   - keypair file raw content wrapped in `Zeroizing<String>`
   - parsed keypair bytes wrapped in `Zeroizing<Vec<u8>>`
7. Contract test coverage:
   - added `secret_value_debug_redacts_plaintext`
   - registered in `tools/executor_contract_smoke_test.sh`

## Files

- `crates/executor/Cargo.toml`
- `crates/executor/src/secret_value.rs`
- `crates/executor/src/secret_source.rs`
- `crates/executor/src/auth_verifier.rs`
- `crates/executor/src/route_backend.rs`
- `crates/executor/src/signer_source.rs`
- `crates/executor/src/request_ingress.rs`
- `crates/executor/src/executor_config_env.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q auth_verifier_` — PASS
3. `cargo test -p copybot-executor -q request_ingress_` — PASS
4. `cargo test -p copybot-executor -q route_backend_` — PASS
5. `cargo test -p copybot-executor -q resolve_secret_source_` — PASS
6. `cargo test -p copybot-executor -q resolve_signer_source_config_` — PASS
7. `cargo test -p copybot-executor -q secret_value_debug_redacts_plaintext` — PASS
8. `cargo test -p copybot-executor -q` — PASS (515/515)
9. `bash tools/executor_contract_smoke_test.sh` — PASS
