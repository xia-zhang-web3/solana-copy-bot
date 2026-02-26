# Executor Phase 2B — Slice 161

Date: 2026-02-26  
Owner: execution-dev

## Scope

- harden auth ingress header parsing to fail-close on invalid non-ASCII required headers.

## Changes

1. Runtime hardening (`crates/executor/src/auth_verifier.rs`):
   - `Authorization` parsing now separates missing header (`auth_missing`) from invalid header encoding (`auth_invalid`) before Bearer token parsing.
   - `get_required_header` now takes separate `missing_code` and `invalid_code` and rejects non-ASCII header values explicitly as invalid.
   - HMAC required headers (`x-copybot-key-id`, `x-copybot-signature-alg`, `x-copybot-timestamp`, `x-copybot-auth-ttl-sec`, `x-copybot-nonce`, `x-copybot-signature`) now use strict missing/invalid split.
2. Added auth guard tests:
   - `auth_verifier_rejects_non_ascii_authorization_header`
   - `auth_verifier_hmac_rejects_non_ascii_required_header`
3. Registered both tests in `tools/executor_contract_smoke_test.sh`.
4. Updated roadmap item 321 in `ROAD_TO_PRODUCTION.md`.

## Validation

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q auth_verifier_rejects_non_ascii_authorization_header` — PASS
3. `cargo test -p copybot-executor -q auth_verifier_hmac_rejects_non_ascii_required_header` — PASS
4. `cargo test -p copybot-executor -q` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS

## Result

- auth boundary now reports invalid header encoding deterministically instead of masking invalid values as missing headers.
- fail-closed semantics are explicit for required auth headers across bearer and hmac paths.
