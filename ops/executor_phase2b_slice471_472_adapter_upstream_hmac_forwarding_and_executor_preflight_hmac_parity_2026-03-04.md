# Executor Phase 2B — Slices 471-472

Date (UTC): 2026-03-04
Owner: execution-dev

## Scope

1. Slice 471: adapter upstream HMAC forwarding for executor hop.
2. Slice 472: executor preflight runtime-parity for bearer/HMAC auth requirements + adapter upstream HMAC parity checks.

## Implemented

### Slice 471 — Adapter upstream HMAC forwarding

Files:

1. `crates/adapter/src/main.rs`
2. `ops/executor_contract_v1.md`
3. `ops/executor_backend_master_plan_2026-02-24.md`
4. `ops/server_templates/adapter.env.example`
5. `adapter.env`

Behavior changes:

1. Added optional adapter upstream HMAC env contract:
   1. `COPYBOT_ADAPTER_UPSTREAM_HMAC_KEY_ID`
   2. `COPYBOT_ADAPTER_UPSTREAM_HMAC_SECRET` / `COPYBOT_ADAPTER_UPSTREAM_HMAC_SECRET_FILE`
   3. `COPYBOT_ADAPTER_UPSTREAM_HMAC_TTL_SEC`
2. Added fail-closed adapter startup validation for upstream HMAC pair/TTL (`5..=300`, no partial config).
3. `forward_to_upstream` now signs forwarded `/simulate` and `/submit` requests when upstream HMAC is configured:
   1. headers: `x-copybot-key-id`, `x-copybot-signature-alg`, `x-copybot-timestamp`, `x-copybot-auth-ttl-sec`, `x-copybot-nonce`, `x-copybot-signature`
   2. signature payload is raw-byte exact (`timestamp\n ttl\n nonce\n body_bytes`).
4. Kept bearer forwarding behavior unchanged; HMAC is additive.
5. Added adapter unit coverage for:
   1. HMAC header forwarding path
   2. raw non-UTF8 body signing path

### Slice 472 — Executor preflight HMAC parity and auth-probe hardening

Files:

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`

Behavior changes:

1. Preflight auth requirement modeling now mirrors runtime behavior:
   1. `executor_bearer_required=true` when bearer token is configured
   2. `executor_hmac_required=true` when HMAC key id is configured
   3. `executor_auth_required=true` when unauth is disabled **or** bearer/HMAC is configured
2. Removed bearer-only assumption from unauth checks; fail-closed message now allows bearer or HMAC pair when unauth is disabled.
3. Added adapter upstream HMAC config parsing/validation in preflight:
   1. pair consistency (`KEY_ID` + `SECRET|SECRET_FILE`)
   2. TTL range and type
4. Added fail-closed parity checks when executor HMAC is enabled:
   1. adapter upstream HMAC missing
   2. key id mismatch
   3. secret mismatch
   4. ttl mismatch
5. Auth probe now signs request with HMAC headers when executor HMAC is required.
6. Added summary observability fields:
   1. `executor_auth_required`
   2. `executor_hmac_required`
   3. `adapter_upstream_hmac_configured`
   4. `adapter_upstream_hmac_ttl_sec`

Smoke coverage added:

1. missing adapter upstream HMAC under executor HMAC requirement
2. key mismatch
3. secret mismatch
4. ttl mismatch
5. `_FILE` secret mismatch
6. updated auth-probe assertion wording for new generic auth-probe path
7. adjusted unauth smoke case to remove bearer token from fixture when validating "allow_unauthenticated=true but endpoint requires auth"

## Validation

1. `cargo fmt --package copybot-adapter` — PASS
2. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
3. `cargo check -p copybot-adapter -q` — PASS
4. `cargo test -p copybot-adapter -q forward_to_upstream_adds_upstream_hmac_headers` — PASS
5. `cargo test -p copybot-adapter -q forward_to_upstream_signs_hmac_over_raw_non_utf8_body` — PASS
6. `cargo test -p copybot-adapter -q forward_to_upstream_uses_fallback_auth_token_when_retrying` — PASS
7. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS
8. `cargo check -p copybot-executor -q` — PASS

## Residual gap

1. Full `tools/ops_scripts_smoke_test.sh` and full workspace tests were intentionally not executed in this slice; targeted smoke + crate-level checks were used for speed.
