# Executor Phase 2B — Slice 473

Date (UTC): 2026-03-04  
Owner: execution-dev

## Scope

Fix auditor finding from commit `865a7f4`: `tools/executor_preflight.sh` HMAC auth-probe signature builder used conflicting stdin redirects (heredoc + herestring), causing probe signature generation failure in strict HMAC-only scenarios.

## Changes

Files:

1. `tools/executor_preflight.sh`
2. `tools/ops_scripts_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

### 1) `hmac_sha256_hex` hotfix

- Replaced broken form:
  - `python <<'PY' <<<"$payload"`
- With stable pipe form:
  - `printf '%s' "$payload" | python -c ...`

Result: HMAC probe signature is now computed over the real payload bytes.

### 2) Smoke contract hardening (`executor_preflight` target)

`write_fake_curl_executor_preflight` enhanced with HMAC-aware simulate probe behavior:

1. New toggles:
   1. `FAKE_EXECUTOR_SIMULATE_REQUIRE_BEARER`
   2. `FAKE_EXECUTOR_SIMULATE_REQUIRE_HMAC`
   3. `FAKE_EXECUTOR_SIMULATE_EXPECT_HMAC_KEY_ID`
   4. `FAKE_EXECUTOR_SIMULATE_EXPECT_HMAC_TTL_SEC`
2. Fake curl now parses `x-copybot-*` headers and returns `hmac_missing/hmac_invalid` when required.

Added dedicated positive pin:

1. strict HMAC-only auth probe path (`bearer` disabled in executor env)
2. preflight expected `PASS`
3. asserts:
   1. `executor_bearer_required: false`
   2. `executor_hmac_required: true`
   3. `auth_probe_without_auth_code: hmac_missing`
   4. `auth_probe_with_auth_code: invalid_request`

## Validation

1. `bash -n tools/executor_preflight.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_TARGET_CASES=executor_preflight bash tools/ops_scripts_smoke_test.sh` — PASS
3. `cargo check -p copybot-executor -q` — PASS

## Outcome

- Auditor finding closed: preflight HMAC auth probe no longer false-fails due to broken signing helper.
- Regression guard added so this class of bug is caught by targeted smoke in future slices.
