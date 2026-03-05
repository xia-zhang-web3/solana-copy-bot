# Slice 529 — strict submit-verify identity normalization in go/no-go guard

## Scope
- `tools/execution_go_nogo_report.sh`
- `tools/ops_scripts_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Problem
`GO_NOGO_REQUIRE_SUBMIT_VERIFY_STRICT=true` previously compared submit-verify primary/fallback as raw URL strings.
Equivalent endpoint identities (for example `https://verify.integration.test` vs `https://VERIFY.integration.test:443/`) could bypass the fallback collision guard and produce a false `PASS`.

## Implemented
1. Added `endpoint_identity()` helper in `execution_go_nogo_report.sh`:
   - canonicalizes to `scheme://host:port/path`,
   - lowercases scheme/host,
   - applies default ports (`http=80`, `https=443`),
   - defaults empty path to `/`,
   - rejects invalid URL shapes fail-closed.
2. Updated strict submit-verify guard flow:
   - parse/canonicalize primary identity first,
   - parse/canonicalize fallback identity when fallback is set,
   - compare canonical identities for distinctness,
   - return `UNKNOWN` with reason codes:
     - `submit_verify_primary_invalid`
     - `submit_verify_fallback_invalid`
   - keep existing collision outcome:
     - `WARN` + `submit_verify_fallback_same_as_primary`.
3. Added smoke pins in `go_nogo_executor_backend_mode_guard`:
   - normalized identity collision (case+default-port+path equivalent) => `WARN` / `submit_verify_fallback_same_as_primary` / `NO_GO`.
   - invalid strict primary verify URL => `UNKNOWN` / `submit_verify_primary_invalid` / `NO_GO`.

## Validation
1. `bash -n tools/execution_go_nogo_report.sh tools/ops_scripts_smoke_test.sh` — PASS
2. `OPS_SMOKE_PROFILE=full OPS_SMOKE_TARGET_CASES=go_nogo_executor_backend_mode_guard bash tools/ops_scripts_smoke_test.sh` — PASS
3. `OPS_SMOKE_PROFILE=fast OPS_SMOKE_TARGET_CASES=execution_server_rollout_fast bash tools/ops_scripts_smoke_test.sh` — PASS
4. `cargo check -p copybot-executor -q` — PASS

## Outcome
Strict submit-verify guard no longer fail-opens on raw-string URL mismatches and now aligns with preflight-style endpoint identity semantics.
