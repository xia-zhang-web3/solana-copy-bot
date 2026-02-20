# Audit Package: Adapter Secret Rotation Hardening (2026-02-20)

Branch: `feat/yellowstone-grpc-migration`

Scope commit range:
1. `bad4368` -> `f2b009c`
2. Includes:
   1. `bad4368` adapter file-based secret sources (fail-closed)
   2. `e0c9efa` permission warning + atomic rotation guidance
   3. `d14c805` rotation readiness report helper + smoke/doc/ROAD sync
   4. `a101acd` parser hardening (last-wins + inline/file conflicts + quoted `#`)
   5. `f2b009c` underscore route-id conflict coverage (`FAST_LANE`)

Out of scope:
1. Real signed-transaction upstream execution backend behavior.
2. Server-side systemd rollout evidence artifacts (executed drill reports).
3. Yellowstone migration closure (separate completed track).

## Changelog (Audit-Oriented)

1. Adapter runtime secret source hardening
   Files:
   1. `crates/adapter/src/main.rs`
   Highlights:
   1. file-based secret sources added for inbound and upstream auth:
      1. `COPYBOT_ADAPTER_BEARER_TOKEN_FILE`
      2. `COPYBOT_ADAPTER_HMAC_SECRET_FILE`
      3. `COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE`
      4. `COPYBOT_ADAPTER_ROUTE_<ROUTE>_AUTH_TOKEN_FILE`
   2. fail-closed semantics:
      1. inline + file conflict -> startup error
      2. empty/missing file -> startup error
   3. warning when secret file permissions are broader than owner-only.
2. Ops helper for rotation readiness
   Files:
   1. `tools/adapter_secret_rotation_report.sh`
   2. `tools/ops_scripts_smoke_test.sh`
   3. `ops/adapter_backend_runbook.md`
   4. `ROAD_TO_PRODUCTION.md`
   Highlights:
   1. report helper validates secret file readiness and auth contract.
   2. emits `rotation_readiness_verdict: PASS|WARN|FAIL` with exit codes `0|2|1`.
   3. supports artifact export via `OUTPUT_DIR`.
   4. smoke coverage includes pass/warn/fail + duplicate-key precedence + conflict cases.

## Mandatory Verification Points

1. Adapter runtime secret source fail-closed
   1. `resolve_secret_source` rejects inline+file for same secret.
   2. `read_trimmed_secret_file` rejects empty/missing files.
   3. per-route auth token file fallback to default works without conflict bypass.
2. Permission-hardening behavior
   1. broad permissions (`0644`) only warn (no startup block).
   2. restrictive permissions (`0600/0400`) pass cleanly.
3. Rotation report parity with runtime contract
   1. report must fail on inline+file conflicts for:
      1. bearer
      2. hmac secret
      3. upstream auth token
      4. per-route auth token (including underscore route IDs like `FAST_LANE`)
   2. report must follow `last-wins` key resolution for duplicated env keys.
   3. `#` inside quoted values must not be treated as comment.
4. Smoke coverage sufficiency
   1. pass/warn/fail branches are exercised.
   2. duplicate-key precedence branch is exercised.
   3. route underscore conflict branch is exercised.
5. Documentation parity
   1. runbook includes atomic rotation pattern and helper usage.
   2. ROAD includes this hardening block and next queue wording remains consistent.

## Targeted Test Commands

```bash
cargo test -p copybot-adapter -q
cargo test --workspace -q

bash -n tools/adapter_secret_rotation_report.sh
bash -n tools/ops_scripts_smoke_test.sh
tools/ops_scripts_smoke_test.sh
```

## Manual Repro Commands (Optional but Recommended)

```bash
# 1) PASS baseline
ADAPTER_ENV_PATH=/etc/solana-copy-bot/adapter.env \
OUTPUT_DIR=state/adapter-rotation \
tools/adapter_secret_rotation_report.sh

# 2) conflict should FAIL (example)
cat >> /tmp/adapter-conflict.env <<'EOF'
COPYBOT_ADAPTER_BEARER_TOKEN=inline-conflict
COPYBOT_ADAPTER_BEARER_TOKEN_FILE=secrets/adapter_bearer.token
EOF
ADAPTER_ENV_PATH=/tmp/adapter-conflict.env tools/adapter_secret_rotation_report.sh

# 3) broad permissions should WARN
chmod 644 /etc/solana-copy-bot/secrets/adapter_bearer.token
ADAPTER_ENV_PATH=/etc/solana-copy-bot/adapter.env tools/adapter_secret_rotation_report.sh
```

## Expected Outcome

1. No new `High` or `Medium` findings for this scope.
2. Report helper verdict matches runtime fail-closed contract for secret-source conflicts.
3. Smoke and workspace tests pass without flaky behavior.
