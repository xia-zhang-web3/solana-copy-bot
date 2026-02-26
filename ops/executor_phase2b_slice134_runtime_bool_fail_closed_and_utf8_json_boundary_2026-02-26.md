# Executor Phase 2B Slice 134 — Runtime Bool Fail-Closed + UTF-8 JSON Boundary Hardening (2026-02-26)

## Scope

- close Rust runtime fail-open bool parsing for executor env gates
- close bounded-JSON lossy UTF-8 parse regression in runtime HTTP paths
- harden devnet rehearsal top-level bool parsing for `GO_NOGO_TEST_MODE` and `execution.enabled`

## Changes

1. Rust env bool parsing (`crates/executor/src/env_parsing.rs`)
   - `parse_bool_token` is now strict: returns `Option<bool>` (`Some(true|false)` only for valid tokens)
   - `parse_bool_env` now returns `Result<bool>` and fails closed on invalid tokens
   - invalid token message includes env name + raw value
   - `NotPresent` keeps default, `NotUnicode` now rejects
2. Runtime config wiring updated to propagate parse errors (`?`)
   - `ExecutorConfig::from_env()` bool gates now fail at startup on invalid tokens:
     - `COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED`
     - `COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED`
     - `COPYBOT_EXECUTOR_ALLOW_NONZERO_TIP`
   - `COPYBOT_EXECUTOR_LOG_JSON` and `COPYBOT_EXECUTOR_SUBMIT_VERIFY_STRICT` now also strict-parse via `parse_bool_env(...)?`
3. Bounded response reader metadata (`crates/executor/src/http_utils.rs`)
   - `ReadResponseBody` now includes raw `bytes: Vec<u8>` in addition to `text` and `was_truncated`
4. JSON parse boundary hardened (no lossy UTF-8 acceptance)
   - `send_rpc`, `upstream_forward`, `submit_verify` switched from `serde_json::from_str(body_read.text)` to `serde_json::from_slice(body_read.bytes)`
   - prevents non-UTF8 payload bytes from being normalized to `\uFFFD` and accepted as valid JSON
5. Devnet rehearsal strict bool input (`tools/execution_devnet_rehearsal.sh`)
   - added strict parse for top-level `GO_NOGO_TEST_MODE`
   - `execution_enabled` now strict-parsed from `SOLANA_COPY_BOT_EXECUTION_ENABLED`/config value (no weak `normalize_bool_token` path)
   - nested go/no-go/windowed-signoff invocations now receive normalized strict bool value
6. Coverage
   - new integration guards (runtime):
     - `forward_to_upstream_rejects_invalid_utf8_json_response_body`
     - `send_signed_transaction_via_rpc_rejects_invalid_utf8_json_response_body`
     - `verify_submit_signature_keeps_invalid_utf8_json_classification`
   - new config guards:
     - `executor_config_from_env_rejects_invalid_submit_fastlane_enabled_token`
     - `executor_config_from_env_rejects_invalid_allow_nonzero_tip_token`
   - new ops smoke guards for devnet rehearsal invalid bool tokens:
     - `GO_NOGO_TEST_MODE=sometimes`
     - `SOLANA_COPY_BOT_EXECUTION_ENABLED=sometimes`
   - `tools/executor_contract_smoke_test.sh` guard registry updated for new runtime/config tests
7. Ledger
   - `ROAD_TO_PRODUCTION.md` item `294` added.

## Files

- `crates/executor/src/env_parsing.rs`
- `crates/executor/src/executor_config_env.rs`
- `crates/executor/src/main.rs`
- `crates/executor/src/http_utils.rs`
- `crates/executor/src/send_rpc.rs`
- `crates/executor/src/upstream_forward.rs`
- `crates/executor/src/submit_verify.rs`
- `tools/execution_devnet_rehearsal.sh`
- `tools/ops_scripts_smoke_test.sh`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Verification

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q invalid_utf8_json` — PASS (3 tests)
3. `cargo test -p copybot-executor -q executor_config_from_env_rejects_invalid_` — PASS (2 tests)
4. `cargo test -p copybot-executor -q parse_bool_token_` — PASS (2 tests)
5. `bash tools/executor_contract_smoke_test.sh` — PASS
6. `CONFIG_PATH=configs/paper.toml GO_NOGO_TEST_MODE=sometimes bash tools/execution_devnet_rehearsal.sh 24 60` — PASS (`exit 1`, strict bool error)
7. `CONFIG_PATH=configs/paper.toml SOLANA_COPY_BOT_EXECUTION_ENABLED=sometimes bash tools/execution_devnet_rehearsal.sh 24 60` — PASS (`exit 1`, strict bool error)

Note: full `tools/ops_scripts_smoke_test.sh` was not run in this slice by plan; targeted invalid-token branches for this script change were validated directly.
