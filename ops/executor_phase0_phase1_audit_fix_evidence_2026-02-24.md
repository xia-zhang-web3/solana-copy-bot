# Executor Phase 0/1 Audit Fix Evidence

Date: 2026-02-24  
Branch: `executor/phase-0-1-scaffold`

## Fixed Findings

1. `/simulate` business reject now always returns HTTP 200 JSON (contract §6.4).
2. Adapter->executor auth policy now enforces Bearer as mandatory (HMAC optional additive guard only).
3. `/simulate` now enforces non-empty required fields:
   1. `request_id`
   2. `signal_id`
4. `/submit` now enforces non-empty `signal_id`.
5. Signer file custody bootstrap hardened:
   1. requires valid Solana keypair JSON array (64 bytes)
   2. verifies derived pubkey matches `COPYBOT_EXECUTOR_SIGNER_PUBKEY`
6. Contract/master-plan taxonomy wording aligned with current compatibility transport code strategy.
7. `tools/executor_contract_smoke_test.sh` now runs contract-guard tests (not only markdown section presence).

## Regression Pack

1. `cargo test -p copybot-executor -q` — PASS (46)
2. `bash tools/executor_contract_smoke_test.sh` — PASS
3. `cargo test --workspace -q` — PASS
4. `timeout 300 bash tools/ops_scripts_smoke_test.sh` — PASS

## Added/Updated Guard Tests

1. `handle_simulate_rejects_empty_request_id`
2. `handle_simulate_rejects_empty_signal_id`
3. `handle_submit_rejects_empty_signal_id`
4. `resolve_signer_source_config_rejects_keypair_pubkey_mismatch`
5. `simulate_reject_status_is_http_200_for_retryable_and_terminal`
