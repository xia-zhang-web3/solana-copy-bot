# Executor Phase 2B Slice 79 — Submit-Transport Artifact Consistency (2026-02-25)

## Scope

- harden submit transport extraction contract to enforce exactly-one artifact
- fail closed on ambiguous upstream success payloads carrying both transport artifacts

## Changes

1. Updated `extract_submit_transport_artifact(...)` in `submit_transport`:
   - now rejects when both `tx_signature` and `signed_tx_base64` are present
   - new error: `SubmitTransportArtifactError::ConflictingSubmitArtifacts`
2. Updated reject mapping:
   - conflicting artifacts map to retryable `submit_adapter_invalid_response`
   - detail: `upstream response must include exactly one of tx_signature or signed_tx_base64`
3. Added unit guard in `submit_transport.rs`:
   - `submit_transport_extract_rejects_conflicting_artifacts_when_both_present`
4. Added integration guard in `main.rs`:
   - `handle_submit_rejects_when_upstream_returns_conflicting_transport_artifacts`
5. Registered new guards in contract smoke and updated roadmap ledger.

## Files

- `crates/executor/src/submit_transport.rs`
- `crates/executor/src/reject_mapping.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q submit_transport_extract_` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_returns_conflicting_transport_artifacts` — PASS
4. `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_missing_transport_artifacts` — PASS
5. `bash tools/executor_contract_smoke_test.sh` — PASS
