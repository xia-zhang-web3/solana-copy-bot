# Executor Phase 2B Slice 80 — Submit-Transport Type Boundary (2026-02-25)

## Scope

- harden submit transport extraction against malformed field types
- reject non-string transport artifacts with precise fail-closed reason

## Changes

1. Updated `extract_submit_transport_artifact(...)` in `submit_transport`:
   - added typed field parser for `tx_signature` / `signed_tx_base64`
   - non-string non-null values now reject with new error variant:
     - `SubmitTransportArtifactError::InvalidSubmitArtifactType { field_name }`
2. Updated reject mapping:
   - invalid transport field type maps to retryable `submit_adapter_invalid_response`
   - detail: `upstream <field> must be string when present`
3. Added unit guards in `submit_transport.rs`:
   - `submit_transport_extract_rejects_non_string_tx_signature_type`
   - `submit_transport_extract_rejects_non_string_signed_tx_base64_type`
4. Added integration guards in `main.rs`:
   - `handle_submit_rejects_when_upstream_tx_signature_type_is_invalid`
   - `handle_submit_rejects_when_upstream_signed_tx_base64_type_is_invalid`
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
3. `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_tx_signature_type_is_invalid` — PASS
4. `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_signed_tx_base64_type_is_invalid` — PASS
5. `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_returns_conflicting_transport_artifacts` — PASS
6. `bash tools/executor_contract_smoke_test.sh` — PASS
