# Executor Phase 2B Slice 95 — Submit Transport Non-Empty Boundary Hardening (2026-02-26)

## Scope

- harden upstream submit transport artifact parsing (`tx_signature`, `signed_tx_base64`)
- remove fail-open behavior where present-null/empty transport fields were silently treated as absent

## Changes

1. Updated `parse_optional_non_empty_submit_transport_field(...)` in `submit_transport.rs`:
   - absent field => `None` (unchanged)
   - present `null|non-string|empty` => `InvalidSubmitArtifactType`
   - present non-empty string => accepted
2. Updated reject mapping detail in `reject_mapping.rs`:
   - now emits `upstream <field> must be non-empty string when present`
3. Added unit tests:
   - `submit_transport_extract_rejects_null_tx_signature`
   - `submit_transport_extract_rejects_empty_signed_tx_base64`
4. Added integration tests:
   - `handle_submit_rejects_when_upstream_tx_signature_is_null`
   - `handle_submit_rejects_when_upstream_signed_tx_base64_is_empty`
5. Registered new guards in `tools/executor_contract_smoke_test.sh`.
6. Updated roadmap ledger.

## Files

- `crates/executor/src/submit_transport.rs`
- `crates/executor/src/reject_mapping.rs`
- `crates/executor/src/main.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q submit_transport_extract_rejects_(null|empty)` — PASS
3. `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_(tx_signature_is_null|signed_tx_base64_is_empty)` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
