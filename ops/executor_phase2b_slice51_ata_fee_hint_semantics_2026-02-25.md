# Executor Phase 2B Slice 51 — ATA Fee Hint Semantics Hardening (2026-02-25)

## Scope

- tighten ATA fee-hint contract semantics in `/submit` success payload
- align runtime behavior with contract clause: ATA rent hint only when ATA-create path is actually used
- add dedicated `ata_*` guard coverage

## Changes

1. `resolve_fee_hints(...)` now normalizes `response_ata_create_rent_lamports=0` to `None` (absent).
2. `/submit` success payload builder now emits `ata_create_rent_lamports` only when hint is present (`Some(value)`).
3. Added ATA-focused unit tests:
   - `ata_fee_hint_zero_is_treated_as_absent`
   - `ata_fee_hint_positive_value_is_preserved`
   - `ata_fee_hint_rejects_values_exceeding_i64`
   - `ata_submit_payload_omits_ata_create_rent_lamports_when_absent`
   - `ata_submit_payload_includes_ata_create_rent_lamports_when_present`
4. Added ATA tests to contract smoke guard list.
5. Updated roadmap ledger with Slice 51 note.

## Files

- `crates/executor/src/fee_hints.rs`
- `crates/executor/src/submit_payload.rs`
- `tools/executor_contract_smoke_test.sh`
- `ROAD_TO_PRODUCTION.md`

## Regression Pack

1. `cargo check -p copybot-executor -q` — PASS
2. `cargo test -p copybot-executor -q ata_` — PASS
3. `cargo test -p copybot-executor -q resolve_fee_hints_` — PASS
4. `bash tools/executor_contract_smoke_test.sh` — PASS
