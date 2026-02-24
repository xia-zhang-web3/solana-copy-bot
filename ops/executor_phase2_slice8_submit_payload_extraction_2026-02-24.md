# Executor Phase 2 Slice 8 Evidence (2026-02-24)

## Scope

Continued submit-path extraction by moving successful submit response payload assembly into a dedicated module.

## Implemented

1. `crates/executor/src/submit_payload.rs`:
   1. Added payload input struct:
      1. `SubmitSuccessPayloadInputs`
   2. Added payload builder:
      1. `build_submit_success_payload(...)`
   3. Preserved response schema fields:
      1. `status/ok/accepted`
      2. route + contract + request identity
      3. `tx_signature` + `submit_transport`
      4. `submitted_at`
      5. `slippage_bps` + `tip_lamports`
      6. `compute_budget`
      7. fee hints (`network/base/priority/ata`)
      8. optional `tip_policy`
      9. `submit_signature_verify`
   4. Added module tests:
      1. `submit_payload_includes_tip_policy_when_present`
      2. `submit_payload_omits_tip_policy_when_absent`
2. `crates/executor/src/main.rs`:
   1. Replaced inline submit response JSON assembly with module builder.
   2. Preserved existing behavior and fields by passing all current values through `SubmitSuccessPayloadInputs`.
3. `tools/executor_contract_smoke_test.sh`:
   1. Added payload guard test:
      1. `submit_payload_includes_tip_policy_when_present`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q submit_payload_` — PASS
2. `cargo test -p copybot-executor -q handle_submit_forces_rpc_tip_to_zero_and_emits_trace` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
