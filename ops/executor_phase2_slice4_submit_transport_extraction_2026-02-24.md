# Executor Phase 2 Slice 4 Evidence (2026-02-24)

## Scope

Continued submit-path extraction by isolating transport-artifact parsing/validation into a dedicated module while preserving reject-code behavior.

## Implemented

1. `crates/executor/src/submit_transport.rs`:
   1. Added typed submit transport extraction:
      1. `SubmitTransportArtifact::UpstreamSignature(String)`
      2. `SubmitTransportArtifact::SignedTransactionBase64(String)`
   2. Added strict parser `extract_submit_transport_artifact(&Value)`.
   3. Added typed error surface:
      1. `InvalidUpstreamSignature`
      2. `MissingSubmitArtifact`
   4. Added module unit tests (`submit_transport_*`).
2. `crates/executor/src/main.rs`:
   1. Wired `handle_submit` to use `extract_submit_transport_artifact`.
   2. Preserved transport outcome behavior:
      1. upstream signature path -> `submit_transport="upstream_signature"`
      2. signed tx path -> RPC send path + `submit_transport="adapter_send_rpc"`
   3. Preserved fail-closed reject semantics via mapper:
      1. `submit_adapter_invalid_response` on invalid upstream signature
      2. `submit_adapter_invalid_response` when both artifacts are missing
   4. Added integration guard test:
      1. `handle_submit_rejects_when_upstream_missing_transport_artifacts`
3. `tools/executor_contract_smoke_test.sh`:
   1. Added coverage for:
      1. `handle_submit_rejects_when_upstream_missing_transport_artifacts`
      2. `submit_transport_extract_rejects_missing_artifacts`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q submit_transport_` — PASS
2. `cargo test -p copybot-executor -q handle_submit_rejects_when_upstream_missing_transport_artifacts` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
