# Executor Phase 2 Slice 7 Evidence (2026-02-24)

## Scope

Continued submit/simulate pipeline extraction by moving upstream business-outcome classification into a dedicated module with parsed reject model.

## Implemented

1. `crates/executor/src/upstream_outcome.rs`:
   1. Added parsed reject structure:
      1. `ParsedUpstreamReject { retryable, code, detail }`
   2. Added outcome enum:
      1. `UpstreamOutcome::Success`
      2. `UpstreamOutcome::Reject(ParsedUpstreamReject)`
   3. Added classifier:
      1. `parse_upstream_outcome(&Value, default_reject_code)`
   4. Added module tests:
      1. `upstream_outcome_rejects_unknown_status`
      2. `upstream_outcome_rejects_explicit_reject`
2. `crates/executor/src/main.rs`:
   1. Removed inline `parse_upstream_outcome` implementation.
   2. Wired simulate/submit paths to use module parser.
   3. Preserved behavior via explicit mapper:
      1. `map_parsed_upstream_reject` converts parsed rejects to `Reject::terminal|retryable` with unchanged code/detail semantics.
3. `tools/executor_contract_smoke_test.sh`:
   1. Added guard test:
      1. `upstream_outcome_rejects_unknown_status`

## Regression Pack (quick/standard)

1. `cargo test -p copybot-executor -q upstream_outcome_` — PASS
2. `cargo test -p copybot-executor -q parse_upstream_outcome_rejects_unknown_status` — PASS
3. `bash tools/executor_contract_smoke_test.sh` — PASS
