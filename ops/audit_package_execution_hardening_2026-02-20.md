# Audit Package: Execution Hardening Batch (2026-02-20)

Branch: `feat/yellowstone-grpc-migration`

Scope commit range:
1. `f2c71f2` -> `347172d`
2. Includes follow-up commits `a423f02`, `8e73564`, `9d554e9`, `3dabd14`, `b49cf3c`, `6d1ce27`

Out of scope:
1. External adapter backend behavior outside current repo
2. Operational server evidence windows (post-cutover timeline artifacts)

## Changelog (Audit-Oriented)

1. `f2c71f2`: runtime hardening baseline
   Files: `crates/app/src/main.rs`, `crates/ingestion/src/source.rs`, `crates/storage/src/lib.rs`, tooling/docs.
   Highlights:
   1. `parser-stall` and infra guards expansion groundwork
   2. stale-close reliable pricing path + risk event on missing reliable price
2. `a423f02`: parser-stall negative-threshold coverage
   Files: `crates/app/src/main.rs`
   Highlights:
   1. added test for `error_ratio < 0.95` does not block
3. `8e73564`: simulator terminal/retryable hardening
   Files: `crates/execution/src/simulator.rs`
   Highlights:
   1. invalid JSON response becomes terminal reject (no fallback)
   2. endpoint-attempt logging for simulator path
4. `9d554e9`: CU policy constants unification
   Files: `crates/config/src/lib.rs`, `crates/app/src/main.rs`, `crates/execution/src/submitter.rs`
   Highlights:
   1. single-source constants for CU bounds in config/runtime/submitter
5. `3dabd14`: idempotency collision hardening
   Files: `crates/execution/src/idempotency.rs`, `crates/execution/src/lib.rs`
   Highlights:
   1. deterministic hash suffix for non-colon/long signal IDs
   2. legacy colon-delimited format preserved
6. `b49cf3c`: simulator diagnostics redaction
   Files: `crates/execution/src/simulator.rs`
   Highlights:
   1. detail strings redact endpoint labels to scheme+host[:port]
7. `6d1ce27`: simulator structured-log redaction parity
   Files: `crates/execution/src/simulator.rs`
   Highlights:
   1. structured logs now also use redacted endpoint label
8. `347172d`: parser-stall boundary threshold coverage
   Files: `crates/app/src/main.rs`, `ops/audit_package_execution_hardening_2026-02-20.md`
   Highlights:
   1. added boundary test for `error_ratio == 0.95` blocking behavior
   2. updated audit package test matrix and wording parity notes

## Mandatory Verification Points

1. Parser-stall gate behavior (`crates/app/src/main.rs`)
   1. blocks only when full-window no output progress + tx updates move + parser/decode error ratio high
   2. does not block when ratio stays below threshold
2. Stale-close price hardening (`crates/storage/src/lib.rs`, `crates/app/src/main.rs`)
   1. stale close uses reliable price aggregation, not raw last swap
   2. when reliable price unavailable, lot is skipped and `shadow_stale_close_price_unavailable` is emitted
3. Adapter simulator fail-closed semantics (`crates/execution/src/simulator.rs`)
   1. `invalid_json` is terminal reject and does not fallback
   2. fallback remains only for retryable endpoint errors (send/429/5xx)
4. Endpoint redaction consistency (`crates/execution/src/simulator.rs`)
   1. detail strings do not expose raw URL path/query/fragment
   2. structured logs do not expose raw endpoint URL either
5. Idempotency collision resistance (`crates/execution/src/idempotency.rs`)
   1. non-colon delimiter variants produce different IDs
   2. colon-delimited legacy IDs stay stable
6. CU bounds parity (`crates/config/src/lib.rs`, `crates/app/src/main.rs`, `crates/execution/src/submitter.rs`)
   1. config load performs type-parse; min/max bounds are enforced consistently in runtime validate and submitter normalization using the same constants

## Targeted Test Commands

```bash
cargo test -p copybot-app -q risk_guard_infra_blocks_when_parser_stall_detected
cargo test -p copybot-app -q risk_guard_infra_parser_stall_does_not_block_below_ratio_threshold
cargo test -p copybot-app -q risk_guard_infra_parser_stall_blocks_at_ratio_threshold_boundary
cargo test -p copybot-app -q stale_lot_cleanup_ignores_micro_swap_outlier_price
cargo test -p copybot-app -q stale_lot_cleanup_skips_and_records_risk_event_when_reliable_price_missing

cargo test -p copybot-execution -q adapter_intent_simulator_does_not_fallback_on_invalid_json_terminal_reject
cargo test -p copybot-execution -q redacted_endpoint_label_drops_path_and_query
cargo test -p copybot-execution -q client_order_id_avoids_collision_for_non_colon_delimiters
cargo test -p copybot-execution -q client_order_id_keeps_legacy_format_for_colon_delimited_signal

cargo test --workspace -q
tools/ops_scripts_smoke_test.sh
```

## Optional SQL Spot Checks (Operator/Auditor)

```sql
-- stale-close unavailable price events
SELECT COUNT(*) AS cnt
FROM risk_events
WHERE type = 'shadow_stale_close_price_unavailable';

-- parser-stall/no-progress infra events
SELECT type, COUNT(*) AS cnt
FROM risk_events
WHERE type IN ('shadow_risk_infra_stop', 'shadow_risk_infra_cleared')
GROUP BY type;
```

## Expected Outcome

1. All targeted tests pass.
2. Workspace test suite passes.
3. Ops smoke script passes.
4. No raw endpoint URLs with query/fragment appear in simulator diagnostics paths.
