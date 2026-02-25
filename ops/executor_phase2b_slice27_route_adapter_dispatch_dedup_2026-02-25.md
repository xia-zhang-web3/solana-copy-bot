# Executor Phase 2B Slice 27 Evidence (2026-02-25)

## Scope

1. Removed duplicated per-route execution methods in `crates/executor/src/route_adapters.rs` and consolidated to one adapter execution path split by `UpstreamAction`.
2. Preserved route-specific behavior: only `Rpc` keeps the non-zero tip submit boundary guard via `validate_rpc_submit_tip_payload`.
3. Added explicit guard test `route_adapter_rpc_tip_guard_applies_only_to_rpc_submit` and included it in contract smoke guard-pack.

## Files

1. `crates/executor/src/route_adapters.rs`
2. `tools/executor_contract_smoke_test.sh`
3. `ROAD_TO_PRODUCTION.md`

## Regression Pack (quick/standard)

1. `cargo check -p copybot-executor -q`
2. `cargo test -p copybot-executor -q route_adapter_`
3. `bash tools/executor_contract_smoke_test.sh`
