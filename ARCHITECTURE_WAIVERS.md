# Architecture Waivers

Status: active baseline
Date: 2026-05-03

This file records explicit temporary exceptions to `BUILD_POLICY.md`.

Waivers are not permission to grow debt. Existing oversized files are
grandfathered only at or below their current line count. Any growth requires a
new waiver entry or a direct production-blocker justification.

## Grandfathered Oversized Files

No active oversized-file waivers.

## Retired Oversized Waivers

These files were reduced under the hard limit and are now protected by the
normal guard:

- `crates/storage/src/lib.rs`
- `crates/storage/src/market_data.rs`
- `crates/storage/src/discovery_scoring.rs`
- `crates/app/src/main.rs`
- `crates/app/src/observed_swap_writer.rs`
- `crates/discovery/src/lib.rs`
- `crates/discovery/src/discovery_parts/service_methods_28_collect_buy_mints_prepass.rs`
- `crates/discovery/src/discovery_parts/service_methods_29_persisted_stream_rebuild.rs`
- `crates/discovery/src/discovery_parts/service_methods_30_run_cycle.rs`

## Grandfathered Inline Tests

Existing inline tests are debt. New inline test bodies are forbidden.

Removal path:

1. move test bodies into `src/tests.rs`, `src/<module>/tests.rs`, or
   integration tests,
2. leave only `#[cfg(test)] mod tests;` declarations in production files,
3. reduce production file line count in the same batch when feasible.

## Emergency Production Build Waiver

No active waiver.

Production-local builds must be recorded here if artifact deploy is unavailable:

| Date | Command | Reason artifact deploy failed | Owner | Follow-up |
| --- | --- | --- | --- | --- |

## Temporary Operator Dependency Waivers

No active waiver.
