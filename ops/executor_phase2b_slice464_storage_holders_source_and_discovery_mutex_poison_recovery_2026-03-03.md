# Executor Phase-2B Slice 464 — Storage Holders Source + Discovery Mutex Poison Recovery

Date: 2026-03-03

## Scope

1. `crates/storage/src/market_data.rs`
2. `crates/discovery/src/lib.rs`
3. `ROAD_TO_PRODUCTION.md`

## Problem

1. Holders metric source used `getTokenSupply` (token supply), which is not holder count.
2. Discovery runtime used `.lock().expect("discovery window mutex poisoned")`, which panicked on poisoned mutex.

## Change

1. Holders metric source corrected:
   1. switched RPC method to `getTokenAccountsByMint` with `encoding=jsonParsed`.
   2. holder count is now computed as unique account owners with `tokenAmount.amount > 0`.
   3. malformed account response shape now fails this fetch path explicitly.
2. Discovery fail-safe mutex handling:
   1. replaced `.lock().expect(...)` with recover path:
      1. `Ok(guard)` -> normal path,
      2. `Err(poisoned)` -> warning + `poisoned.into_inner()`.

## Tests

1. Storage:
   1. `parse_token_holders_from_accounts_response_counts_unique_nonzero_owners`
   2. `parse_token_holders_from_accounts_response_rejects_invalid_shape`
2. Discovery:
   1. `run_cycle_recovers_from_poisoned_window_mutex`

## Validation

1. `cargo test -p copybot-storage -q parse_token_holders_from_accounts_response` — PASS
2. `cargo test -p copybot-discovery -q run_cycle_recovers_from_poisoned_window_mutex` — PASS
3. `cargo check -p copybot-storage -q` — PASS
4. `cargo check -p copybot-discovery -q` — PASS

## Mapping

1. Post bring-up gate closure:
   1. holders metric source correctness,
   2. discovery mutex poison handling without panic-path.
