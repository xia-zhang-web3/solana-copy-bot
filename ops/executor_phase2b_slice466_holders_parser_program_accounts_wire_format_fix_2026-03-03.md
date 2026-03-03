# Executor Phase-2B Slice 466 — Holders Parser Program-Accounts Wire-Format Fix

Date: 2026-03-03

## Scope

1. `crates/storage/src/market_data.rs`
2. `ROAD_TO_PRODUCTION.md`

## Problem

After switching holders fetch to `getProgramAccounts`, parser still expected wrapped shape `result.value=[...]`, which mismatched standard Solana JSON-RPC wire format (`result=[...]`).

## Change

1. Updated holders parser to accept standard `getProgramAccounts` response first:
   1. `rpc_result(response).as_array()`.
2. Kept compatibility fallback for wrapped responses:
   1. `rpc_result(response).get("value").as_array()`.
3. Updated tests:
   1. primary success fixture now uses standard `result=[...]` format,
   2. added explicit compatibility test for wrapped `result.value=[...]`,
   3. invalid-shape reject test preserved.

## Validation

1. `cargo test -p copybot-storage -q` — PASS (17/17)
2. `cargo check -p copybot-storage -q` — PASS

## Mapping

1. closes audit finding on program-accounts response shape mismatch.
