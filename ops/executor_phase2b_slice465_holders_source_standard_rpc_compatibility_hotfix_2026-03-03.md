# Executor Phase-2B Slice 465 — Holders Source Standard RPC Compatibility Hotfix

Date: 2026-03-03

## Scope

1. `crates/storage/src/market_data.rs`
2. `ROAD_TO_PRODUCTION.md`

## Problem

Previous holders-source change used `getTokenAccountsByMint`, which is not part of standard Solana JSON-RPC and can fail on non-Helius providers.

## Change

1. Replaced holders RPC method with standard Solana JSON-RPC:
   1. method: `getProgramAccounts`
   2. program: `TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA`
   3. filters: `dataSize=165` + `memcmp(offset=0, bytes=<mint>)`
   4. encoding: `jsonParsed`
2. Kept holder counting logic unchanged:
   1. unique `owner` values,
   2. include only accounts with `tokenAmount.amount > 0`.

## Validation

1. `cargo test -p copybot-storage -q` — PASS
2. `cargo check -p copybot-storage -q` — PASS
3. `rg -n "getTokenAccountsByMint|getTokenSupply|getProgramAccounts" crates/storage/src/market_data.rs` confirms active method is `getProgramAccounts` only.

## Mapping

1. Removes provider-specific dependency for holders metric source in post-bring-up hardening path.
