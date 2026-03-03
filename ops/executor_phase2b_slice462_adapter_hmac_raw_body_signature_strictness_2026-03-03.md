# Executor Phase-2B Slice 462 — Adapter HMAC Raw-Body Signature Strictness

Date: 2026-03-03

## Scope

1. `crates/adapter/src/main.rs`
2. `ROAD_TO_PRODUCTION.md`

## Problem

`AuthVerifier::verify` built the HMAC payload using `String::from_utf8_lossy(raw_body)`, which could alter non-UTF8 bytes before signature verification.

## Change

1. Switched HMAC payload construction to exact raw bytes:
   1. prefix: `"{timestamp}\n{ttl}\n{nonce}\n"`
   2. suffix: unchanged request `raw_body` bytes.
2. Removed lossy UTF-8 conversion from runtime verification path.
3. Added tests:
   1. `auth_verifier_accepts_hmac_signature_over_raw_non_utf8_body`
   2. `auth_verifier_rejects_hmac_signature_computed_over_lossy_body`

## Validation

1. `cargo test -p copybot-adapter -q hmac_signature_over_raw_non_utf8_body` — PASS
2. `cargo test -p copybot-adapter -q hmac_signature_computed_over_lossy_body` — PASS
3. `cargo check -p copybot-adapter -q` — PASS
4. `rg -n "from_utf8_lossy\(" crates/adapter/src/main.rs` — runtime verify path no longer uses lossy conversion (remaining occurrences are test-only helpers).

## Mapping

1. `ROAD_TO_PRODUCTION.md` post-6.1 gate: strict UTF-8/HMAC verify path hardening.
