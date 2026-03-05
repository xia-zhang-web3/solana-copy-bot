# TEMP Consolidated Audit Report - Solana Copy Bot

Date: 2026-03-05
Status: Draft / temporary working file
Compiler: Codex

## Scope

This file consolidates:

- Codex local repo review.
- Auditor summary provided by the user ("SVODNY AUDIT-OTCHET").
- Additional auditor findings and addendum provided by the user.
- Local verification against `ROAD_TO_PRODUCTION.md` and current workspace code.

## Operator Note

- User instruction on 2026-03-05: committed QuikNode / Helius credentials are treated as test credentials and will not be rotated now.
- That instruction is recorded here as an operational choice, not as closure of the issue.
- Even if these are treated as test credentials, they still provide usable mainnet RPC access and remain an open repo / process problem.

## Verification Legend

- `Verified locally`: I checked the current code / config directly.
- `Imported from auditors`: included from the auditor pack, not fully re-verified by me in this pass.
- `Partially verified`: auditor finding plus a narrower local code check.

## Handoff Rule

- `Section 1` is the coder handoff list: only items with enough evidence to be actionable are kept there.
- `Section 2` is still an action inventory, not informational prose, but some items remain unpromoted until they get a narrower verification pass.

## 1. Consolidated Priority List

### Critical

#### C-1. Credentials committed to git

- Severity: Critical
- Sources: Codex, auditors
- Verification: Verified locally
- Summary: Real QuikNode credentials are committed in tracked config / template / docs. This conflicts with Stage B requirements that key material must not live in repo / local env files.
- Evidence:
  - `configs/live.toml:17`
  - `configs/live.toml:18`
  - `configs/live.toml:21`
  - `ops/server_templates/live.server.toml.example:17`
  - `ops/server_templates/live.server.toml.example:18`
  - `ops/server_templates/live.server.toml.example:21`
  - `ops/6_1_SERVER_EXECUTION_PLAYBOOK.md:62`
  - `ops/6_1_SERVER_EXECUTION_PLAYBOOK.md:63`
  - `ops/6_1_SERVER_EXECUTION_PLAYBOOK.md:65`
  - `ROAD_TO_PRODUCTION.md:122`
  - `ROAD_TO_PRODUCTION.md:150`
- Note: operator currently accepts non-rotation because these are treated as test credentials.

### High

#### H-1. Production-like config is internally contradictory and will self-trigger universe stop

- Severity: High
- Sources: Codex
- Verification: Verified locally
- Summary: `discovery.follow_top_n = 10`, while risk requires `shadow_universe_min_active_follow_wallets = 15`. Active follow wallets can never satisfy the configured minimum, so `shadow_risk_universe_stop` becomes inevitable after enough discovery cycles and blocks BUY flow.
- Evidence:
  - `configs/live.toml:58`
  - `configs/live.toml:166`
  - `configs/prod.toml:55`
  - `configs/prod.toml:149`
  - `ops/server_templates/live.server.toml.example:58`
  - `ops/server_templates/live.server.toml.example:164`
  - `crates/app/src/main.rs:711`
  - `crates/app/src/main.rs:906`
  - `ROAD_TO_PRODUCTION.md:1531`
  - `ROAD_TO_PRODUCTION.md:1559`

#### H-2. Execution confirm path reconciles synthetic fills instead of on-chain fills

- Severity: High
- Sources: Codex, auditor agent
- Verification: Verified locally
- Summary: confirmed orders are finalized using `latest_token_sol_price()` or fallback avg-cost, and `qty` is computed as `intent.notional_sol / avg_price_sol`. This can drift inventory, exposure and PnL away from actual chain reality.
- Evidence:
  - `crates/execution/src/confirmation.rs:173`
  - `crates/execution/src/confirmation.rs:238`
  - `crates/execution/src/confirmation.rs:245`
  - `crates/execution/src/reconcile.rs:15`

#### H-3. Confirm timeout can strand real positions outside automated management

- Severity: High
- Sources: Codex, auditor agent
- Verification: Verified locally
- Summary: on confirm timeout the order is marked failed / manual-reconcile-required, but the batch scanner does not revisit `execution_failed`, while SELL gating rejects sells without a local open position. A tx that lands after deadline can leave the bot with a real unmanaged position.
- Evidence:
  - `crates/execution/src/confirmation.rs:355`
  - `crates/execution/src/confirmation.rs:360`
  - `crates/execution/src/lib.rs:302`
  - `crates/execution/src/risk_gates.rs:47`

#### H-4. Executor idempotency can break after a successful live submit

- Severity: High
- Sources: Codex, auditor agent
- Verification: Verified locally
- Summary: executor sends upstream submit before persisting the cached response. If `store_submit_response()` fails, the RAII claim guard releases ownership and the next retry can re-submit the same order.
- Evidence:
  - `crates/executor/src/submit_handler.rs:128`
  - `crates/executor/src/submit_handler.rs:159`
  - `crates/executor/src/submit_handler.rs:285`
  - `crates/executor/src/submit_claim_guard.rs:27`

#### H-5. Executor upstream fallback can duplicate live submit across backends

- Severity: High
- Sources: Codex, auditor agent
- Verification: Verified locally
- Summary: retryable send / timeout / HTTP 5xx / read failures cause the same raw submit payload to be forwarded to fallback endpoints, without proving the primary backend did not already accept the request.
- Evidence:
  - `crates/executor/src/upstream_forward.rs:47`
  - `crates/executor/src/upstream_forward.rs:78`
  - `crates/executor/src/upstream_forward.rs:114`

#### H-6. Exposure TOCTOU on parallel confirm path

- Severity: High
- Sources: auditors, Codex
- Verification: Verified locally
- Summary: two already-submitted orders may both confirm in one batch and push live exposure above configured limits because risk gating is concentrated on pre-submit paths.
- Evidence:
  - `crates/execution/src/risk_gates.rs:56`
  - `crates/execution/src/confirmation.rs:238`
  - `crates/execution/src/lib.rs:302`

#### H-7. NaN poisoning through parser / Yellowstone amount parsing

- Severity: High
- Sources: auditors, Codex
- Verification: Partially verified
- Summary: NaN values can bypass non-positive guards in ingestion because `amount <= 0.0` does not reject NaN. Yellowstone amount parsing also accepts parsed `f64` values without an `is_finite()` check. There is also a NaN comparator fallback in followlist ranking, but I did not confirm the stronger claim that a NaN-scored wallet can directly reach followlist because the current ranking path filters `score >= min_score` first.
- Evidence:
  - `crates/ingestion/src/parser.rs:25`
  - `crates/ingestion/src/source/yellowstone.rs:124`
  - `crates/ingestion/src/source/yellowstone.rs:343`
  - `crates/discovery/src/followlist.rs:37`

#### H-10. Secrets may leak through `Debug`

- Severity: High
- Sources: auditors, Codex
- Verification: Verified locally
- Summary: config structures containing secrets derive `Debug`, so future `{:?}` logging / panic surfaces can expose sensitive fields. I did not confirm an active runtime callsite leaking `ExecutionConfig` in this pass, so this is a real leakage surface rather than a currently observed live leak.
- Evidence:
  - `crates/config/src/schema.rs:4`
  - `crates/config/src/schema.rs:40`
  - `crates/config/src/schema.rs:53`
  - `crates/config/src/schema.rs:56`
  - `crates/config/src/schema.rs:72`

#### H-11. Stage C says "check/create ATA", runtime only checks ATA existence

- Severity: High
- Sources: auditors
- Verification: Partially verified
- Summary: roadmap promises ATA check / creation, but current pre-trade checker only verifies existence through RPC and rejects if missing. This is a rollout-readiness mismatch.
- Evidence:
  - `ROAD_TO_PRODUCTION.md:211`
  - `crates/execution/src/pretrade.rs:205`
  - `crates/execution/src/pretrade.rs:323`

### Medium

#### M-1. Adapter HMAC replay cache can be poisoned and grow unbounded

- Severity: Medium
- Sources: Codex, auditors, auditor agent
- Verification: Verified locally
- Summary: adapter stores nonce before HMAC signature verification and the cache has no capacity cap. Invalid requests can consume memory for the full TTL window and can poison a legitimate nonce before the valid request arrives.
- Evidence:
  - `crates/adapter/src/main.rs:439`
  - `crates/adapter/src/main.rs:473`
  - `crates/adapter/src/main.rs:546`
  - `crates/adapter/src/main.rs:559`
  - compare with hardened executor flow: `crates/executor/src/auth_verifier.rs:164`, `crates/executor/src/auth_verifier.rs:173`

#### M-2. Secret-file permission checks are not fail-closed across runtime surfaces

- Severity: Medium
- Sources: auditors
- Verification: Verified locally
- Summary: some generic secret-loading paths warn on relaxed permissions instead of refusing startup, leaving broad-readable secret files acceptable in production. Important correction: the executor signer keypair path is already fail-closed and is intentionally excluded from this finding.
- Evidence:
  - `crates/app/src/secrets.rs:88`
  - `crates/adapter/src/main.rs:1671`
  - `crates/executor/src/secret_source.rs:38`

#### M-4. Shadow scheduler holdback queue can bypass the main queue cap

- Severity: Medium
- Sources: auditors, Codex
- Verification: Verified locally
- Summary: held sells are tracked separately from the main pending queue and may grow outside `SHADOW_PENDING_TASK_CAPACITY`, then release as a burst later.
- Evidence:
  - `crates/app/src/shadow_scheduler.rs:23`
  - `crates/app/src/shadow_scheduler.rs:73`
  - `crates/app/src/shadow_scheduler.rs:81`
  - `crates/app/src/shadow_scheduler.rs:99`
  - `crates/app/src/shadow_scheduler.rs:223`
  - `crates/app/src/main.rs:1826`

#### M-5. Discovery rug gate treats "not yet evaluated" as "safe"

- Severity: Medium
- Sources: Codex
- Verification: Verified locally
- Summary: when a buy has not aged past `rug_lookahead_seconds`, it is skipped from `rug_evaluated`, and `rug_ratio` falls back to `0.0`, which is treated as good. Recent risky leaders may therefore look cleaner than they should.
- Evidence:
  - `crates/discovery/src/lib.rs:425`
  - `crates/discovery/src/lib.rs:427`
  - `crates/discovery/src/lib.rs:470`
  - `crates/discovery/src/lib.rs:510`

## 2. Auditor Master Inventory Imported As Reported (Action Inventory, Not Informational)

This section is imported from the auditor pack so nothing gets lost. Items below are not all locally re-verified in this pass.

- These are still action items, not decorative notes.
- They are not all promoted into `Section 1` yet because some still need narrower verification or sharper scoping before they become a good coder handoff brief.

### Critical (auditor summary)

- C-1: real QuikNode API credentials committed in git in `configs/live.toml`; Helius API keys also present in local `.env`.

### High (auditor summary)

- H-1: TOCTOU in execution confirm path can exceed exposure limits.
- H-2: NaN passes parser because `amount <= 0.0` does not catch NaN.
- H-3: NaN in ranking can let a wallet with NaN-score reach followlist.
- H-4: `f64` used for financial values (`positions`, `PnL`, `cost_sol`).
- H-5: TOCTOU in `reconcile_followlist` due to `SELECT -> DELETE -> INSERT` without transaction.
- H-6: `#[derive(Debug)]` on secret-bearing config structs can leak credentials.

### Medium (auditor summary)

#### Execution pipeline

- No status precondition in SQL (`mark_order_submitted` can regress `execution_confirmed`).
- Simulation error can retry forever without attempt tracking and starve batch slots.
- Confirmed on-chain tx can become `execution_failed` if there is no price and no position.
- `mark_order_simulated` can regress `execution_submitted` and enable duplicate submit.
- Restart recovery for simulated orders relies on adapter idempotency.

#### Storage

- TOCTOU in `activate_follow_wallet` (`SELECT + INSERT` without transaction).
- `insert_shadow_lot` returns `last_insert_rowid()` outside retry closure.
- `unchecked_transaction()` in `persist_discovery_cycle` is deferred and has no retry.
- No foreign key constraints between tables.
- `insert_observed_swap` and other writes without retry.
- No enforced order-status state machine in storage.
- `positions` allows multiple open rows per token because there is no unique index.

#### Shadow + Discovery

- `has_shadow_lots` counts dust lots and can cause phantom exit logic.
- Discovery quality gates are bypassed when RPC is unavailable.
- Sell qty is computed from signal notional rather than actual position size.
- Cap eviction can make wallets disappear from scoring and cause false demotion.
- `partition_point()` in rug detection depends on fragile ordering invariants.

#### Adapter

- Nonce replay map grows without limit.
- No explicit request body size limit beyond framework defaults.
- No response size limit for upstream JSON parsing.
- Submit endpoint returns HTTP 200 even on reject.
- `validate_endpoint_url` allows `http://` and does not force TLS everywhere.

#### Executor

- HMAC nonce replay is possible after restart because cache is in-memory.
- Nonce cache overflow can become a DoS vector.
- `std::sync::Mutex` used around SQLite in async context can block runtime workers.

#### Config + App + Ingestion

- Env overrides for risk config allegedly lack range validation and lack visibility.
- WebSocket URL with API key in query string can be logged as-is.
- Hand-rolled JSON escaper instead of `serde_json`.
- `f64` precision loss in token amount parsing.
- `RUST_LOG` can suppress security-relevant warnings.
- Default `ShadowConfig.min_holders = 1` is too permissive.

#### Core-types

- `f64` used for financial sums instead of integer lamports.
- `SignalSide` deserialization is case-sensitive and has no `#[serde(other)]`.
- All fields are `pub`, so invalid state is constructible.

### Low (auditor summary)

- Auditor pack mentions 28 low-severity items, but full detail was not included in the pasted summary.
- Summary note from the auditors: timestamp inconsistency, median overflow, unbounded heartbeat table, and other theoretical issues exist in the full report set.

## 3. Additional Auditor Findings With References

These were pasted separately from the summary and are preserved here with their references.

### Imported finding set A

- High: committed live credentials violate Stage B secret-handling expectations.
  - `configs/live.toml#L17`
  - `configs/live.toml#L21`
  - `ops/6_1_SERVER_EXECUTION_PLAYBOOK.md#L62`
  - `ops/server_templates/live.server.toml.example#L17`
  - `ROAD_TO_PRODUCTION.md#L122`
  - `ROAD_TO_PRODUCTION.md#L150`

- High: roadmap says check / create ATA, but runtime only checks existence and hard-rejects if missing.
  - `ROAD_TO_PRODUCTION.md#L211`
  - `crates/execution/src/pretrade.rs#L211`
  - `crates/execution/src/pretrade.rs#L323`
  - `ROAD_TO_PRODUCTION.md#L739`

- Medium: adapter HMAC replay cache is a DoS surface.
  - `crates/adapter/src/main.rs#L443`
  - `crates/adapter/src/main.rs#L547`
  - `crates/adapter/src/main.rs#L556`
  - `crates/adapter/src/main.rs#L564`

- Medium: file-based secrets outside signer path are not fail-closed on permissions.
  - `crates/app/src/secrets.rs#L14`
  - `crates/app/src/secrets.rs#L88`
  - `crates/adapter/src/main.rs#L1671`
  - `crates/executor/src/secret_source.rs#L38`
  - Note after local verification: `crates/executor/src/signer_source.rs#L87` is fail-closed and should not be used as evidence for this item.

- Medium: Stage B requires working alert delivery, but code review did not find an obvious webhook / Telegram delivery path.
  - `ROAD_TO_PRODUCTION.md#L134`
  - `ROAD_TO_PRODUCTION.md#L150`
  - `crates/app/src/main.rs#L1602`
  - `tools/runtime_snapshot.sh#L245`

### Imported addendum

- Medium: shadow scheduler holdback queue can grow outside the main queue capacity.
  - `crates/app/src/shadow_scheduler.rs#L23`
  - `crates/app/src/shadow_scheduler.rs#L73`
  - `crates/app/src/shadow_scheduler.rs#L81`
  - `crates/app/src/shadow_scheduler.rs#L99`
  - `crates/app/src/shadow_scheduler.rs#L223`
  - `crates/app/src/main.rs#L1826`

## 4. Deferred / Secondary Findings

These are real enough to preserve, but not strong candidates for the top remediation brief in their current form.

### D-1. `reconcile_followlist()` helper is non-transactional, but currently unused

- Severity: Medium
- Sources: auditors, Codex
- Verification: Verified locally
- Summary: storage exposes a non-transactional helper that does `list_active_follow_wallets()`, then separate activate / deactivate calls without wrapping the full reconciliation in one transaction. I am not keeping this in the main priority list because the method is currently unused in the repo; the runtime path uses `persist_discovery_cycle()` instead.
- Evidence:
  - helper definition only: `crates/storage/src/discovery.rs:315`
  - non-transactional helpers it composes: `crates/storage/src/discovery.rs:268`, `crates/storage/src/discovery.rs:286`
  - no in-repo callsites found for `reconcile_followlist(`

### D-2. `f64` financial-state drift is a broad architecture issue, not a narrow first fix slice

- Severity: High
- Sources: auditors
- Verification: Partially verified
- Summary: the auditor concern is directionally valid: financial state currently relies on floating point types in storage / reconciliation paths. I am not keeping it in `Section 1` because, at the current evidence level, this is an architecture epic rather than a bounded coder handoff. It should be narrowed to specific flows such as open-position accounting, realized PnL settlement, or persistence schema changes before assigning implementation work.
- Evidence:
  - Auditor summary item `H-4`
  - broad storage surface: `crates/storage/src/lib.rs`
  - one concrete reconciliation path: `crates/execution/src/reconcile.rs:28`

### D-3. Stage B alert-delivery gap remains plausible, but this pass did not narrow it enough for direct coding handoff

- Severity: Medium
- Sources: auditors
- Verification: Imported from auditors
- Summary: roadmap requires tested alert delivery, while the code inspected in this pass clearly shows local risk events / logs / snapshot tooling but not an obvious notification transport. I am preserving the finding, but not keeping it in `Section 1` because it still needs a narrower code-level verification pass and a clearer target surface.
- Evidence:
  - `ROAD_TO_PRODUCTION.md:134`
  - `ROAD_TO_PRODUCTION.md:150`
  - broad app surface: `crates/app/src/main.rs`
  - operational tooling reference: `tools/runtime_snapshot.sh`

## 5. Dedupe / Merge Notes

- Credential leak findings from Codex and the auditors are the same issue. They are merged under `C-1`.
- Adapter HMAC replay / nonce-cache findings from Codex and auditors are the same issue. They are merged under `M-1`.
- ATA roadmap mismatch is not the same as the credential leak; it is a rollout-readiness mismatch and stays separate as `H-11`.
- The impossible universe thresholds issue (`follow_top_n < min_active_follow_wallets`) appears to explain the observed `eligible_wallets_last=0` / `active_follow_wallets_last=0` operational symptoms, but it is logically separate from the discovery OOM work already documented in `ROAD_TO_PRODUCTION.md`.

## 6. Suggested First Remediation Slice

If / when fixes start, the highest-leverage sequence looks like:

1. Decide how to handle committed credentials operationally, even if rotation is deferred.
2. Add startup config invariants so impossible production profiles fail before runtime:
   - `discovery.follow_top_n >= risk.shadow_universe_min_active_follow_wallets`
   - optional check for bootstrap / placeholder signer pubkeys in production-like profiles
3. Fix duplicate-submit risk in executor:
   - no claim release after a successful upstream submit unless response persistence succeeded
   - no fallback submit to a second backend without stronger proof / dedupe semantics
4. Replace synthetic confirm reconciliation with actual fill data from chain / adapter contract
5. Cap and harden adapter HMAC replay cache

## 7. Temporary File Note

- This file is intended as a working consolidation artifact and may be replaced by a cleaner final audit document later.
